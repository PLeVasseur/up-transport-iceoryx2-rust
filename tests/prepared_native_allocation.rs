// Copyright (c) 2026 Contributors to the Eclipse Foundation
// SPDX-License-Identifier: Apache-2.0
//! Harness-free all-thread allocation observations over actual native delivery.

#[path = "support/prepared_native.rs"]
mod common;
use common::Value;
use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
use up_rust::{
    HeaderLimits, InitializedStablePayload, LoanError, MessageFields, PreparedRpcClient,
    PreparedRpcServer, PreparedTransportCore, RequestHandler, RouteConfig, RpcClientConfig,
    RpcServerConfig, RxAdmission, RxLease, SendError, StablePayloadInit, TokenError, TxTemplate,
    UCode, UPriority, UUID, UUri, WireRegistration, ZeroCopyHandler,
};
use up_transport_iceoryx2_rust::Iceoryx2PreparedRx;

static ACTIVE: AtomicBool = AtomicBool::new(false);
static CALLS: AtomicUsize = AtomicUsize::new(0);
static BYTES: AtomicUsize = AtomicUsize::new(0);
struct Counter;
fn charge(bytes: usize) {
    if ACTIVE.load(Ordering::SeqCst) {
        CALLS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(bytes, Ordering::Relaxed);
    }
}
// SAFETY: every operation forwards the caller's allocation contract unchanged
// to System; accounting accesses only static atomic counters on all threads.
unsafe impl GlobalAlloc for Counter {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        charge(layout.size());
        // SAFETY: forwarding the caller's original valid layout.
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        charge(layout.size());
        // SAFETY: forwarding the caller's original valid layout.
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        charge(size);
        // SAFETY: forwarding the original pointer/layout and new size.
        unsafe { System.realloc(pointer, layout, size) }
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: forwarding the original live allocation exactly once.
        unsafe { System.dealloc(pointer, layout) }
    }
}
#[global_allocator]
static ALLOCATOR: Counter = Counter;

struct Observe {
    count: Arc<AtomicUsize>,
    sequence: u32,
    tokens: bool,
}
impl ZeroCopyHandler<Value, Iceoryx2PreparedRx> for Observe {
    async fn on_receive(&mut self, lease: RxLease<Value, Iceoryx2PreparedRx>) {
        let value = lease.payload().unwrap();
        assert_eq!(value.sequence, self.sequence);
        assert!(
            value
                .bytes
                .iter()
                .enumerate()
                .all(|(i, byte)| *byte == i as u8)
        );
        let header = lease.header().unwrap();
        assert_eq!(header.token().map(str::len), self.tokens.then_some(1536));
        assert_eq!(header.traceparent(), self.tokens.then_some(common::TRACE));
        drop(lease);
        self.sequence += 1;
        self.count.fetch_add(1, Ordering::Release);
    }
}

fn window(name: &str, messages: u32, operation: impl FnOnce()) {
    CALLS.store(0, Ordering::SeqCst);
    BYTES.store(0, Ordering::SeqCst);
    assert!(!ACTIVE.swap(true, Ordering::SeqCst));
    operation();
    ACTIVE.store(false, Ordering::SeqCst);
    let calls = CALLS.load(Ordering::SeqCst);
    let bytes = BYTES.load(Ordering::SeqCst);
    println!(
        "subject={name}\tmessages={messages}\tallocations={calls}\tallocated_bytes={bytes}\tall_threads=true\tobserver_subtracted=false"
    );
    assert_eq!(
        (calls, bytes),
        (0, 0),
        "native allocation contract failed for {name}"
    );
}

struct ReplyHandler;
impl RequestHandler<Value, Value, Iceoryx2PreparedRx> for ReplyHandler {
    type Reply = u32;
    async fn handle(&mut self, request: RxLease<Value, Iceoryx2PreparedRx>) -> Result<u32, UCode> {
        let sequence = request.payload().unwrap().sequence;
        let header = request.header().unwrap();
        if let Some(token) = header.token() {
            assert!(matches!(token.len(), 64 | 1536));
        }
        drop(request);
        tokio::task::yield_now().await;
        if sequence == u32::MAX {
            Err(UCode::PermissionDenied)
        } else {
            Ok(sequence + 1)
        }
    }
    fn write_reply<'a>(
        &mut self,
        sequence: u32,
        init: <Value as StablePayloadInit>::Initializer<'a>,
    ) -> InitializedStablePayload<'a, Value> {
        init.sequence(sequence)
            .bytes_fill_with(|i| i as u8)
            .finish()
    }
}

fn response_accounting(
    server: &PreparedRpcServer<up_transport_iceoryx2_rust::Iceoryx2PreparedCore, Value>,
    expected: u64,
) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while server.stats().responded.load(Ordering::Acquire) != expected {
        assert!(
            std::time::Instant::now() < deadline,
            "native RPC response accounting did not finish"
        );
        std::thread::yield_now();
    }
}

fn rpc(runtime: &tokio::runtime::Runtime) {
    use up_transport_iceoryx2_rust::{Iceoryx2PreparedCore, Iceoryx2RouteReservation};
    let method = UUri::try_from_parts("rpc-service", 0x4210, 1, 0x1000).unwrap();
    let replies: Vec<_> = (0..9)
        .map(|id| UUri::try_from_parts("rpc-client", id + 1, 1, 0).unwrap())
        .collect();
    let mut catalog = Vec::new();
    for reply in &replies {
        catalog.push(Iceoryx2RouteReservation {
            source: reply.clone(),
            sink: Some(method.clone()),
            limits: common::limits(68),
        });
        catalog.push(Iceoryx2RouteReservation {
            source: method.clone(),
            sink: Some(reply.clone()),
            limits: common::limits(68),
        });
    }
    let core =
        Arc::new(Iceoryx2PreparedCore::new(common::config(4096, 1), &catalog, 32, 10).unwrap());
    let agreement = common::agreement();
    let server = runtime
        .block_on(PreparedRpcServer::<_, Value>::start::<Value, _>(
            Arc::clone(&core),
            agreement.clone(),
            method.clone(),
            ReplyHandler,
            RpcServerConfig {
                registration: common::registration(),
                header: HeaderLimits::default(),
                max_clients: 8,
            },
        ))
        .unwrap();
    let clients: Vec<_> = replies
        .into_iter()
        .map(|reply| {
            runtime
                .block_on(PreparedRpcClient::<_, Value, Value>::new(
                    Arc::clone(&core),
                    agreement.clone(),
                    reply,
                    method.clone(),
                    RpcClientConfig {
                        max_in_flight: 1,
                        ttl: std::time::Duration::from_millis(100),
                        max_token_len: 2048,
                        priority: UPriority::CS4,
                    },
                ))
                .unwrap()
        })
        .collect();
    runtime.block_on(core.wait_ready()).unwrap();
    // Warm native delivery/application workers with an equal-size foreign request,
    // which is rejected before server handling/cache admission. No client is called.
    let foreign = TxTemplate::<common::Foreign>::new(agreement)
        .unwrap()
        .route(
            Arc::clone(&core),
            up_rust::RouteDefinition {
                kind: up_rust::UMessageType::Request,
                source: catalog[0].source.clone(),
                sink: Some(method),
                priority: Some(UPriority::CS4),
                ttl: Some(std::time::Duration::from_secs(1)),
                permission_level: None,
            },
            RouteConfig::default(),
        )
        .unwrap();
    for sequence in 0..100 {
        foreign
            .send(MessageFields::default(), |init| {
                init.into_initializer()
                    .sequence(sequence)
                    .bytes_fill(0)
                    .finish()
            })
            .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    runtime.block_on(async {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    });
    assert_eq!(server.stats().responded.load(Ordering::Acquire), 0);
    let token_long = "l".repeat(1536);
    let token_short = "s".repeat(64);
    let oversized = "x".repeat(2049);
    let mut total = 0_u64;
    window("iceoryx2_rpc_first_contact_8_new_clients", 800, || {
        runtime.block_on(async {
            for client in clients.iter().take(8) {
                for sequence in 0..100 {
                    let reply = client
                        .call(|init| {
                            init.into_initializer()
                                .sequence(sequence)
                                .bytes_fill(0)
                                .finish()
                        })
                        .await
                        .unwrap();
                    assert_eq!(reply.payload().unwrap().sequence, sequence + 1);
                    assert!(reply.header().unwrap().reqid.is_some());
                    drop(reply);
                    total += 1;
                }
            }
        });
        response_accounting(&server, total);
    });
    window("iceoryx2_rpc_rotating_token", 5000, || {
        runtime.block_on(async {
            for sequence in 0..5000 {
                clients[0]
                    .set_token(if sequence % 2 == 0 {
                        &token_long
                    } else {
                        &token_short
                    })
                    .unwrap();
                let reply = clients[0]
                    .call(|init| {
                        init.into_initializer()
                            .sequence(sequence)
                            .bytes_fill(0)
                            .finish()
                    })
                    .await
                    .unwrap();
                assert_eq!(reply.payload().unwrap().sequence, sequence + 1);
                drop(reply);
                total += 1;
            }
        });
        response_accounting(&server, total);
    });
    window("iceoryx2_rpc_status_only_response", 1, || {
        let status = runtime.block_on(clients[0].call(|init| {
            init.into_initializer()
                .sequence(u32::MAX)
                .bytes_fill(0)
                .finish()
        }));
        assert!(matches!(
            status,
            Err(up_rust::CallError::Status(UCode::PermissionDenied))
        ));
        total += 1;
        response_accounting(&server, total);
    });
    window(
        "iceoryx2_rpc_client_capacity_refusal_and_timeout",
        1,
        || {
            let result = runtime.block_on(
                clients[8].call(|init| init.into_initializer().sequence(0).bytes_fill(0).finish()),
            );
            assert!(matches!(result, Err(up_rust::CallError::TimedOut)));
            assert_eq!(server.stats().refused.load(Ordering::Acquire), 1);
            assert_eq!(clients[8].in_flight(), 0);
        },
    );
    window("iceoryx2_rpc_token_rejection", 1000, || {
        for _ in 0..1000 {
            assert!(matches!(
                clients[0].set_token(&oversized),
                Err(TokenError::TooLong)
            ));
        }
    });
    drop(clients);
    runtime.block_on(server.stop()).unwrap();
}

fn main() {
    let started = Arc::new(AtomicUsize::new(0));
    let threads = Arc::clone(&started);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .on_thread_start(move || {
            std::hint::black_box(UUID::build());
            threads.fetch_add(1, Ordering::Release);
        })
        .enable_all()
        .build()
        .unwrap();
    common::wait(&started, 2);
    std::hint::black_box(UUID::build());
    thread_local! {
        static WARMED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    }
    let warmed = Arc::new(AtomicUsize::new(0));
    let warmers: Vec<_> = (0..8)
        .map(|_| {
            let warmed = Arc::clone(&warmed);
            runtime.spawn(async move {
                while warmed.load(Ordering::Acquire) < 2 {
                    let before = std::thread::current().id();
                    tokio::task::yield_now().await;
                    if before == std::thread::current().id()
                        && WARMED.with(|flag| !flag.replace(true))
                    {
                        warmed.fetch_add(1, Ordering::AcqRel);
                    }
                    // Yielding/sleeping leaves both scheduler workers free to
                    // steal setup work. A blocking barrier can strand its peer
                    // in the same worker's local queue before native setup.
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                }
            })
        })
        .collect();
    runtime.block_on(async {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            for task in warmers {
                task.await.unwrap();
            }
        })
        .await
        .expect("both actual runtime workers must finish bounded yield warmup");
    });
    assert_eq!(warmed.load(Ordering::Acquire), 2);
    println!("setup_runtime_workers_warmed=2\tfirst_client_admission_warmed=false");
    for tokens in [false, true] {
        let core = common::core(std::mem::size_of::<Value>(), 1);
        let agreement = common::agreement();
        let admission =
            RxAdmission::<Value>::new(agreement.clone(), HeaderLimits::default()).unwrap();
        let count = Arc::new(AtomicUsize::new(0));
        let worker = runtime.block_on(async {
            admission
                .register_filtered(
                    common::source(),
                    None,
                    common::registration(),
                    Observe {
                        count: Arc::clone(&count),
                        sequence: 0,
                        tokens,
                    },
                    || {},
                )
                .unwrap()
        });
        let subscription = runtime
            .block_on(WireRegistration::attach(
                Arc::clone(&core),
                &common::source(),
                None,
                worker,
            ))
            .unwrap();
        runtime.block_on(core.wait_ready()).unwrap();
        let route = TxTemplate::<Value>::new(agreement)
            .unwrap()
            .route(
                Arc::clone(&core),
                common::definition(common::source()),
                RouteConfig {
                    max_token_len: 2048,
                    allow_traceparent: true,
                },
            )
            .unwrap();
        let token = "t".repeat(1536);
        let send = |sequence| {
            route
                .send(
                    MessageFields {
                        token: tokens.then_some(token.as_str()),
                        traceparent: tokens.then_some(common::TRACE),
                        ..MessageFields::default()
                    },
                    |init| {
                        init.into_initializer()
                            .sequence(sequence)
                            .bytes_fill_with(|i| i as u8)
                            .finish()
                    },
                )
                .unwrap();
            common::wait(&count, sequence as usize + 1);
        };
        // Warm every actual delivery and application thread through real frames.
        for sequence in 0..100 {
            send(sequence);
        }
        let name = if tokens {
            "iceoryx2_publish_token_1536_and_traceparent"
        } else {
            "iceoryx2_publish_async_handler"
        };
        window(name, 50_000, || {
            for sequence in 100..50_100 {
                send(sequence);
            }
        });
        let oversized = "x".repeat(2049);
        window("iceoryx2_reject_token_and_traceparent", 1000, || {
            for _ in 0..1000 {
                assert!(matches!(
                    route.send(
                        MessageFields {
                            token: Some(&oversized),
                            ..MessageFields::default()
                        },
                        |init| init.into_initializer().sequence(0).bytes_fill(0).finish()
                    ),
                    Err(SendError::TokenTooLong)
                ));
                assert!(matches!(
                    route.send(
                        MessageFields {
                            traceparent: Some(&oversized),
                            ..MessageFields::default()
                        },
                        |init| init.into_initializer().sequence(0).bytes_fill(0).finish()
                    ),
                    Err(SendError::TraceparentRejected)
                ));
            }
        });
        let physical = core
            .prepare_route(&common::source(), None, common::limits(68))
            .unwrap();
        let layout = up_rust::TxFrameLayout {
            metadata_len: 128,
            payload_len: Some(68),
            payload_alignment: 8,
        };
        let held = core.loan(&physical, layout).unwrap();
        window("iceoryx2_reject_native_pool_exhaustion", 1000, || {
            for _ in 0..1000 {
                assert!(matches!(
                    core.loan(&physical, layout),
                    Err(LoanError::PoolExhausted)
                ));
            }
        });
        drop(held);
        window("iceoryx2_reject_native_slot_too_small", 1000, || {
            for _ in 0..1000 {
                assert!(matches!(
                    core.loan(
                        &physical,
                        up_rust::TxFrameLayout {
                            payload_len: Some(69),
                            ..layout
                        }
                    ),
                    Err(LoanError::SlotTooSmall)
                ));
            }
        });
        assert_eq!(admission.stats().admitted.load(Ordering::Acquire), 50_100);
        assert_eq!(admission.stats().rejected.load(Ordering::Acquire), 0);
        runtime.block_on(subscription.stop()).unwrap();
    }
    rpc(&runtime);
}
