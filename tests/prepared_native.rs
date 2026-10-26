// Copyright (c) 2026 Contributors to the Eclipse Foundation
// SPDX-License-Identifier: Apache-2.0
//! Real native prepared delivery, isolation, immutable retention and identity rejection.

#[path = "support/prepared_native.rs"]
mod common;

use common::{Foreign, Value};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use up_rust::{
    Frame, HeaderAdmission, HeaderLimits, MessageFields, OpaqueTemplate, PayloadEncoding,
    PreparedTransportCore, RouteConfig, RxAdmission, RxLease, TxHeaderParts, TxTemplate,
    UFrameMetadata, WireRegistration, ZeroCopyHandler,
};
use up_transport_iceoryx2_rust::Iceoryx2PreparedRx;

type Observation = (u32, Option<String>, Option<String>);

struct Observe {
    count: Arc<AtomicUsize>,
    seen: Arc<Mutex<Vec<Observation>>>,
}
impl ZeroCopyHandler<Value, Iceoryx2PreparedRx> for Observe {
    async fn on_receive(&mut self, lease: RxLease<Value, Iceoryx2PreparedRx>) {
        let value = lease.payload().unwrap();
        assert!(
            value
                .bytes
                .iter()
                .enumerate()
                .all(|(index, byte)| *byte == index as u8)
        );
        let header = lease.header().unwrap();
        self.seen.lock().unwrap().push((
            value.sequence,
            header.token().map(str::to_owned),
            header.traceparent().map(str::to_owned),
        ));
        self.count.fetch_add(1, Ordering::Release);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_values_tokens_traceparent_fifo_and_foreign_type_rejection() {
    let core = common::core(std::mem::size_of::<Value>(), 1);
    let agreement = common::agreement();
    let admission = RxAdmission::<Value>::new(agreement.clone(), HeaderLimits::default()).unwrap();
    let count = Arc::new(AtomicUsize::new(0));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let worker = admission
        .register_filtered(
            common::source(),
            None,
            common::registration(),
            Observe {
                count: Arc::clone(&count),
                seen: Arc::clone(&seen),
            },
            || {},
        )
        .unwrap();
    let subscription = WireRegistration::attach(Arc::clone(&core), &common::source(), None, worker)
        .await
        .unwrap();
    core.wait_ready().await.unwrap();
    let route = TxTemplate::<Value>::new(agreement.clone())
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
    for sequence in 0..300 {
        route
            .send(
                MessageFields {
                    token: (sequence % 3 != 0).then_some(token.as_str()),
                    traceparent: (sequence % 5 == 0).then_some(common::TRACE),
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
    }
    let foreign = TxTemplate::<Foreign>::new(agreement)
        .unwrap()
        .route(
            Arc::clone(&core),
            common::definition(common::source()),
            RouteConfig::default(),
        )
        .unwrap();
    foreign
        .send(MessageFields::default(), |init| {
            init.into_initializer().sequence(999).bytes_fill(0).finish()
        })
        .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while admission.stats().rejected.load(Ordering::Acquire) != 1 {
        assert!(std::time::Instant::now() < deadline);
        tokio::task::yield_now().await;
    }
    subscription.stop().await.unwrap();
    assert_eq!(count.load(Ordering::Acquire), 300);
    for (index, (sequence, carried, trace)) in seen.lock().unwrap().iter().enumerate() {
        assert_eq!(*sequence as usize, index);
        assert_eq!(
            carried.as_deref(),
            (sequence % 3 != 0).then_some(token.as_str())
        );
        assert_eq!(
            trace.as_deref(),
            (sequence % 5 == 0).then_some(common::TRACE)
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_absent_and_present_empty_are_distinct() {
    let core = common::core(0, 1);
    let inbox = Arc::new(
        up_rust::RegistrationInbox::<Iceoryx2PreparedRx>::new(common::registration()).unwrap(),
    );
    let mut subscription = core
        .attach(&common::source(), None, inbox.clone())
        .await
        .unwrap();
    core.wait_ready().await.unwrap();
    let route = OpaqueTemplate::for_wire::<up_rust::ProtobufWire>()
        .unwrap()
        .route(
            Arc::clone(&core),
            common::source(),
            None,
            common::limits(0),
            RouteConfig::default(),
        )
        .unwrap();
    let admission =
        HeaderAdmission::for_wire::<up_rust::ProtobufWire>(HeaderLimits::default()).unwrap();
    for present in [false, true] {
        let builder = UFrameMetadata::publish(common::source());
        let metadata = if present {
            builder.with_payload_encoding(PayloadEncoding::PROTOBUF)
        } else {
            builder
        }
        .build()
        .unwrap();
        route
            .send_bytes(TxHeaderParts::from(&metadata), present.then_some(&[][..]))
            .unwrap();
        let frame = tokio::time::timeout(Duration::from_secs(5), inbox.recv())
            .await
            .unwrap()
            .unwrap();
        let frame = admission.admit(frame).unwrap();
        assert_eq!(frame.payload().is_some(), present);
        assert_eq!(frame.header().unwrap().payload_encoding.is_some(), present);
        if present {
            assert!(frame.payload().unwrap().bytes().is_empty());
        }
    }
    core.detach(&mut subscription).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn independent_native_fanout_retains_original_addresses_through_detach_and_pressure() {
    let mut config = common::config(std::mem::size_of::<Value>() + 4096, 2);
    // This reuse-pressure control intentionally permits native safe overflow;
    // reliable/block tests retain their separately explicit no-overflow setup.
    config
        .iceoryx2_config
        .as_mut()
        .unwrap()
        .defaults
        .publish_subscribe
        .enable_safe_overflow = true;
    let core = Arc::new(
        up_transport_iceoryx2_rust::Iceoryx2PreparedCore::new(
            config,
            &[up_transport_iceoryx2_rust::Iceoryx2RouteReservation {
                source: common::source(),
                sink: None,
                limits: common::limits(std::mem::size_of::<Value>()),
            }],
            16,
            2,
        )
        .unwrap(),
    );
    let a = Arc::new(
        up_rust::RegistrationInbox::<Iceoryx2PreparedRx>::new(common::registration()).unwrap(),
    );
    let b = Arc::new(
        up_rust::RegistrationInbox::<Iceoryx2PreparedRx>::new(common::registration()).unwrap(),
    );
    let mut a_sub = core
        .attach(&common::source(), None, a.clone())
        .await
        .unwrap();
    let mut b_sub = core
        .attach(&common::source(), None, b.clone())
        .await
        .unwrap();
    core.wait_ready().await.unwrap();
    let route = TxTemplate::<Value>::new(common::agreement())
        .unwrap()
        .route(
            Arc::clone(&core),
            common::definition(common::source()),
            RouteConfig::default(),
        )
        .unwrap();
    route
        .send(MessageFields::default(), |init| {
            init.into_initializer()
                .sequence(42)
                .bytes_fill(0x5a)
                .finish()
        })
        .unwrap();
    let first = tokio::time::timeout(Duration::from_secs(5), a.recv())
        .await
        .unwrap()
        .unwrap();
    let second = tokio::time::timeout(Duration::from_secs(5), b.recv())
        .await
        .unwrap()
        .unwrap();
    let bytes = first.payload().unwrap().bytes();
    let address = bytes.as_ptr() as usize;
    assert_eq!(bytes, second.payload().unwrap().bytes());
    core.detach(&mut a_sub).unwrap();
    core.detach(&mut b_sub).unwrap();
    let replacement = Arc::new(
        up_rust::RegistrationInbox::<Iceoryx2PreparedRx>::new(common::registration()).unwrap(),
    );
    assert!(matches!(
        core.attach(&common::source(), None, replacement.clone())
            .await,
        Err(up_rust::SetupError::TableFull)
    ));
    for sequence in 0..256 {
        route
            .send(MessageFields::default(), |init| {
                init.into_initializer()
                    .sequence(sequence)
                    .bytes_fill(0)
                    .finish()
            })
            .unwrap();
    }
    assert_eq!(first.payload().unwrap().bytes().as_ptr() as usize, address);
    assert_eq!(
        first.payload().unwrap().bytes(),
        second.payload().unwrap().bytes()
    );
    assert!(
        first
            .payload()
            .unwrap()
            .bytes()
            .get(4..)
            .unwrap()
            .iter()
            .all(|byte| *byte == 0x5a)
    );
    drop(a_sub);
    drop(b_sub);
    drop(route);
    drop(core);
    std::thread::spawn(move || {
        assert_eq!(first.payload().unwrap().bytes().as_ptr() as usize, address);
        drop(first);
        drop(second);
    })
    .join()
    .unwrap();
}

#[tokio::test]
async fn physical_route_and_loan_tables_fail_closed_and_reclaim() {
    let core = common::core(68, 1);
    let other = common::core(68, 1);
    let limits = common::limits(68);
    let route = core.prepare_route(&common::source(), None, limits).unwrap();
    let layout = up_rust::TxFrameLayout {
        metadata_len: 128,
        payload_len: Some(68),
        payload_alignment: 8,
    };
    assert!(matches!(
        other.loan(&route, layout),
        Err(up_rust::LoanError::InvalidRoute)
    ));
    let loan = core.loan(&route, layout).unwrap();
    assert!(matches!(
        core.loan(&route, layout),
        Err(up_rust::LoanError::PoolExhausted)
    ));
    drop(loan);
    drop(core.loan(&route, layout).unwrap());
    let unknown = up_rust::UUri::try_from_parts("unknown", 1, 1, 0x9001).unwrap();
    assert!(matches!(
        core.prepare_route(&unknown, None, limits),
        Err(up_rust::SetupError::InvalidRoute(_))
    ));
    let mut routes = Vec::new();
    for _ in 0..15 {
        routes.push(core.prepare_route(&common::source(), None, limits).unwrap());
    }
    assert!(matches!(
        core.prepare_route(&common::source(), None, limits),
        Err(up_rust::SetupError::TableFull)
    ));
    drop(routes.pop());
    drop(core.prepare_route(&common::source(), None, limits).unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_type_zero_foreign_token_is_transported_then_rejected_before_callback() {
    use up_rust::{
        NativeProfile, NativeProfileAgreement, NativeProfileMode, NativeSingleType, StablePayload,
    };
    fn single<P: StablePayload>() -> NativeProfileAgreement {
        let profile = NativeProfile::new(
            "prepared-single-type",
            1,
            NativeProfileMode::SingleType(NativeSingleType::new(P::native_representation())),
        )
        .unwrap();
        NativeProfileAgreement::new(Arc::new(profile.clone()), &profile).unwrap()
    }
    assert_eq!(std::mem::size_of::<Value>(), std::mem::size_of::<Foreign>());
    let core = common::core(std::mem::size_of::<Value>(), 1);
    let agreement = single::<Value>();
    let admission = RxAdmission::<Value>::new(agreement.clone(), HeaderLimits::default()).unwrap();
    assert_eq!(admission.identity().encoding().id(), 0);
    let count = Arc::new(AtomicUsize::new(0));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let worker = admission
        .register_filtered(
            common::source(),
            None,
            common::registration(),
            Observe {
                count: Arc::clone(&count),
                seen: Arc::clone(&seen),
            },
            || {},
        )
        .unwrap();
    let subscription = WireRegistration::attach(Arc::clone(&core), &common::source(), None, worker)
        .await
        .unwrap();
    core.wait_ready().await.unwrap();
    let valid = TxTemplate::<Value>::new(agreement)
        .unwrap()
        .route(
            Arc::clone(&core),
            common::definition(common::source()),
            RouteConfig::default(),
        )
        .unwrap();
    valid
        .send(MessageFields::default(), |init| {
            init.into_initializer()
                .sequence(0)
                .bytes_fill_with(|i| i as u8)
                .finish()
        })
        .unwrap();
    common::wait(&count, 1);
    let foreign_agreement = single::<Foreign>();
    assert_eq!(
        foreign_agreement
            .identity_for(&Foreign::native_representation())
            .unwrap()
            .encoding()
            .id(),
        0
    );
    let foreign = TxTemplate::<Foreign>::new(foreign_agreement)
        .unwrap()
        .route(
            Arc::clone(&core),
            common::definition(common::source()),
            RouteConfig::default(),
        )
        .unwrap();
    foreign
        .send(MessageFields::default(), |init| {
            init.into_initializer().sequence(1).bytes_fill(0).finish()
        })
        .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while admission.stats().rejected.load(Ordering::Acquire) != 1 {
        assert!(std::time::Instant::now() < deadline);
        tokio::task::yield_now().await;
    }
    assert_eq!(core.stats().published.load(Ordering::Acquire), 2);
    assert_eq!(core.stats().received.load(Ordering::Acquire), 2);
    assert_eq!(count.load(Ordering::Acquire), 1);
    assert_eq!(seen.lock().unwrap().len(), 1);
    subscription.stop().await.unwrap();
}

#[cfg(target_os = "linux")]
fn backing(bytes: &[u8]) -> (String, u64, u64) {
    let address = bytes.as_ptr() as u64;
    for line in std::fs::read_to_string("/proc/self/maps").unwrap().lines() {
        let mut fields = line.split_whitespace();
        let (start, end) = fields.next().unwrap().split_once('-').unwrap();
        let start = u64::from_str_radix(start, 16).unwrap();
        let end = u64::from_str_radix(end, 16).unwrap();
        let permissions = fields.next().unwrap();
        let offset = u64::from_str_radix(fields.next().unwrap(), 16).unwrap();
        let device = fields.next().unwrap();
        let inode: u64 = fields.next().unwrap().parse().unwrap();
        if address >= start && address.checked_add(bytes.len() as u64).unwrap() <= end {
            assert!(permissions.ends_with('s'));
            assert_ne!(inode, 0);
            return (device.into(), inode, offset + address - start);
        }
    }
    panic!("real native payload mapping missing");
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_size_native_fanout_retains_same_backing_after_all_endpoint_handles_drop() {
    for bytes in [4_915_328, 12_441_784] {
        let mut config = common::config(bytes + 4096, 2);
        let native = &mut config
            .iceoryx2_config
            .as_mut()
            .unwrap()
            .defaults
            .publish_subscribe;
        native.subscriber_max_buffer_size = 2;
        native.subscriber_max_borrowed_samples = 2;
        let core = Arc::new(
            up_transport_iceoryx2_rust::Iceoryx2PreparedCore::new(
                config,
                &[up_transport_iceoryx2_rust::Iceoryx2RouteReservation {
                    source: common::source(),
                    sink: None,
                    limits: common::limits(bytes),
                }],
                1,
                2,
            )
            .unwrap(),
        );
        let a = Arc::new(
            up_rust::RegistrationInbox::<Iceoryx2PreparedRx>::new(common::registration()).unwrap(),
        );
        let b = Arc::new(
            up_rust::RegistrationInbox::<Iceoryx2PreparedRx>::new(common::registration()).unwrap(),
        );
        let mut a_sub = core
            .attach(&common::source(), None, a.clone())
            .await
            .unwrap();
        let mut b_sub = core
            .attach(&common::source(), None, b.clone())
            .await
            .unwrap();
        core.wait_ready().await.unwrap();
        let route = OpaqueTemplate::for_wire::<up_rust::ProtobufWire>()
            .unwrap()
            .route(
                Arc::clone(&core),
                common::source(),
                None,
                common::limits(bytes),
                RouteConfig::default(),
            )
            .unwrap();
        let metadata = UFrameMetadata::publish(common::source())
            .with_payload_encoding(PayloadEncoding::PROTOBUF)
            .build()
            .unwrap();
        let payload = vec![0x5a; bytes];
        route
            .send_bytes(TxHeaderParts::from(&metadata), Some(&payload))
            .unwrap();
        let first = tokio::time::timeout(Duration::from_secs(5), a.recv())
            .await
            .unwrap()
            .unwrap();
        let second = tokio::time::timeout(Duration::from_secs(5), b.recv())
            .await
            .unwrap()
            .unwrap();
        let original = backing(first.payload().unwrap().bytes());
        assert_eq!(original, backing(second.payload().unwrap().bytes()));
        let address = first.payload().unwrap().bytes().as_ptr() as usize;
        core.detach(&mut a_sub).unwrap();
        core.detach(&mut b_sub).unwrap();
        drop(a_sub);
        drop(b_sub);
        drop(route);
        drop(core);
        drop(payload);
        std::thread::spawn(move || {
            let retained = first.payload().unwrap().bytes();
            assert_eq!(retained.as_ptr() as usize, address);
            assert_eq!(retained.len(), bytes);
            assert!(retained.iter().all(|byte| *byte == 0x5a));
            assert_eq!(original, backing(retained));
            assert_eq!(retained, second.payload().unwrap().bytes());
        })
        .join()
        .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn receiver_only_mask_borrows_real_native_samples_without_an_extra_pool() {
    use iceoryx2::prelude::SemanticString;
    use up_transport_iceoryx2_rust::{Iceoryx2PreparedCore, Iceoryx2RouteReservation};
    let config = common::config(8192, 1);
    let prefix = String::from_utf8(
        config
            .iceoryx2_config
            .as_ref()
            .unwrap()
            .global
            .prefix
            .as_bytes()
            .to_vec(),
    )
    .unwrap();
    let backings = || -> std::collections::BTreeSet<(String, String)> {
        std::fs::read_to_string("/proc/self/maps")
            .unwrap()
            .lines()
            .filter_map(|line| {
                let f: Vec<_> = line.split_whitespace().collect();
                (f.len() >= 6 && f[5].contains(&prefix) && f[5].ends_with(".data"))
                    .then(|| (f[3].to_string(), f[4].to_string()))
            })
            .collect()
    };
    let catalog = [Iceoryx2RouteReservation {
        source: common::source(),
        sink: None,
        limits: common::limits(0),
    }];
    let sender = Arc::new(Iceoryx2PreparedCore::new(config.clone(), &catalog, 4, 1).unwrap());
    let before = backings();
    assert_eq!(before.len(), 1);
    assert!(
        Iceoryx2PreparedCore::new_with_transmit_mask(config.clone(), &catalog, &[], 4, 1).is_err()
    );
    let receiver = Arc::new(
        Iceoryx2PreparedCore::new_with_transmit_mask(config, &catalog, &[false], 4, 1).unwrap(),
    );
    assert_eq!(backings(), before);
    assert!(
        receiver
            .prepare_route(&common::source(), None, common::limits(0))
            .is_err()
    );
    let inbox = Arc::new(
        up_rust::RegistrationInbox::<Iceoryx2PreparedRx>::new(common::registration()).unwrap(),
    );
    let mut subscription = receiver
        .attach(&common::source(), None, inbox.clone())
        .await
        .unwrap();
    sender.wait_ready().await.unwrap();
    let route = OpaqueTemplate::for_wire::<up_rust::ProtobufWire>()
        .unwrap()
        .route(
            Arc::clone(&sender),
            common::source(),
            None,
            common::limits(0),
            RouteConfig::default(),
        )
        .unwrap();
    let metadata = UFrameMetadata::publish(common::source())
        .with_payload_encoding(PayloadEncoding::PROTOBUF)
        .build()
        .unwrap();
    route
        .send_bytes(TxHeaderParts::from(&metadata), Some(&[]))
        .unwrap();
    let raw = tokio::time::timeout(Duration::from_secs(5), inbox.recv())
        .await
        .unwrap()
        .unwrap();
    let admission =
        HeaderAdmission::for_wire::<up_rust::ProtobufWire>(HeaderLimits::default()).unwrap();
    let frame = admission.admit(raw).unwrap();
    assert!(frame.payload().unwrap().bytes().is_empty());
    assert_eq!(backings(), before);
    drop(frame);
    receiver.detach(&mut subscription).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn receiver_first_mask_keeps_service_until_a_native_publisher_arrives() {
    use iceoryx2::prelude::SemanticString;
    use up_transport_iceoryx2_rust::{Iceoryx2PreparedCore, Iceoryx2RouteReservation};
    let config = common::config(8192, 1);
    let prefix = String::from_utf8(
        config
            .iceoryx2_config
            .as_ref()
            .unwrap()
            .global
            .prefix
            .as_bytes()
            .to_vec(),
    )
    .unwrap();
    let backings = || -> std::collections::BTreeSet<(String, String)> {
        std::fs::read_to_string("/proc/self/maps")
            .unwrap()
            .lines()
            .filter_map(|line| {
                let fields: Vec<_> = line.split_whitespace().collect();
                (fields.len() >= 6 && fields[5].contains(&prefix) && fields[5].ends_with(".data"))
                    .then(|| (fields[3].to_string(), fields[4].to_string()))
            })
            .collect()
    };
    let catalog = [Iceoryx2RouteReservation {
        source: common::source(),
        sink: None,
        limits: common::limits(0),
    }];
    let receiver = Arc::new(
        Iceoryx2PreparedCore::new_with_transmit_mask(config.clone(), &catalog, &[false], 4, 1)
            .unwrap(),
    );
    assert!(backings().is_empty());
    let inbox = Arc::new(
        up_rust::RegistrationInbox::<Iceoryx2PreparedRx>::new(common::registration()).unwrap(),
    );
    // No publisher exists yet: the receive-only core must own its service metadata.
    let mut subscription = receiver
        .attach(&common::source(), None, inbox.clone())
        .await
        .unwrap();
    assert!(backings().is_empty());
    let sender = Arc::new(Iceoryx2PreparedCore::new(config, &catalog, 4, 1).unwrap());
    let physical = backings();
    assert_eq!(physical.len(), 1);
    sender.wait_ready().await.unwrap();
    let route = OpaqueTemplate::for_wire::<up_rust::ProtobufWire>()
        .unwrap()
        .route(
            Arc::clone(&sender),
            common::source(),
            None,
            common::limits(0),
            RouteConfig::default(),
        )
        .unwrap();
    let metadata = UFrameMetadata::publish(common::source())
        .with_payload_encoding(PayloadEncoding::PROTOBUF)
        .build()
        .unwrap();
    route
        .send_bytes(TxHeaderParts::from(&metadata), Some(&[]))
        .unwrap();
    let raw = tokio::time::timeout(Duration::from_secs(5), inbox.recv())
        .await
        .unwrap()
        .unwrap();
    let admission =
        HeaderAdmission::for_wire::<up_rust::ProtobufWire>(HeaderLimits::default()).unwrap();
    let frame = admission.admit(raw).unwrap();
    assert!(frame.payload().unwrap().bytes().is_empty());
    assert_eq!(backings(), physical);
    drop(frame);
    receiver.detach(&mut subscription).unwrap();
}
