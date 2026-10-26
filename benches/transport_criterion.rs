// ################################################################################
// Copyright (c) 2026 Contributors to the Eclipse Foundation
//
// SPDX-License-Identifier: Apache-2.0
// ################################################################################

//! Criterion measurements of selected-wire publish and receive over isolated iceoryx2 services.
use std::{sync::atomic::AtomicU64, sync::atomic::Ordering, time::Duration};

use criterion::{Criterion, black_box, criterion_main};
use iceoryx2::prelude::SemanticString;
use iceoryx2_bb_system_types::{file_name::FileName, path::Path};
use std::sync::Arc;
use tokio::runtime::Runtime;
use up_rust::{
    Frame, OpaqueTemplate, OverflowPolicy, PayloadEncoding, PreparedTransportCore, ProtobufWire,
    RegistrationConfig, RegistrationInbox, RouteConfig, RouteLimits, TxHeaderParts, UFrameMetadata,
    UUri,
};
use up_transport_iceoryx2_rust::{
    Iceoryx2PreparedCore, Iceoryx2PreparedRx, Iceoryx2PubSubConfig, Iceoryx2RouteReservation,
};

const PAYLOAD_LEN: usize = 256;
static BENCH_SEQUENCE: AtomicU64 = AtomicU64::new(1);

fn benchmark_config() -> iceoryx2::prelude::Config {
    let root = std::path::PathBuf::from(
        std::env::var("UP_ICEORYX2_TEST_ROOT")
            .expect("provide a short isolated native event root for the benchmark"),
    );
    std::fs::create_dir_all(&root).expect("create durable iceoryx2 benchmark root");
    let sequence = BENCH_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let prefix = format!("b{}_{}", std::process::id(), sequence);

    let mut config = iceoryx2::prelude::Config::default();
    config.global.set_root_path(
        &Path::new(root.as_os_str().as_encoded_bytes()).expect("iceoryx2 benchmark root path"),
    );
    config.global.prefix = FileName::new(prefix.as_bytes()).expect("iceoryx2 benchmark prefix");
    config
}

fn source() -> UUri {
    let authority = format!("iox-r19-benchmark-{}", std::process::id());
    UUri::try_from_parts(&authority, 0x4210, 1, 0x9000).expect("benchmark source URI")
}

fn selected_wire_round_trip(c: &mut Criterion) {
    let runtime = Runtime::new().expect("benchmark runtime");
    let config = benchmark_config();
    let source = source();
    let limits = RouteLimits {
        max_metadata_len: 1024,
        max_payload_len: PAYLOAD_LEN,
        payload_alignment: 8,
    };
    let core = Arc::new(
        Iceoryx2PreparedCore::new(
            Iceoryx2PubSubConfig::static_allocation(64 * 1024).with_iceoryx2_config(config),
            &[Iceoryx2RouteReservation {
                source: source.clone(),
                sink: None,
                limits,
            }],
            1,
            1,
        )
        .unwrap(),
    );
    let inbox = Arc::new(
        RegistrationInbox::<Iceoryx2PreparedRx>::new(RegistrationConfig {
            capacity: 1,
            overflow: OverflowPolicy::Block {
                deadline: Duration::from_secs(5),
            },
        })
        .unwrap(),
    );
    let mut subscription = runtime
        .block_on(core.attach(&source, None, inbox.clone()))
        .unwrap();
    runtime.block_on(core.wait_ready()).unwrap();
    let publisher = OpaqueTemplate::for_wire::<ProtobufWire>()
        .unwrap()
        .route(
            Arc::clone(&core),
            source.clone(),
            None,
            limits,
            RouteConfig::default(),
        )
        .unwrap();
    let payload = [0x5a; PAYLOAD_LEN];

    c.bench_function("selected_wire_protobuf_real_shm_256b", |b| {
        b.iter(|| {
            runtime.block_on(async {
                let metadata = UFrameMetadata::publish(source.clone())
                    .with_payload_encoding(PayloadEncoding::PROTOBUF)
                    .build()
                    .expect("benchmark metadata");
                publisher
                    .send_bytes(TxHeaderParts::from(&metadata), Some(&payload))
                    .expect("benchmark send");
                let frame = tokio::time::timeout(Duration::from_secs(5), inbox.recv())
                    .await
                    .unwrap()
                    .unwrap();
                let bytes = frame.payload().unwrap().bytes();
                assert_eq!(bytes, &payload);
                black_box(bytes.len());
            });
        });
    });
    core.detach(&mut subscription).unwrap();
}

fn benches() {
    let mut criterion = Criterion::default().configure_from_args();
    selected_wire_round_trip(&mut criterion);
}
criterion_main!(benches);
