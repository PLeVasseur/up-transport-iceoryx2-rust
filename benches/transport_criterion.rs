// ################################################################################
// Copyright (c) 2026 Contributors to the Eclipse Foundation
//
// SPDX-License-Identifier: Apache-2.0
// ################################################################################

use std::{sync::atomic::AtomicU64, sync::atomic::Ordering, time::Duration};

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use iceoryx2::prelude::SemanticString;
use iceoryx2_bb_system_types::{file_name::FileName, path::Path};
use tokio::runtime::Runtime;
use up_rust::{
    PayloadEncoding, ProtobufWire, UFrameMetadata, UFrameView, UTxBuffer, UTxLoanSpec, UUri,
    UZeroCopyTransportImpl,
};
use up_transport_iceoryx2_rust::{Iceoryx2PubSub, Iceoryx2PubSubConfig};

const PAYLOAD_LEN: usize = 256;
static BENCH_SEQUENCE: AtomicU64 = AtomicU64::new(1);

fn benchmark_config() -> iceoryx2::prelude::Config {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("i2-bench");
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

async fn receive_payload(
    transport: &up_rust::UNativePrefixWireTransport<Iceoryx2PubSub, ProtobufWire>,
    source: &UUri,
) -> usize {
    for _ in 0..100 {
        match transport.receive_validated_zero_copy(source, None).await {
            Ok(frame) => return frame.payload_len(),
            Err(status) if status.code() == up_rust::UCode::NotFound => {
                tokio::time::sleep(Duration::from_micros(50)).await;
            }
            Err(status) => panic!("iceoryx2 benchmark receive failed: {status:?}"),
        }
    }
    panic!("iceoryx2 benchmark receive timed out")
}

fn selected_wire_round_trip(c: &mut Criterion) {
    let runtime = Runtime::new().expect("benchmark runtime");
    let config = benchmark_config();
    let publisher = Iceoryx2PubSub::with_config(
        Iceoryx2PubSubConfig::static_allocation(64 * 1024).with_iceoryx2_config(config.clone()),
    )
    .with_selected_wire(ProtobufWire);
    let subscriber = Iceoryx2PubSub::with_config(
        Iceoryx2PubSubConfig::static_allocation(64 * 1024).with_iceoryx2_config(config),
    )
    .with_selected_wire(ProtobufWire);
    let source = source();
    let first_receive = runtime.block_on(subscriber.receive_validated_zero_copy(&source, None));
    assert_eq!(
        first_receive
            .expect_err("subscriber should initially be empty")
            .code(),
        up_rust::UCode::NotFound
    );

    c.bench_function("selected_wire_protobuf_real_shm_256b", |b| {
        b.iter(|| {
            runtime.block_on(async {
                let metadata = UFrameMetadata::publish(source.clone())
                    .with_payload_encoding(PayloadEncoding::PROTOBUF)
                    .build()
                    .expect("benchmark metadata");
                let mut tx = publisher
                    .loan_validated_tx(
                        UTxLoanSpec::payload(metadata, PAYLOAD_LEN, 1)
                            .expect("benchmark loan spec"),
                    )
                    .await
                    .expect("benchmark loan");
                tx.payload_mut().fill(0x5a);
                publisher
                    .send_validated_zero_copy(tx)
                    .await
                    .expect("benchmark send");
                black_box(receive_payload(&subscriber, &source).await);
            });
        });
    });
}

criterion_group!(benches, selected_wire_round_trip);
criterion_main!(benches);
