// Copyright (c) 2026 Contributors to the Eclipse Foundation
// SPDX-License-Identifier: Apache-2.0
//! Native static backing-page allocation and unchanged slot/loan limits.
#![cfg(target_os = "linux")]
#[path = "support/allocating_control.rs"]
mod control;
use control::Core as Iceoryx2PubSub;
use std::sync::atomic::{AtomicUsize, Ordering};
use up_rust::{PayloadEncoding, ProtobufWire, UCode, UFrameMetadata, UTxBuffer, UTxLoanSpec, UUri};
use up_transport_iceoryx2_rust::Iceoryx2PubSubConfig;

static SEQUENCE: AtomicUsize = AtomicUsize::new(0);

fn config(enabled: bool, bytes: usize) -> Iceoryx2PubSubConfig {
    let root = std::env::var("UP_ICEORYX2_TEST_ROOT").expect("native test root");
    std::fs::create_dir_all(&root).unwrap();
    let prefix = format!(
        "warm{}_{}_",
        std::process::id(),
        SEQUENCE.fetch_add(1, Ordering::Relaxed)
    );
    let mut config = Iceoryx2PubSubConfig::static_allocation(bytes)
        .with_namespace(&root, &prefix)
        .unwrap();
    config.require_exact_service_limits = true;
    config.publisher_prewarm = enabled;
    let native = &mut config
        .iceoryx2_config
        .as_mut()
        .unwrap()
        .defaults
        .publish_subscribe;
    native.max_subscribers = 2;
    native.subscriber_max_buffer_size = 2;
    native.subscriber_max_borrowed_samples = 2;
    native.publisher_history_size = 0;
    native.publisher_max_loaned_samples = 1;
    config
}

fn spec(bytes: usize) -> UTxLoanSpec {
    UTxLoanSpec::payload(
        UFrameMetadata::publish(UUri::try_from_parts("pool", 0x4210, 1, 0x9000).unwrap())
            .with_payload_encoding(PayloadEncoding::PROTOBUF)
            .build()
            .unwrap(),
        bytes,
        8,
    )
    .unwrap()
}

fn backing_path(address: usize) -> String {
    for line in std::fs::read_to_string("/proc/self/maps").unwrap().lines() {
        let fields: Vec<_> = line.split_whitespace().collect();
        let (start, end) = fields[0].split_once('-').unwrap();
        let start = usize::from_str_radix(start, 16).unwrap();
        let end = usize::from_str_radix(end, 16).unwrap();
        if (start..end).contains(&address) {
            return fields[5..].join(" ");
        }
    }
    panic!("native backing mapping missing");
}

#[test]
fn prewarming_rejects_nonexact_and_dynamic_setup() {
    let mut invalid = config(true, 4096);
    invalid.require_exact_service_limits = false;
    assert_eq!(
        invalid.validate().unwrap_err().code(),
        UCode::InvalidArgument
    );
    invalid.require_exact_service_limits = true;
    invalid.publisher_allocation_strategy = iceoryx2::prelude::AllocationStrategy::PowerOfTwo;
    assert_eq!(
        invalid.validate().unwrap_err().code(),
        UCode::InvalidArgument
    );
}

#[tokio::test]
async fn entire_native_static_arena_is_resident_without_increasing_tx_limit() {
    let tool = std::env::var("UP_ICEORYX2_FINCORE_BIN").expect("recorded native fincore tool");
    for slice_bytes in [1024 * 1024, 4_915_470, 12_441_926] {
        for enabled in [false, true] {
            let transport = Iceoryx2PubSub::try_with_config(config(enabled, slice_bytes))
                .unwrap()
                .with_selected_wire(ProtobufWire);
            let mut loan = transport.loan_validated_tx(spec(16)).await.unwrap();
            loan.payload_mut().fill(0x5a);
            let path = backing_path(loan.payload().as_ptr() as usize);
            let command = [
                "--json",
                "--bytes",
                "--output",
                "PAGES,RES,SIZE,FILE",
                &path,
            ];
            let output = std::process::Command::new(&tool)
                .args(command)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            let raw = String::from_utf8(output.stdout).unwrap();
            let value: serde_json::Value = serde_json::from_str(&raw).unwrap();
            let file = &value["fincore"][0];
            let size = file["size"].as_u64().unwrap();
            let resident = file["res"].as_u64().unwrap();
            assert!(size >= 9 * slice_bytes as u64);
            if enabled {
                assert!(resident >= size, "whole native pool not resident: {raw}");
            } else {
                assert!(
                    resident < size / 2,
                    "ordinary sparse default changed: {raw}"
                );
            }
            assert_eq!(loan.payload(), &[0x5a; 16]);
            assert_eq!(
                transport
                    .loan_validated_tx(spec(16))
                    .await
                    .err()
                    .expect("TX1 must remain enforced")
                    .code(),
                UCode::ResourceExhausted
            );
            println!(
                "NATIVE_POOL_PAGE_PROOF enabled={enabled} slice_bytes={slice_bytes} raw={}",
                raw.replace('\n', " ")
            );
            drop(loan);
            drop(transport.loan_validated_tx(spec(16)).await.unwrap());
        }
    }
}
