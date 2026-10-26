// Copyright (c) 2026 Contributors to the Eclipse Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use up_rust::{
    NativeProfile, NativeProfileAgreement, NativeProfileMode, NativeProfileTable, OverflowPolicy,
    PayloadEncoding, RegistrationConfig, RouteDefinition, RouteLimits, StablePayload, UMessageType,
    UUri,
};
use up_transport_iceoryx2_rust::{
    Iceoryx2PreparedCore, Iceoryx2PubSubConfig, Iceoryx2RouteReservation,
};

#[repr(C)]
#[derive(up_rust::StablePayload, up_rust::StablePayloadInit)]
#[stable_payload(type_name = "uprotocol.test.Iceoryx2PreparedValue")]
pub struct Value {
    pub sequence: u32,
    pub bytes: [u8; 64],
}

#[repr(C)]
#[derive(up_rust::StablePayload, up_rust::StablePayloadInit)]
#[stable_payload(type_name = "uprotocol.test.Iceoryx2ForeignValue")]
pub struct Foreign {
    pub sequence: u32,
    pub bytes: [u8; 64],
}

pub const TRACE: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
static NEXT: AtomicUsize = AtomicUsize::new(0);

pub fn agreement() -> NativeProfileAgreement {
    let table = NativeProfileTable::new([
        (
            PayloadEncoding::private_use(0xF101),
            Value::native_representation(),
        ),
        (
            PayloadEncoding::private_use(0xF102),
            Foreign::native_representation(),
        ),
    ])
    .unwrap();
    let profile = NativeProfile::new(
        "iceoryx2-prepared-tests",
        1,
        NativeProfileMode::Table(table),
    )
    .unwrap();
    NativeProfileAgreement::new(Arc::new(profile.clone()), &profile).unwrap()
}

pub fn source() -> UUri {
    UUri::try_from_parts("prepared", 0x4210, 1, 0x9000).unwrap()
}

pub fn definition(source: UUri) -> RouteDefinition {
    RouteDefinition {
        kind: UMessageType::Publish,
        source,
        sink: None,
        priority: None,
        ttl: None,
        permission_level: None,
    }
}

pub fn limits(payload: usize) -> RouteLimits {
    RouteLimits {
        max_metadata_len: 3072,
        max_payload_len: payload,
        payload_alignment: 8,
    }
}

pub fn config(slice: usize, subscribers: usize) -> Iceoryx2PubSubConfig {
    let root = std::env::var("UP_ICEORYX2_TEST_ROOT").expect("real native test root is required");
    std::fs::create_dir_all(&root).unwrap();
    let prefix = format!(
        "prepared{}_{}_",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    );
    let mut config = Iceoryx2PubSubConfig::static_allocation(slice)
        .with_namespace(&root, &prefix)
        .unwrap();
    config.require_exact_service_limits = true;
    let native = &mut config
        .iceoryx2_config
        .as_mut()
        .unwrap()
        .defaults
        .publish_subscribe;
    native.max_subscribers = subscribers;
    native.subscriber_max_buffer_size = 32;
    native.subscriber_max_borrowed_samples = 8;
    native.publisher_history_size = 0;
    native.publisher_max_loaned_samples = 1;
    native.enable_safe_overflow = false;
    config
}

pub fn core(payload: usize, subscribers: usize) -> Arc<Iceoryx2PreparedCore> {
    Arc::new(
        Iceoryx2PreparedCore::new(
            config(payload + 4096, subscribers),
            &[Iceoryx2RouteReservation {
                source: source(),
                sink: None,
                limits: limits(payload),
            }],
            16,
            subscribers,
        )
        .unwrap(),
    )
}

pub fn registration() -> RegistrationConfig {
    RegistrationConfig {
        capacity: 4,
        overflow: OverflowPolicy::Block {
            deadline: Duration::from_secs(5),
        },
    }
}

pub fn wait(counter: &AtomicUsize, expected: usize) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while counter.load(Ordering::Acquire) != expected {
        assert!(
            Instant::now() < deadline,
            "native receiver did not reach {expected}"
        );
        std::thread::yield_now();
    }
}
