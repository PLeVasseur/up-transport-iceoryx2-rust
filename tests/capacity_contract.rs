// Copyright (c) 2026 Contributors to the Eclipse Foundation
// SPDX-License-Identifier: Apache-2.0
//! Actual native service/loan limits, including mismatched existing services.
use iceoryx2::{
    node::NodeBuilder,
    prelude::ServiceName,
    service::{ipc_threadsafe, port_factory::PortFactory as _},
};
use std::{
    sync::Arc,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use up_rust::{
    ExactUUri, PayloadEncoding, ProtobufWire, UCode, UFrameMetadata, UFrameView,
    UNativePrefixWireTransport, UTxBuffer, UTxLoanSpec, UUri, UZeroCopyListener,
    UZeroCopyTransportImpl,
};
use up_transport_iceoryx2_rust::{Iceoryx2PubSub, Iceoryx2PubSubConfig, UProtocolHeader};

static SEQUENCE: AtomicU64 = AtomicU64::new(1);

#[tokio::test(flavor = "multi_thread")]
async fn logical_filters_delegate_to_independent_native_subscribers() {
    let mut config = config();
    config
        .iceoryx2_config
        .as_mut()
        .unwrap()
        .defaults
        .publish_subscribe
        .max_subscribers = 3;
    config.publisher_min_subscribers = 3; // Two sinkless filters and one directed filter.
    let receiver = Iceoryx2PubSub::try_with_config(config.clone())
        .unwrap()
        .with_selected_wire(ProtobufWire);
    let sender = Iceoryx2PubSub::try_with_config(config)
        .unwrap()
        .with_selected_wire(ProtobufWire);
    let (a, mut a_rx) = tokio::sync::mpsc::unbounded_channel();
    let (b, mut b_rx) = tokio::sync::mpsc::unbounded_channel();
    let (directed, mut directed_rx) = tokio::sync::mpsc::unbounded_channel();
    let a: Arc<dyn UZeroCopyListener<Frame>> = Arc::new(Capture(a));
    let b: Arc<dyn UZeroCopyListener<Frame>> = Arc::new(Capture(b));
    let directed: Arc<dyn UZeroCopyListener<Frame>> = Arc::new(Capture(directed));
    let sink = UUri::try_from_parts("receiver", 0x4210, 1, 0).unwrap();
    receiver
        .register_validated_zero_copy_listener(&source(), None, a.clone())
        .await
        .unwrap();
    receiver
        .register_validated_zero_copy_listener(&source(), None, b.clone())
        .await
        .unwrap();
    receiver
        .register_validated_zero_copy_listener(&UUri::any(), Some(&sink), directed.clone())
        .await
        .unwrap();
    let mut tx = sender.loan_validated_tx(spec(4)).await.unwrap();
    tx.payload_mut().copy_from_slice(b"both");
    sender.send_validated_zero_copy(tx).await.unwrap();
    let first = tokio::time::timeout(Duration::from_secs(2), a_rx.recv())
        .await
        .unwrap()
        .unwrap();
    let second = tokio::time::timeout(Duration::from_secs(2), b_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.metadata().id(), second.metadata().id());
    #[cfg(target_os = "linux")]
    assert_eq!(
        native_backing(first.try_contiguous_payload().unwrap()),
        native_backing(second.try_contiguous_payload().unwrap())
    );
    assert_eq!(first.try_contiguous_payload(), Some(b"both".as_slice()));
    assert_eq!(second.try_contiguous_payload(), Some(b"both".as_slice()));
    assert!(directed_rx.try_recv().is_err()); // None is sinkless, not a wildcard.
    receiver
        .unregister_validated_zero_copy_listener(&source(), None, a)
        .await
        .unwrap();
    drop(first);
    receiver
        .unregister_validated_zero_copy_listener(&source(), None, b)
        .await
        .unwrap();
    receiver
        .unregister_validated_zero_copy_listener(&UUri::any(), Some(&sink), directed)
        .await
        .unwrap();
    drop(receiver);
    drop(sender);
    assert_eq!(second.try_contiguous_payload(), Some(b"both".as_slice()));
}

// Compare shared object + offset, not virtual addresses of separate native maps.
#[cfg(target_os = "linux")]
fn native_backing(bytes: &[u8]) -> (String, u64, u64) {
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
        if start <= address && address + bytes.len() as u64 <= end {
            assert!(
                permissions.ends_with('s'),
                "payload must remain in shared mapping"
            );
            assert_ne!(
                inode, 0,
                "payload must retain its native shared-memory backing"
            );
            return (device.into(), inode, offset + address - start);
        }
    }
    panic!("native payload mapping missing");
}

async fn retained_sample_under_publisher_pressure(listener_mode: bool) {
    let config = config();
    let publisher = Iceoryx2PubSub::try_with_config(config.clone())
        .unwrap()
        .with_selected_wire(ProtobufWire);
    let receiver = Iceoryx2PubSub::try_with_config(config.clone())
        .unwrap()
        .with_selected_wire(ProtobufWire);
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let listener: Arc<dyn UZeroCopyListener<Frame>> = Arc::new(Capture(tx));
    if listener_mode {
        receiver
            .register_validated_zero_copy_listener(&source(), None, listener.clone())
            .await
            .unwrap();
    } else {
        assert_eq!(
            receiver
                .receive_validated_zero_copy(&source(), None)
                .await
                .unwrap_err()
                .code(),
            UCode::NotFound
        );
    }
    let mut tx = publisher.loan_validated_tx(spec(4)).await.unwrap();
    tx.payload_mut().copy_from_slice(b"kept");
    publisher.send_validated_zero_copy(tx).await.unwrap();
    let retained = if listener_mode {
        tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap()
    } else {
        receiver
            .receive_validated_zero_copy(&source(), None)
            .await
            .unwrap()
    };
    let address = retained.try_contiguous_payload().unwrap().as_ptr() as usize;
    let observer = NodeBuilder::new()
        .config(config.iceoryx2_config.as_ref().unwrap())
        .create::<ipc_threadsafe::Service>()
        .unwrap();
    let service_name = Iceoryx2PubSub::exact_source_publish_subscribe_service_name(
        &ExactUUri::try_from(source()).unwrap(),
    )
    .unwrap();
    let service = observer
        .service_builder(&ServiceName::new(&service_name).unwrap())
        .publish_subscribe::<[u8]>()
        .user_header::<UProtocolHeader>()
        .open()
        .unwrap();
    if listener_mode {
        receiver
            .unregister_validated_zero_copy_listener(&source(), None, listener)
            .await
            .unwrap();
    }
    drop(receiver);
    // Allow the weak dispatcher to retire; inspect the actual native port count
    // rather than mistaking a still-running callback/worker for lease ownership.
    tokio::time::sleep(Duration::from_millis(100)).await;
    println!(
        "retained native subscribers before reuse: {}",
        service.dynamic_config().number_of_subscribers()
    );
    // If the old endpoint incorrectly released its registration, a replacement
    // can occupy it. Retain two new samples and fill its queue to exercise every
    // slot, rather than repeatedly recycling one free bucket in a LIFO allocator.
    let mut pressure_samples = Vec::new();
    let pressure = if service.dynamic_config().number_of_subscribers() == 0 {
        let pressure = Iceoryx2PubSub::try_with_config(config.clone())
            .unwrap()
            .with_selected_wire(ProtobufWire);
        assert_eq!(
            pressure
                .receive_validated_zero_copy(&source(), None)
                .await
                .unwrap_err()
                .code(),
            UCode::NotFound
        );
        for value in 0..2_u32 {
            let mut tx = publisher.loan_validated_tx(spec(4)).await.unwrap();
            tx.payload_mut().copy_from_slice(&value.to_ne_bytes());
            publisher.send_validated_zero_copy(tx).await.unwrap();
            pressure_samples.push(
                pressure
                    .receive_validated_zero_copy(&source(), None)
                    .await
                    .unwrap(),
            );
        }
        Some(pressure)
    } else {
        None
    };
    // Do not retain a borrowed &[u8] across this deliberately adversarial baseline.
    // Native writes finish before the retained sample is inspected again.
    for value in 0..256_u32 {
        let mut tx = publisher.loan_validated_tx(spec(4)).await.unwrap();
        tx.payload_mut().copy_from_slice(&value.to_ne_bytes());
        publisher.send_validated_zero_copy(tx).await.unwrap();
    }
    assert_eq!(
        retained.try_contiguous_payload(),
        Some(b"kept".as_slice()),
        "live native sample was recycled after endpoint destruction"
    );
    assert_eq!(service.dynamic_config().number_of_subscribers(), 1);
    drop(pressure_samples);
    drop(pressure);
    assert_eq!(
        retained.try_contiguous_payload().unwrap().as_ptr() as usize,
        address
    );
    let retained = std::thread::spawn(move || {
        assert_eq!(retained.try_contiguous_payload(), Some(b"kept".as_slice()));
        retained
    })
    .join()
    .unwrap();
    let replacement = Iceoryx2PubSub::try_with_config(config)
        .unwrap()
        .with_selected_wire(ProtobufWire);
    assert_eq!(
        replacement
            .receive_validated_zero_copy(&source(), None)
            .await
            .unwrap_err()
            .code(),
        UCode::Internal,
        "retained sample must keep its native subscriber registration alive"
    );
    drop(retained);
    assert_eq!(
        replacement
            .receive_validated_zero_copy(&source(), None)
            .await
            .unwrap_err()
            .code(),
        UCode::NotFound,
        "final lease release must restore the native subscriber slot"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn retained_listener_sample_survives_continued_publication_after_unregister() {
    retained_sample_under_publisher_pressure(true).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn retained_pull_sample_survives_continued_publication_after_endpoint_drop() {
    retained_sample_under_publisher_pressure(false).await;
}

type Transport = UNativePrefixWireTransport<Iceoryx2PubSub, ProtobufWire>;
type Frame = <Transport as UZeroCopyTransportImpl>::Rx;

struct Capture(tokio::sync::mpsc::UnboundedSender<Frame>);

#[async_trait::async_trait]
impl UZeroCopyListener<Frame> for Capture {
    async fn on_receive_zero_copy(&self, frame: Frame) {
        self.0.send(frame).unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn strict_two_subscriber_fanout_retains_independent_native_loans() {
    let mut config = config();
    config
        .iceoryx2_config
        .as_mut()
        .unwrap()
        .defaults
        .publish_subscribe
        .max_subscribers = 2;
    config.publisher_min_subscribers = 2;
    let publisher = Iceoryx2PubSub::try_with_config(config.clone())
        .unwrap()
        .with_selected_wire(ProtobufWire);
    let first = Iceoryx2PubSub::try_with_config(config.clone())
        .unwrap()
        .with_selected_wire(ProtobufWire);
    let second = Iceoryx2PubSub::try_with_config(config.clone())
        .unwrap()
        .with_selected_wire(ProtobufWire);
    let (first_tx, mut first_rx) = tokio::sync::mpsc::unbounded_channel();
    let (second_tx, mut second_rx) = tokio::sync::mpsc::unbounded_channel();
    let first_listener: Arc<dyn UZeroCopyListener<Frame>> = Arc::new(Capture(first_tx));
    let second_listener: Arc<dyn UZeroCopyListener<Frame>> = Arc::new(Capture(second_tx));
    first
        .register_validated_zero_copy_listener(&source(), None, first_listener.clone())
        .await
        .unwrap();
    second
        .register_validated_zero_copy_listener(&source(), None, second_listener.clone())
        .await
        .unwrap();
    let mut tx = publisher.loan_validated_tx(spec(4)).await.unwrap();
    tx.payload_mut().copy_from_slice(b"both");
    publisher.send_validated_zero_copy(tx).await.unwrap();
    let first_frame = tokio::time::timeout(Duration::from_secs(2), first_rx.recv())
        .await
        .unwrap()
        .unwrap();
    let second_frame = tokio::time::timeout(Duration::from_secs(2), second_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first_frame.metadata().id(), second_frame.metadata().id());
    let first_address = first_frame.try_contiguous_payload().unwrap().as_ptr();
    let second_address = second_frame.try_contiguous_payload().unwrap().as_ptr();
    assert_eq!(
        first_frame.try_contiguous_payload(),
        Some(b"both".as_slice())
    );
    assert_eq!(
        second_frame.try_contiguous_payload(),
        Some(b"both".as_slice())
    );
    let excess = Iceoryx2PubSub::try_with_config(config)
        .unwrap()
        .with_selected_wire(ProtobufWire);
    assert_eq!(
        excess
            .receive_validated_zero_copy(&source(), None)
            .await
            .unwrap_err()
            .code(),
        UCode::Internal
    );
    first
        .unregister_validated_zero_copy_listener(&source(), None, first_listener)
        .await
        .unwrap();
    drop(first);
    assert_eq!(
        first_frame.try_contiguous_payload().unwrap().as_ptr(),
        first_address
    );
    drop(first_frame);
    assert_eq!(
        second_frame.try_contiguous_payload().unwrap().as_ptr(),
        second_address
    );
    assert_eq!(
        second_frame.try_contiguous_payload(),
        Some(b"both".as_slice())
    );
    second
        .unregister_validated_zero_copy_listener(&source(), None, second_listener)
        .await
        .unwrap();
    drop(second);
    drop(publisher);
    assert_eq!(
        second_frame.try_contiguous_payload().unwrap().as_ptr(),
        second_address
    );
    assert_eq!(
        second_frame.try_contiguous_payload(),
        Some(b"both".as_slice())
    );
}

fn config() -> Iceoryx2PubSubConfig {
    let root = std::env::var("UP_ICEORYX2_TEST_ROOT").unwrap_or_else(|_| {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target/capacity-runtime")
            .to_string_lossy()
            .into_owned()
    });
    std::fs::create_dir_all(&root).unwrap();
    let prefix = format!(
        "cap{}_{}_",
        std::process::id(),
        SEQUENCE.fetch_add(1, Ordering::Relaxed)
    );
    let mut config = Iceoryx2PubSubConfig::static_allocation(4096)
        .with_namespace(&root, &prefix)
        .unwrap();
    config.require_exact_service_limits = true;
    let limits = &mut config
        .iceoryx2_config
        .as_mut()
        .unwrap()
        .defaults
        .publish_subscribe;
    limits.max_subscribers = 1;
    limits.subscriber_max_buffer_size = 2;
    limits.subscriber_max_borrowed_samples = 2;
    limits.publisher_max_loaned_samples = 1;
    limits.publisher_history_size = 0;
    config
}

fn source() -> UUri {
    UUri::try_from_parts("capacity", 0x4210, 1, 0x9001).unwrap()
}

fn spec(bytes: usize) -> UTxLoanSpec {
    UTxLoanSpec::payload(
        UFrameMetadata::publish(source())
            .with_payload_encoding(PayloadEncoding::PROTOBUF)
            .build()
            .unwrap(),
        bytes,
        1,
    )
    .unwrap()
}

#[test]
fn invalid_and_overflowing_limits_fail_before_native_creation() {
    let valid = config();
    for field in 0..6 {
        let mut bad = valid.clone();
        match field {
            0 => bad.publisher_initial_max_slice_len = 0,
            1 => {
                bad.iceoryx2_config
                    .as_mut()
                    .unwrap()
                    .defaults
                    .publish_subscribe
                    .max_subscribers = 0
            }
            2 => {
                bad.iceoryx2_config
                    .as_mut()
                    .unwrap()
                    .defaults
                    .publish_subscribe
                    .subscriber_max_buffer_size = 0
            }
            3 => {
                bad.iceoryx2_config
                    .as_mut()
                    .unwrap()
                    .defaults
                    .publish_subscribe
                    .subscriber_max_borrowed_samples = 0
            }
            4 => {
                bad.iceoryx2_config
                    .as_mut()
                    .unwrap()
                    .defaults
                    .publish_subscribe
                    .publisher_max_loaned_samples = 0
            }
            5 => bad.publisher_min_subscribers = 2,
            _ => unreachable!(),
        }
        assert_eq!(
            Iceoryx2PubSub::try_with_config(bad).unwrap_err().code(),
            UCode::InvalidArgument
        );
    }
    let mut overflow = valid.clone();
    overflow
        .iceoryx2_config
        .as_mut()
        .unwrap()
        .defaults
        .publish_subscribe
        .max_subscribers = usize::MAX;
    assert_eq!(
        Iceoryx2PubSub::try_with_config(overflow)
            .unwrap_err()
            .code(),
        UCode::OutOfRange
    );
    let mut overflow = valid;
    overflow.publisher_initial_max_slice_len = usize::MAX;
    assert_eq!(
        Iceoryx2PubSub::try_with_config(overflow)
            .unwrap_err()
            .code(),
        UCode::OutOfRange
    );
}

#[tokio::test]
async fn strict_static_capacity_never_grows_for_first_or_later_loans() {
    let transport = Iceoryx2PubSub::try_with_config(config())
        .unwrap()
        .with_selected_wire(ProtobufWire);
    assert_eq!(
        transport
            .loan_validated_tx(spec(4096))
            .await
            .err()
            .expect("oversized first loan rejects")
            .code(),
        UCode::ResourceExhausted
    );
    // Metadata makes the required physical slice larger than the payload alone.
    drop(transport.loan_validated_tx(spec(4)).await.unwrap());
    assert_eq!(
        transport
            .loan_validated_tx(spec(4096))
            .await
            .err()
            .expect("oversized later loan rejects")
            .code(),
        UCode::ResourceExhausted
    );
}

#[tokio::test]
async fn mismatched_existing_service_rejects_both_publisher_and_subscriber() {
    let exact = config();
    let mut wide = exact.clone();
    wide.iceoryx2_config
        .as_mut()
        .unwrap()
        .defaults
        .publish_subscribe
        .max_subscribers = 8;
    let creator = Iceoryx2PubSub::try_with_config(wide)
        .unwrap()
        .with_selected_wire(ProtobufWire);
    drop(creator.loan_validated_tx(spec(4)).await.unwrap());
    let mismatch = Iceoryx2PubSub::try_with_config(exact)
        .unwrap()
        .with_selected_wire(ProtobufWire);
    assert_eq!(
        mismatch
            .loan_validated_tx(spec(4))
            .await
            .err()
            .expect("mismatched service rejects publisher")
            .code(),
        UCode::FailedPrecondition
    );
    assert_eq!(
        mismatch
            .receive_validated_zero_copy(&source(), None)
            .await
            .unwrap_err()
            .code(),
        UCode::FailedPrecondition
    );
}

#[tokio::test]
async fn native_service_and_live_loan_limits_match_the_five_slot_contract() {
    let config = config();
    let publisher = Iceoryx2PubSub::try_with_config(config.clone())
        .unwrap()
        .with_selected_wire(ProtobufWire);
    let subscriber = Iceoryx2PubSub::try_with_config(config.clone())
        .unwrap()
        .with_selected_wire(ProtobufWire);
    assert_eq!(
        subscriber
            .receive_validated_zero_copy(&source(), None)
            .await
            .unwrap_err()
            .code(),
        UCode::NotFound
    );
    let native = NodeBuilder::new()
        .config(config.iceoryx2_config.as_ref().unwrap())
        .create::<ipc_threadsafe::Service>()
        .unwrap();
    let name = Iceoryx2PubSub::exact_source_publish_subscribe_service_name(
        &ExactUUri::try_from(source()).unwrap(),
    )
    .unwrap();
    let service = native
        .service_builder(&ServiceName::new(&name).unwrap())
        .publish_subscribe::<[u8]>()
        .user_header::<UProtocolHeader>()
        .open()
        .unwrap();
    let actual = service.static_config();
    assert_eq!(actual.max_subscribers(), 1);
    assert_eq!(actual.subscriber_max_buffer_size(), 2);
    assert_eq!(actual.subscriber_max_borrowed_samples(), 2);
    assert_eq!(actual.history_size(), 0);
    assert_eq!(
        actual.max_subscribers()
            * (actual.subscriber_max_buffer_size() + actual.subscriber_max_borrowed_samples())
            + actual.history_size()
            + 1,
        5
    );

    let mut first = publisher.loan_validated_tx(spec(4)).await.unwrap();
    assert_eq!(
        publisher
            .loan_validated_tx(spec(4))
            .await
            .err()
            .expect("second live TX loan exceeds configured limit")
            .code(),
        UCode::ResourceExhausted
    );
    first.payload_mut().copy_from_slice(b"live");
    publisher.send_validated_zero_copy(first).await.unwrap();
    let received = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match subscriber
                .receive_validated_zero_copy(&source(), None)
                .await
            {
                Ok(frame) => break frame,
                Err(error) if error.code() == UCode::NotFound => {
                    tokio::time::sleep(Duration::from_millis(10)).await
                }
                Err(error) => panic!("native receive failed: {error}"),
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(received.try_contiguous_payload(), Some(b"live".as_slice()));
    let excess = Iceoryx2PubSub::try_with_config(config)
        .unwrap()
        .with_selected_wire(ProtobufWire);
    assert_eq!(
        excess
            .receive_validated_zero_copy(&source(), None)
            .await
            .unwrap_err()
            .code(),
        UCode::Internal // Native port creation rejects the second subscriber, not an empty receive.
    );
}
