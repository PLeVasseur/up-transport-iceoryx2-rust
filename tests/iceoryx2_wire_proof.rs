// ################################################################################
// Copyright (c) 2026 Contributors to the Eclipse Foundation
//
// SPDX-License-Identifier: Apache-2.0
// ################################################################################

use std::{
    sync::{Arc, Mutex as StdMutex, atomic::AtomicU64, atomic::Ordering},
    time::Duration,
};

use async_trait::async_trait;
use iceoryx2::{
    node::NodeBuilder,
    prelude::{AllocationStrategy, SemanticString, ServiceName},
    service::ipc_threadsafe,
};
use iceoryx2_bb_system_types::{file_name::FileName, path::Path};
use tokio::sync::{Mutex as TokioMutex, MutexGuard};
use up_rust::selected_wire_user_api::UNativePrefixWireTransport;
use up_rust::{
    ExactUUri, NATIVE_PREFIX_METADATA_LAYOUT_ID, NativePrefixFrameMetadataCodec,
    PROTOBUF_PAYLOAD_FAMILY_ID, PayloadEncoding, PayloadLoanProvenance, ProtobufWire,
    StableContainerWireFormat, UCode, UEncodedLoanedRxFrame, UFrameMetadata, UFrameView, UStatus,
    UTxBuffer, UTxLoanSpec, UUninitTxBuffer, UUri, UWire, UWireMetadataCodec, UZeroCopyListener,
    UZeroCopyTransportImpl, UZeroCopyUninitTransportImpl, WireIdentity, XCDR_V2_WIRE_ID,
};
use up_transport_iceoryx2_rust::{Iceoryx2PubSub, Iceoryx2PubSubConfig, UProtocolHeader};
use up_wire_xcdrv2::{VEHICLE_SIGNAL_V1_GOLDEN_BYTES, XCDR_V2_ENCODING_ID, XcdrV2Wire};

static ICEORYX2_TEST_MUTEX: TokioMutex<()> = TokioMutex::const_new(());
static TEST_CONFIG_SEQUENCE: AtomicU64 = AtomicU64::new(1);

type NativeIceoryx2Transport<W> = UNativePrefixWireTransport<Iceoryx2PubSub, W>;
type XcdrV2Iceoryx2Transport = NativeIceoryx2Transport<XcdrV2Wire>;
type XcdrV2Iceoryx2Rx = <XcdrV2Iceoryx2Transport as UZeroCopyTransportImpl>::Rx;

async fn iceoryx2_test_guard() -> MutexGuard<'static, ()> {
    ICEORYX2_TEST_MUTEX.lock().await
}

fn topic(test_name: &str) -> UUri {
    let authority = format!("iox-r19-{test_name}-{}", std::process::id());
    UUri::try_from_parts(&authority, 0x4210, 0x01, 0x9000).expect("topic URI")
}

fn test_config() -> iceoryx2::prelude::Config {
    let root = std::env::var_os("UP_ICEORYX2_TEST_ROOT")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("target")
                .join("r19-iceoryx2-runtime")
        });
    std::fs::create_dir_all(&root).expect("create durable iceoryx2 test root");
    let sequence = TEST_CONFIG_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let prefix = format!("r19_{}_{}_", std::process::id(), sequence);

    let mut config = iceoryx2::prelude::Config::default();
    config.global.set_root_path(
        &Path::new(root.as_os_str().as_encoded_bytes()).expect("iceoryx2 test root path"),
    );
    config.global.prefix = FileName::new(prefix.as_bytes()).expect("iceoryx2 test prefix");
    config
}

fn core(config: &iceoryx2::prelude::Config) -> Iceoryx2PubSub {
    Iceoryx2PubSub::with_config(
        Iceoryx2PubSubConfig::static_allocation(64 * 1024).with_iceoryx2_config(config.clone()),
    )
}

#[test]
fn exact_source_service_name_helper_requires_exact_uuri() {
    let source = ExactUUri::try_from(topic("exact-service-name")).expect("exact source");
    let name =
        Iceoryx2PubSub::exact_source_publish_subscribe_service_name(&source).expect("service name");
    assert!(name.starts_with("up/iox-r19-exact-service-name-"));
    assert!(name.ends_with("/4210/0/1/9000"));
}

#[test]
fn exact_source_service_name_helper_rejects_wildcard_before_mapping() {
    let wildcard = UUri::try_from_parts("vehicle", 0x4210, 0x01, 0xFFFF).unwrap();
    assert!(ExactUUri::try_from(wildcard).is_err());
}

fn metadata(topic: UUri, payload_encoding: PayloadEncoding) -> UFrameMetadata {
    UFrameMetadata::publish(topic)
        .with_payload_encoding(payload_encoding)
        .build()
        .expect("metadata")
}

fn metadata_no_payload(topic: UUri) -> UFrameMetadata {
    UFrameMetadata::publish(topic).build().expect("metadata")
}

fn request_metadata(reply_to: UUri, method: UUri) -> UFrameMetadata {
    UFrameMetadata::request(method, reply_to, Duration::from_millis(5_000))
        .with_payload_encoding(PayloadEncoding::PROTOBUF)
        .build()
        .expect("request metadata")
}

async fn prime_subscriber<W>(transport: &NativeIceoryx2Transport<W>, source: &UUri)
where
    W: UWire + Send + Sync + 'static,
{
    match transport.receive_validated_zero_copy(source, None).await {
        Ok(_) => panic!("subscriber unexpectedly received before send"),
        Err(error) => assert_eq!(error.code(), UCode::NotFound),
    }
}

async fn receive_with_retry<T, F>(mut receive: F) -> Result<T, UStatus>
where
    F: AsyncFnMut() -> Result<T, UStatus>,
{
    let mut last = None;
    for _ in 0..50 {
        match receive().await {
            Ok(value) => return Ok(value),
            Err(error) if error.code() == UCode::NotFound => {
                last = Some(error);
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(error) => return Err(error),
        }
    }
    Err(last.expect("receive attempted at least once"))
}

async fn receive_request_with_retry(
    subscriber: NativeIceoryx2Transport<ProtobufWire>,
    source_filter: UUri,
    sink_filter: UUri,
) -> Result<Vec<u8>, UStatus> {
    receive_with_retry(|| async {
        subscriber
            .receive_validated_zero_copy(&source_filter, Some(&sink_filter))
            .await
            .map(|frame| frame.try_contiguous_payload().unwrap_or_default().to_vec())
    })
    .await
}

#[tokio::test(flavor = "multi_thread")]
async fn prepared_metadata_passes_through_for_required_wires() {
    let _guard = iceoryx2_test_guard().await;
    assert_prepared_metadata::<ProtobufWire>("protobuf-prepared", PayloadEncoding::PROTOBUF).await;
    assert_prepared_metadata::<StableContainerWireFormat>(
        "stable-prepared",
        PayloadEncoding::private_use(0xF101),
    )
    .await;
    assert_prepared_metadata::<XcdrV2Wire>(
        "xcdr-prepared",
        PayloadEncoding::private_use(XCDR_V2_ENCODING_ID),
    )
    .await;
}

async fn assert_prepared_metadata<W>(test_name: &str, payload_encoding: PayloadEncoding)
where
    W: UWire + Default + Send + Sync + 'static,
{
    let config = test_config();
    let transport = core(&config).with_selected_wire(W::default());
    let frame_metadata = metadata(topic(test_name), payload_encoding);
    let mut tx = transport
        .loan_validated_tx(UTxLoanSpec::payload(frame_metadata.clone(), 4, 1).expect("loan spec"))
        .await
        .expect("loan");
    tx.payload_mut().copy_from_slice(b"data");

    assert_eq!(
        tx.header().metadata_len as usize,
        tx.encoded_metadata().len()
    );
    assert_eq!(tx.header().payload_len, 4);
    let decoded = NativePrefixFrameMetadataCodec
        .decode_frame_metadata(W::metadata_context(), tx.encoded_metadata())
        .expect("decode");
    assert_eq!(decoded, frame_metadata);
}

#[tokio::test(flavor = "multi_thread")]
async fn external_xcdrv2_bytes_round_trip_through_real_pull_receive() {
    let _guard = iceoryx2_test_guard().await;
    let config = test_config();
    let publisher = core(&config).with_selected_wire(XcdrV2Wire);
    let subscriber = core(&config).with_selected_wire(XcdrV2Wire);
    let source = topic("xcdr-round-trip");
    prime_subscriber(&subscriber, &source).await;
    let frame_metadata = metadata(
        source.clone(),
        PayloadEncoding::private_use(XCDR_V2_ENCODING_ID),
    );
    let mut tx = publisher
        .loan_validated_tx(
            UTxLoanSpec::payload(frame_metadata, VEHICLE_SIGNAL_V1_GOLDEN_BYTES.len(), 8)
                .expect("loan spec"),
        )
        .await
        .expect("loan");
    tx.payload_mut()
        .copy_from_slice(&VEHICLE_SIGNAL_V1_GOLDEN_BYTES);
    publisher.send_validated_zero_copy(tx).await.expect("send");

    let rx = receive_with_retry(|| async {
        subscriber.receive_validated_zero_copy(&source, None).await
    })
    .await
    .expect("receive");
    assert_eq!(
        rx.try_contiguous_payload(),
        Some(&VEHICLE_SIGNAL_V1_GOLDEN_BYTES[..])
    );
    assert_eq!(
        rx.metadata().payload_encoding().expect("encoding").id(),
        XCDR_V2_ENCODING_ID
    );
    assert_eq!(
        rx.raw()
            .loaned_contiguous_payload()
            .expect("loan-backed payload")
            .provenance(),
        PayloadLoanProvenance::OpaqueTransportLoan
    );
    assert_eq!(
        rx.try_contiguous_payload().unwrap().as_ptr() as usize % 8,
        0
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn request_receive_accepts_wildcard_source_and_exact_method_sink() {
    let _guard = iceoryx2_test_guard().await;
    let config = test_config();
    let publisher = core(&config).with_selected_wire(ProtobufWire);
    let subscriber = core(&config).with_selected_wire(ProtobufWire);
    let authority = format!("iox-r19-request-{}", std::process::id());
    let reply_to = UUri::try_from_parts(&authority, 0x5BA0, 0x01, 0x0000).expect("reply URI");
    let method = UUri::try_from_parts(&authority, 0x5BA0, 0x01, 0x1000).expect("method URI");
    let source_filter = UUri::try_from_parts("*", u32::MAX, u8::MAX, u16::MAX).expect("wildcard");

    let receive = receive_request_with_retry(subscriber, source_filter, method.clone());
    let send = async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let mut tx = publisher
            .loan_validated_tx(
                UTxLoanSpec::payload(request_metadata(reply_to, method), 7, 1).expect("loan spec"),
            )
            .await
            .expect("loan");
        tx.payload_mut().copy_from_slice(b"request");
        publisher.send_validated_zero_copy(tx).await.expect("send");
    };
    let (received, ()) = tokio::join!(receive, send);

    assert_eq!(received.expect("receive"), b"request");
}

#[tokio::test(flavor = "multi_thread")]
async fn wrong_wire_is_rejected_before_public_receive() {
    let _guard = iceoryx2_test_guard().await;
    let config = test_config();
    let source = topic("wrong-wire");
    let publisher = core(&config).with_selected_wire(ProtobufWire);
    let subscriber = core(&config).with_selected_wire(XcdrV2Wire);
    prime_subscriber(&subscriber, &source).await;
    let tx = publisher
        .loan_validated_tx(UTxLoanSpec::no_payload(metadata_no_payload(source.clone())).unwrap())
        .await
        .expect("loan");
    publisher.send_validated_zero_copy(tx).await.expect("send");

    let error = receive_with_retry(|| async {
        subscriber.receive_validated_zero_copy(&source, None).await
    })
    .await
    .expect_err("wrong selected wire rejected");
    assert_eq!(error.code(), UCode::InvalidArgument);
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct XcdrWireWrongPayloadFamily;

impl UWire for XcdrWireWrongPayloadFamily {
    const WIRE_ID: WireIdentity = XCDR_V2_WIRE_ID;
    const PAYLOAD_FAMILY_ID: WireIdentity = PROTOBUF_PAYLOAD_FAMILY_ID;
    const METADATA_LAYOUT_ID: WireIdentity = NATIVE_PREFIX_METADATA_LAYOUT_ID;
    const FORMAT_VERSION: u16 = XcdrV2Wire::FORMAT_VERSION;
}

#[tokio::test(flavor = "multi_thread")]
async fn payload_family_mismatch_is_rejected() {
    let _guard = iceoryx2_test_guard().await;
    let config = test_config();
    let source = topic("payload-family-mismatch");
    let publisher = core(&config).with_selected_wire(XcdrV2Wire);
    let subscriber = core(&config).with_selected_wire(XcdrWireWrongPayloadFamily);
    prime_subscriber(&subscriber, &source).await;
    let tx = publisher
        .loan_validated_tx(UTxLoanSpec::no_payload(metadata_no_payload(source.clone())).unwrap())
        .await
        .expect("loan");
    publisher.send_validated_zero_copy(tx).await.expect("send");

    let error = receive_with_retry(|| async {
        subscriber.receive_validated_zero_copy(&source, None).await
    })
    .await
    .expect_err("payload family mismatch rejected");
    assert_eq!(error.code(), UCode::InvalidArgument);
}

#[tokio::test(flavor = "multi_thread")]
async fn uninit_tx_loan_commits_initialized_payload() {
    let _guard = iceoryx2_test_guard().await;
    let config = test_config();
    let publisher = core(&config).with_selected_wire(XcdrV2Wire);
    let subscriber = core(&config).with_selected_wire(XcdrV2Wire);
    let source = topic("uninit-tx");
    prime_subscriber(&subscriber, &source).await;
    let frame_metadata = metadata(
        source.clone(),
        PayloadEncoding::private_use(XCDR_V2_ENCODING_ID),
    );
    let mut tx = publisher
        .loan_validated_uninit_tx(UTxLoanSpec::payload(frame_metadata, 8, 8).unwrap())
        .await
        .expect("loan");
    for (slot, value) in tx.payload_uninit_mut().iter_mut().zip(0u8..8) {
        slot.write(value);
    }
    // SAFETY: all visible payload bytes are initialized immediately above.
    let tx = unsafe { tx.assume_payload_initialized() };
    publisher.send_validated_zero_copy(tx).await.expect("send");

    let rx = receive_with_retry(|| async {
        subscriber.receive_validated_zero_copy(&source, None).await
    })
    .await
    .expect("receive");
    assert_eq!(
        rx.try_contiguous_payload(),
        Some([0, 1, 2, 3, 4, 5, 6, 7].as_slice())
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn no_payload_round_trip_preserves_absence() {
    let _guard = iceoryx2_test_guard().await;
    let config = test_config();
    let publisher = core(&config).with_selected_wire(XcdrV2Wire);
    let subscriber = core(&config).with_selected_wire(XcdrV2Wire);
    let source = topic("no-payload");
    prime_subscriber(&subscriber, &source).await;
    let tx = publisher
        .loan_validated_tx(UTxLoanSpec::no_payload(metadata_no_payload(source.clone())).unwrap())
        .await
        .expect("loan");
    publisher.send_validated_zero_copy(tx).await.expect("send");

    let rx = receive_with_retry(|| async {
        subscriber.receive_validated_zero_copy(&source, None).await
    })
    .await
    .expect("receive");
    assert!(!rx.has_payload());
    assert_eq!(rx.payload_len(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn present_empty_payload_round_trip_preserves_presence() {
    let _guard = iceoryx2_test_guard().await;
    let config = test_config();
    let publisher = core(&config).with_selected_wire(ProtobufWire);
    let subscriber = core(&config).with_selected_wire(ProtobufWire);
    let source = topic("present-empty");
    prime_subscriber(&subscriber, &source).await;
    let tx = publisher
        .loan_validated_tx(
            UTxLoanSpec::payload(metadata(source.clone(), PayloadEncoding::PROTOBUF), 0, 1)
                .expect("present-empty loan spec"),
        )
        .await
        .expect("loan");
    publisher.send_validated_zero_copy(tx).await.expect("send");

    let rx = receive_with_retry(|| async {
        subscriber.receive_validated_zero_copy(&source, None).await
    })
    .await
    .expect("receive");
    assert!(rx.has_payload());
    assert_eq!(rx.payload_len(), 0);
    assert_eq!(rx.try_contiguous_payload(), Some(&[][..]));
}

#[tokio::test(flavor = "multi_thread")]
async fn safe_overflow_keeps_the_newest_bounded_samples() {
    let _guard = iceoryx2_test_guard().await;
    let config = test_config();
    let publisher = core(&config).with_selected_wire(ProtobufWire);
    let subscriber = core(&config).with_selected_wire(ProtobufWire);
    let source = topic("safe-overflow");
    prime_subscriber(&subscriber, &source).await;

    for value in 0_u8..4 {
        let mut tx = publisher
            .loan_validated_tx(
                UTxLoanSpec::payload(metadata(source.clone(), PayloadEncoding::PROTOBUF), 1, 1)
                    .expect("loan spec"),
            )
            .await
            .expect("loan");
        tx.payload_mut()[0] = value;
        publisher.send_validated_zero_copy(tx).await.expect("send");
    }

    let first = receive_with_retry(|| async {
        subscriber.receive_validated_zero_copy(&source, None).await
    })
    .await
    .expect("first retained sample");
    let second = receive_with_retry(|| async {
        subscriber.receive_validated_zero_copy(&source, None).await
    })
    .await
    .expect("second retained sample");
    assert_eq!(first.try_contiguous_payload(), Some(&[2][..]));
    assert_eq!(second.try_contiguous_payload(), Some(&[3][..]));
}

#[tokio::test(flavor = "multi_thread")]
async fn untrusted_source_attributes_are_rejected_before_receive() {
    let _guard = iceoryx2_test_guard().await;
    let config = test_config();
    let source = topic("untrusted-source");
    let service_name = ServiceName::new(
        &Iceoryx2PubSub::publish_subscribe_service_name(&source, None).expect("service name"),
    )
    .expect("service name");
    let raw_node = NodeBuilder::new()
        .config(&config)
        .create::<ipc_threadsafe::Service>()
        .expect("raw node");
    let _spoofed_service = raw_node
        .service_builder(&service_name)
        .publish_subscribe::<[u8]>()
        .user_header::<UProtocolHeader>()
        .open_or_create()
        .expect("spoofed service");
    let subscriber = core(&config).with_selected_wire(ProtobufWire);

    let error = subscriber
        .receive_validated_zero_copy(&source, None)
        .await
        .expect_err("source without immutable provenance attributes rejected");
    assert_eq!(error.code(), UCode::Internal);
}

#[tokio::test(flavor = "multi_thread")]
async fn malformed_metadata_is_rejected_before_public_receive() {
    let _guard = iceoryx2_test_guard().await;
    let config = test_config();
    let source = topic("malformed-metadata");
    let publisher_core = core(&config);
    let publisher = publisher_core.clone().with_selected_wire(ProtobufWire);
    let subscriber = core(&config).with_selected_wire(ProtobufWire);
    prime_subscriber(&subscriber, &source).await;

    let prepared = publisher
        .loan_validated_tx(UTxLoanSpec::no_payload(metadata_no_payload(source.clone())).unwrap())
        .await
        .expect("create attributed service");
    drop(prepared);
    let service_name = ServiceName::new(
        &Iceoryx2PubSub::publish_subscribe_service_name(&source, None).expect("service name"),
    )
    .expect("service name");
    let raw_node = NodeBuilder::new()
        .config(&config)
        .create::<ipc_threadsafe::Service>()
        .expect("raw node");
    let raw_service = raw_node
        .service_builder(&service_name)
        .publish_subscribe::<[u8]>()
        .user_header::<UProtocolHeader>()
        .open()
        .expect("open attributed service");
    let raw_publisher = raw_service
        .publisher_builder()
        .initial_max_slice_len(3)
        .allocation_strategy(AllocationStrategy::Static)
        .create()
        .expect("raw publisher");
    let mut sample = raw_publisher.loan_slice(3).expect("raw sample");
    sample.payload_mut().copy_from_slice(&[0xff, 0x00, 0x01]);
    *sample.user_header_mut() = UProtocolHeader {
        uprotocol_major_version: 0,
        metadata_len: 3,
        payload_offset: 3,
        payload_len: 0,
        payload_alignment: 1,
    };
    sample.send().expect("send malformed metadata");

    let error = receive_with_retry(|| async {
        subscriber.receive_validated_zero_copy(&source, None).await
    })
    .await
    .expect_err("malformed metadata rejected");
    assert_eq!(error.code(), UCode::InvalidArgument);
}

#[tokio::test(flavor = "multi_thread")]
async fn listener_receives_and_unregister_stops_delivery() {
    let _guard = iceoryx2_test_guard().await;
    let config = test_config();
    let source = topic("listener-unregister");
    let publisher = core(&config).with_selected_wire(XcdrV2Wire);
    let listener_transport = core(&config).with_selected_wire(XcdrV2Wire);
    let listener = Arc::new(CountingListener::default());
    listener_transport
        .register_validated_zero_copy_listener(&source, None, listener.clone())
        .await
        .expect("register");

    send_payload(&publisher, source.clone(), &[9, 8, 7]).await;
    for _ in 0..50 {
        if listener.payloads() == vec![vec![9, 8, 7]] {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(listener.payloads(), vec![vec![9, 8, 7]]);

    listener_transport
        .unregister_validated_zero_copy_listener(&source, None, listener.clone())
        .await
        .expect("unregister");
    send_payload(&publisher, source, &[1, 2, 3]).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(listener.payloads(), vec![vec![9, 8, 7]]);
}

async fn send_payload(publisher: &XcdrV2Iceoryx2Transport, source: UUri, payload: &[u8]) {
    let frame_metadata = metadata(source, PayloadEncoding::private_use(XCDR_V2_ENCODING_ID));
    let mut tx = publisher
        .loan_validated_tx(UTxLoanSpec::payload(frame_metadata, payload.len(), 1).unwrap())
        .await
        .expect("loan");
    tx.payload_mut().copy_from_slice(payload);
    publisher.send_validated_zero_copy(tx).await.expect("send");
}

#[derive(Default)]
struct CountingListener {
    payloads: StdMutex<Vec<Vec<u8>>>,
}

impl CountingListener {
    fn payloads(&self) -> Vec<Vec<u8>> {
        self.payloads.lock().expect("payload lock").clone()
    }
}

#[async_trait]
impl UZeroCopyListener<XcdrV2Iceoryx2Rx> for CountingListener {
    async fn on_receive_zero_copy(&self, frame: XcdrV2Iceoryx2Rx) {
        self.payloads
            .lock()
            .expect("payload lock")
            .push(frame.try_contiguous_payload().unwrap_or_default().to_vec());
    }
}

struct RetainingListener(tokio::sync::mpsc::UnboundedSender<XcdrV2Iceoryx2Rx>);

#[async_trait]
impl UZeroCopyListener<XcdrV2Iceoryx2Rx> for RetainingListener {
    async fn on_receive_zero_copy(&self, frame: XcdrV2Iceoryx2Rx) {
        let _ = self.0.send(frame);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn receive_lease_retains_native_storage_after_unregister_and_transport_drop() {
    let _guard = iceoryx2_test_guard().await;
    let config = test_config();
    let source = topic("retained-lease");
    let publisher = core(&config).with_selected_wire(XcdrV2Wire);
    let subscriber = core(&config).with_selected_wire(XcdrV2Wire);
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let listener: Arc<dyn UZeroCopyListener<XcdrV2Iceoryx2Rx>> =
        Arc::new(RetainingListener(sender));
    subscriber
        .register_validated_zero_copy_listener(&source, None, listener.clone())
        .await
        .unwrap();
    send_payload(&publisher, source.clone(), b"retained-native-storage").await;
    let frame = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
        .await
        .unwrap()
        .unwrap();
    let address = frame.try_contiguous_payload().unwrap().as_ptr();
    subscriber
        .unregister_validated_zero_copy_listener(&source, None, listener)
        .await
        .unwrap();
    drop(subscriber);
    drop(publisher);
    assert_eq!(
        frame.try_contiguous_payload().unwrap(),
        b"retained-native-storage"
    );
    assert_eq!(frame.try_contiguous_payload().unwrap().as_ptr(), address);
    assert_eq!(frame.metadata().source(), &source);
    assert_eq!(
        frame
            .raw()
            .loaned_contiguous_payload()
            .unwrap()
            .provenance(),
        PayloadLoanProvenance::OpaqueTransportLoan
    );
}

#[derive(Default)]
struct PausedFirstListener {
    calls: AtomicU64,
    entered: tokio::sync::Notify,
    resume: tokio::sync::Notify,
    next: tokio::sync::Notify,
}

#[async_trait]
impl UZeroCopyListener<XcdrV2Iceoryx2Rx> for PausedFirstListener {
    async fn on_receive_zero_copy(&self, _frame: XcdrV2Iceoryx2Rx) {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            self.entered.notify_one();
            self.resume.notified().await;
        } else {
            self.next.notify_one();
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn unregister_invalidates_already_collected_callback_deliveries() {
    let _guard = iceoryx2_test_guard().await;
    let config = test_config();
    let source = topic("unregister-snapshot");
    let publisher = core(&config).with_selected_wire(XcdrV2Wire);
    let subscriber = core(&config).with_selected_wire(XcdrV2Wire);
    let first = Arc::new(PausedFirstListener::default());
    let first_registration: Arc<dyn UZeroCopyListener<XcdrV2Iceoryx2Rx>> = first.clone();
    let removed = Arc::new(CountingListener::default());
    subscriber
        .register_validated_zero_copy_listener(&source, None, first_registration.clone())
        .await
        .unwrap();
    subscriber
        .register_validated_zero_copy_listener(&source, None, removed.clone())
        .await
        .unwrap();
    send_payload(&publisher, source.clone(), b"first").await;
    tokio::time::timeout(Duration::from_secs(2), first.entered.notified())
        .await
        .unwrap();
    subscriber
        .unregister_validated_zero_copy_listener(&source, None, removed.clone())
        .await
        .unwrap();
    first.resume.notify_one();
    send_payload(&publisher, source.clone(), b"next").await;
    tokio::time::timeout(Duration::from_secs(2), first.next.notified())
        .await
        .unwrap();
    assert!(
        removed.payloads().is_empty(),
        "a removed registration cannot enter from a worker snapshot"
    );
    subscriber
        .unregister_validated_zero_copy_listener(&source, None, first_registration)
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn explicit_publisher_readiness_times_out_without_a_subscriber() {
    let _guard = iceoryx2_test_guard().await;
    let config = test_config();
    let publisher = Iceoryx2PubSub::with_config(
        Iceoryx2PubSubConfig::static_allocation(64 * 1024)
            .with_iceoryx2_config(config)
            .with_publisher_readiness(1, Duration::from_millis(30)),
    )
    .with_selected_wire(XcdrV2Wire);
    let source = topic("missing-subscriber");
    let spec = UTxLoanSpec::payload(
        metadata(source, PayloadEncoding::private_use(XCDR_V2_ENCODING_ID)),
        3,
        1,
    )
    .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(2), publisher.loan_validated_tx(spec))
        .await
        .expect("readiness wait must be bounded");
    let error = match result {
        Ok(_) => panic!("a sendable loan requires the configured subscriber"),
        Err(error) => error,
    };
    assert_eq!(error.code(), UCode::DeadlineExceeded);
}

#[tokio::test(flavor = "multi_thread")]
async fn explicit_readiness_waits_for_actual_subscription_before_first_send() {
    let _guard = iceoryx2_test_guard().await;
    let config = test_config();
    let source = topic("late-subscriber");
    let publisher = Iceoryx2PubSub::with_config(
        Iceoryx2PubSubConfig::static_allocation(64 * 1024)
            .with_iceoryx2_config(config.clone())
            .with_publisher_readiness(1, Duration::from_secs(2)),
    )
    .with_selected_wire(XcdrV2Wire);
    let subscriber = core(&config).with_selected_wire(XcdrV2Wire);
    let service_name = Iceoryx2PubSub::publish_subscribe_service_name(&source, None).unwrap();
    let send_source = source.clone();
    let send = tokio::spawn(async move {
        send_payload(&publisher, send_source, b"single-first-send").await;
        publisher
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while !subscriber
            .core()
            .discover_service_names()
            .unwrap()
            .contains(&service_name)
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        !send.is_finished(),
        "the publisher must not finish before a real subscriber exists"
    );
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let listener: Arc<dyn UZeroCopyListener<XcdrV2Iceoryx2Rx>> =
        Arc::new(RetainingListener(sender));
    subscriber
        .register_validated_zero_copy_listener(&source, None, listener.clone())
        .await
        .unwrap();
    let publisher = send.await.unwrap();
    let frame = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        frame.try_contiguous_payload().unwrap(),
        b"single-first-send"
    );
    subscriber
        .unregister_validated_zero_copy_listener(&source, None, listener)
        .await
        .unwrap();
    drop(publisher);
}

struct ReadinessPeer(std::process::Child);

#[tokio::test(flavor = "multi_thread")]
async fn per_instance_namespaces_isolate_identical_logical_topics() {
    let _guard = iceoryx2_test_guard().await;
    let config = test_config();
    let suffix = std::str::from_utf8(config.global.prefix.as_bytes()).unwrap();
    let root =
        std::path::PathBuf::from(std::env::var("UP_ICEORYX2_TEST_ROOT").unwrap_or_else(|_| {
            format!("{}/target/r19-iceoryx2-runtime", env!("CARGO_MANIFEST_DIR"))
        }));
    let local = Iceoryx2PubSub::with_config(
        Iceoryx2PubSubConfig::static_allocation(65536)
            .with_namespace(root.to_str().unwrap(), &format!("a_{suffix}"))
            .unwrap(),
    )
    .with_selected_wire(XcdrV2Wire);
    let remote = Iceoryx2PubSub::with_config(
        Iceoryx2PubSubConfig::static_allocation(65536)
            .with_namespace(root.to_str().unwrap(), &format!("b_{suffix}"))
            .unwrap(),
    )
    .with_selected_wire(XcdrV2Wire);
    let source = topic("isolated-topic");
    prime_subscriber(&local, &source).await;
    prime_subscriber(&remote, &source).await;
    send_payload(&local, source.clone(), b"requires a bridge").await;
    let own = receive_with_retry(|| local.receive_validated_zero_copy(&source, None))
        .await
        .unwrap();
    assert_eq!(own.try_contiguous_payload().unwrap(), b"requires a bridge");
    assert_eq!(
        remote
            .receive_validated_zero_copy(&source, None)
            .await
            .unwrap_err()
            .code(),
        UCode::NotFound
    );
    send_payload(
        &remote,
        source.clone(),
        own.try_contiguous_payload().unwrap(),
    )
    .await;
    let forwarded = receive_with_retry(|| remote.receive_validated_zero_copy(&source, None))
        .await
        .unwrap();
    assert_eq!(
        forwarded.try_contiguous_payload().unwrap(),
        b"requires a bridge"
    );
}

#[test]
fn native_namespace_rejects_invalid_inputs() {
    assert!(
        Iceoryx2PubSubConfig::default()
            .with_namespace("relative", "prefix_")
            .is_err()
    );
    assert!(
        Iceoryx2PubSubConfig::default()
            .with_namespace("/absolute", "bad/prefix")
            .is_err()
    );
    assert!(
        Iceoryx2PubSubConfig::default()
            .with_namespace("/absolute", "")
            .is_err()
    );
}

impl Drop for ReadinessPeer {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn publisher_readiness_waits_for_cross_process_wildcard_subscriber() {
    cross_process_first_send(
        "publisher_readiness_waits_for_cross_process_wildcard_subscriber",
        b"one cross-process send",
        false,
        false,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn dynamic_publisher_delivers_a_large_first_sample_across_processes() {
    cross_process_first_send(
        "dynamic_publisher_delivers_a_large_first_sample_across_processes",
        &vec![0x5a; 2312],
        true,
        false,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn dynamic_publisher_delivers_a_large_first_request_across_processes() {
    cross_process_first_send(
        "dynamic_publisher_delivers_a_large_first_request_across_processes",
        &vec![0x5a; 2312],
        true,
        true,
    )
    .await;
}

async fn cross_process_first_send(test_name: &str, payload: &[u8], dynamic: bool, rpc: bool) {
    let mut config = test_config();
    let sink = rpc.then(|| UUri::try_from_parts("readiness-service", 0x4210, 1, 0x1000).unwrap());
    if let Ok(prefix) = std::env::var("UP_ICEORYX2_READINESS_PEER_PREFIX") {
        config.global.prefix = FileName::new(prefix.as_bytes()).unwrap();
        let subscriber = core(&config).with_selected_wire(XcdrV2Wire);
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        let listener: Arc<dyn UZeroCopyListener<XcdrV2Iceoryx2Rx>> =
            Arc::new(RetainingListener(sender));
        subscriber
            .register_validated_zero_copy_listener(&UUri::any(), sink.as_ref(), listener.clone())
            .await
            .unwrap();
        let frame = tokio::time::timeout(Duration::from_secs(5), receiver.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(frame.try_contiguous_payload().unwrap(), payload);
        subscriber
            .unregister_validated_zero_copy_listener(&UUri::any(), sink.as_ref(), listener)
            .await
            .unwrap();
        return;
    }
    let _guard = iceoryx2_test_guard().await;
    let source = topic("cross-process-ready");
    let allocation = if dynamic {
        Iceoryx2PubSubConfig::default()
    } else {
        Iceoryx2PubSubConfig::static_allocation(64 * 1024)
    };
    let publisher = Iceoryx2PubSub::with_config(
        allocation
            .with_iceoryx2_config(config.clone())
            .with_publisher_readiness(1, Duration::from_secs(5)),
    )
    .with_selected_wire(XcdrV2Wire);
    if rpc {
        // Model the bridge's reverse route. It is interested in replies from
        // the service, not requests to it, and must not satisfy TX readiness.
        let reverse_source =
            UUri::try_from_parts("readiness-service", u32::MAX, u8::MAX, u16::MAX).unwrap();
        let reverse_sink =
            UUri::try_from_parts(source.authority_name(), u32::MAX, u8::MAX, u16::MAX).unwrap();
        publisher
            .register_validated_zero_copy_listener(
                &reverse_source,
                Some(&reverse_sink),
                Arc::new(CountingListener::default()),
            )
            .await
            .unwrap();
    }
    let mut peer = ReadinessPeer(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", test_name, "--nocapture"])
            .env(
                "UP_ICEORYX2_READINESS_PEER_PREFIX",
                std::str::from_utf8(config.global.prefix.as_bytes()).unwrap(),
            )
            .spawn()
            .unwrap(),
    );
    // There is no same-process receiver to notify and history remains zero.
    // The single send can succeed only after native discovery of the child.
    if let Some(method) = sink {
        let metadata = UFrameMetadata::request(
            method,
            source.clone_with_resource_id(0),
            Duration::from_secs(5),
        )
        .with_payload_encoding(PayloadEncoding::private_use(XCDR_V2_ENCODING_ID))
        .build()
        .unwrap();
        let mut loan = publisher
            .loan_validated_tx(UTxLoanSpec::payload(metadata, payload.len(), 8).unwrap())
            .await
            .unwrap();
        loan.payload_mut().copy_from_slice(payload);
        publisher.send_validated_zero_copy(loan).await.unwrap();
    } else {
        send_payload(&publisher, source, payload).await;
    }
    let exit = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(exit) = peer.0.try_wait().unwrap() {
                break exit;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("peer must receive the first send and exit");
    assert!(exit.success());
}
