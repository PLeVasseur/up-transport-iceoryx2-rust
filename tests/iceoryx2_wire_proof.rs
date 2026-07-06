// ################################################################################
// Copyright (c) 2026 Contributors to the Eclipse Foundation
//
// SPDX-License-Identifier: Apache-2.0
// ################################################################################

use std::{sync::Arc, sync::Mutex as StdMutex, time::Duration};

use async_trait::async_trait;
use tokio::sync::{Mutex as TokioMutex, MutexGuard};
use up_rust::selected_wire_user_api::UNativePrefixWireTransport;
use up_rust::wire_implementer_api::{
    NATIVE_PREFIX_METADATA_LAYOUT_ID, NativePrefixFrameMetadataCodec, PROTOBUF_PAYLOAD_FAMILY_ID,
    ProtobufWire, StableContainerWireFormat, UWire, UWireMetadataCodec, WireIdentity,
    XCDR_V2_WIRE_ID,
};
use up_rust::{
    ExactUUri, PayloadEncoding, PayloadFormat, UCode, UFrameMetadata, UFrameView, UStatus,
    UTxBuffer, UTxLoanSpec, UUninitTxBuffer, UUri, UZeroCopyListener, UZeroCopyTransport,
    UZeroCopyUninitTransport,
};
use up_transport_iceoryx2_rust::{Iceoryx2PubSub, Iceoryx2PubSubConfig};
use up_wire_xcdrv2::{VEHICLE_SIGNAL_V1_GOLDEN_BYTES, XCDR_V2_ENCODING_ID, XcdrV2Wire};

static ICEORYX2_TEST_MUTEX: TokioMutex<()> = TokioMutex::const_new(());

type NativeIceoryx2Transport<W> = UNativePrefixWireTransport<Iceoryx2PubSub, W>;
type XcdrV2Iceoryx2Transport = NativeIceoryx2Transport<XcdrV2Wire>;
type XcdrV2Iceoryx2Rx = <XcdrV2Iceoryx2Transport as UZeroCopyTransport>::Rx;

async fn iceoryx2_test_guard() -> MutexGuard<'static, ()> {
    ICEORYX2_TEST_MUTEX.lock().await
}

fn topic(test_name: &str) -> UUri {
    let authority = format!("iox-usr09i-{test_name}-{}", std::process::id());
    UUri::try_from_parts(&authority, 0x4210, 0x01, 0x9000).expect("topic URI")
}

fn test_config() -> iceoryx2::prelude::Config {
    iceoryx2::testing::generate_isolated_config()
}

fn core(config: &iceoryx2::prelude::Config) -> Iceoryx2PubSub {
    Iceoryx2PubSub::with_config(
        Iceoryx2PubSubConfig::default().with_iceoryx2_config(config.clone()),
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn default_constructor_creates_node_with_fallback_config() {
    let _guard = iceoryx2_test_guard().await;

    let _core = Iceoryx2PubSub::new();
}

#[test]
fn exact_source_service_name_helper_requires_exact_uuri() {
    let source = ExactUUri::try_from(topic("exact-service-name")).expect("exact source");

    let name =
        Iceoryx2PubSub::exact_source_publish_subscribe_service_name(&source).expect("service name");

    assert!(name.starts_with("up/iox-usr09i-exact-service-name-"));
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
    match transport.receive_zero_copy(source, None).await {
        Ok(_) => panic!("subscriber unexpectedly received before send"),
        Err(error) => assert_eq!(error.get_code(), UCode::NotFound),
    }
}

async fn receive_with_retry<T, F>(mut f: F) -> Result<T, UStatus>
where
    F: AsyncFnMut() -> Result<T, UStatus>,
{
    let mut last = None;
    for _ in 0..50 {
        match f().await {
            Ok(value) => return Ok(value),
            Err(error) => {
                if error.get_code() != UCode::NotFound {
                    return Err(error);
                }
                last = Some(error);
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }
    Err(last.expect("receive attempted at least once"))
}

async fn receive_request_with_retry(
    subscriber: NativeIceoryx2Transport<ProtobufWire>,
    source_filter: UUri,
    sink_filter: UUri,
) -> Result<Vec<u8>, UStatus> {
    let mut last = None;
    for _ in 0..100 {
        match subscriber
            .receive_zero_copy(&source_filter, Some(&sink_filter))
            .await
        {
            Ok(frame) => return Ok(frame.try_contiguous_payload().unwrap_or_default().to_vec()),
            Err(error) => {
                if error.get_code() != UCode::NotFound {
                    return Err(error);
                }
                last = Some(error);
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }
    Err(last.expect("receive attempted at least once"))
}

#[tokio::test(flavor = "multi_thread")]
async fn prepared_metadata_passes_through_for_required_wires() {
    let _guard = iceoryx2_test_guard().await;
    assert_prepared_metadata::<ProtobufWire>("protobuf-prepared", PayloadEncoding::PROTOBUF).await;
    assert_prepared_metadata::<StableContainerWireFormat>(
        "stable-prepared",
        PayloadEncoding::custom(
            "up.stable-container-test",
            "application/vnd.uprotocol.stable-container-test",
        )
        .expect("stable encoding"),
    )
    .await;
    assert_prepared_metadata::<XcdrV2Wire>("xcdr-prepared", XcdrV2Wire::encoding()).await;
}

async fn assert_prepared_metadata<W>(test_name: &str, payload_encoding: PayloadEncoding)
where
    W: UWire + Default + Send + Sync + 'static,
{
    let config = test_config();
    let transport = core(&config).with_selected_wire(W::default());
    let frame_metadata = metadata(topic(test_name), payload_encoding);
    let mut tx = transport
        .loan_tx(UTxLoanSpec::payload(frame_metadata.clone(), 4, 1).expect("loan spec"))
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
    let frame_metadata = metadata(source.clone(), XcdrV2Wire::encoding());
    let mut tx = publisher
        .loan_tx(
            UTxLoanSpec::payload(frame_metadata, VEHICLE_SIGNAL_V1_GOLDEN_BYTES.len(), 1)
                .expect("loan spec"),
        )
        .await
        .expect("loan");
    tx.payload_mut()
        .copy_from_slice(&VEHICLE_SIGNAL_V1_GOLDEN_BYTES);
    publisher.send_zero_copy(tx).await.expect("send");

    let rx = receive_with_retry(|| async { subscriber.receive_zero_copy(&source, None).await })
        .await
        .expect("receive");
    assert_eq!(
        rx.try_contiguous_payload(),
        Some(&VEHICLE_SIGNAL_V1_GOLDEN_BYTES[..])
    );
    let (encoding_id, _) = rx
        .metadata()
        .payload_encoding()
        .expect("encoding")
        .custom_identity()
        .expect("custom identity");
    assert_eq!(encoding_id, XCDR_V2_ENCODING_ID);
}

#[tokio::test(flavor = "multi_thread")]
async fn request_receive_accepts_wildcard_source_and_exact_method_sink() {
    let _guard = iceoryx2_test_guard().await;
    let config = test_config();
    let publisher = core(&config).with_selected_wire(ProtobufWire::default());
    let subscriber = core(&config).with_selected_wire(ProtobufWire::default());
    let authority = format!("iox-usr09i-request-{}", std::process::id());
    let reply_to = UUri::try_from_parts(&authority, 0x5BA0, 0x01, 0x0000).expect("reply-to URI");
    let method = UUri::try_from_parts(&authority, 0x5BA0, 0x01, 0x1000).expect("method URI");
    let source_filter =
        UUri::try_from_parts("*", u32::MAX, u8::MAX, u16::MAX).expect("wildcard source");
    let sink_filter = method.clone();

    let receive_task = tokio::spawn(receive_request_with_retry(
        subscriber,
        source_filter,
        sink_filter,
    ));
    tokio::time::sleep(Duration::from_millis(50)).await;

    let frame_metadata = request_metadata(reply_to, method);
    let mut tx = publisher
        .loan_tx(UTxLoanSpec::payload(frame_metadata, 7, 1).expect("loan spec"))
        .await
        .expect("loan");
    tx.payload_mut().copy_from_slice(b"request");
    publisher.send_zero_copy(tx).await.expect("send");

    let payload = receive_task
        .await
        .expect("receive task")
        .expect("request receive");
    assert_eq!(payload, b"request");
}

#[tokio::test(flavor = "multi_thread")]
async fn wrong_wire_is_rejected_before_public_receive() {
    let _guard = iceoryx2_test_guard().await;
    let config = test_config();
    let source = topic("wrong-wire");
    let publisher = core(&config).with_selected_wire(ProtobufWire);
    let subscriber = core(&config).with_selected_wire(XcdrV2Wire);
    prime_subscriber(&subscriber, &source).await;
    let frame_metadata = metadata_no_payload(source.clone());
    let tx = publisher
        .loan_tx(UTxLoanSpec::no_payload(frame_metadata).expect("loan spec"))
        .await
        .expect("loan");
    publisher.send_zero_copy(tx).await.expect("send");

    let error =
        match receive_with_retry(|| async { subscriber.receive_zero_copy(&source, None).await })
            .await
        {
            Ok(_) => panic!("wrong selected wire unexpectedly received"),
            Err(error) => error,
        };
    assert_eq!(error.get_code(), UCode::InvalidArgument);
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
async fn payload_family_mismatch_is_distinct_from_wrong_wire() {
    let _guard = iceoryx2_test_guard().await;
    let config = test_config();
    let source = topic("payload-family-mismatch");
    let publisher = core(&config).with_selected_wire(XcdrV2Wire);
    let subscriber = core(&config).with_selected_wire(XcdrWireWrongPayloadFamily);
    prime_subscriber(&subscriber, &source).await;
    let frame_metadata = metadata_no_payload(source.clone());
    let tx = publisher
        .loan_tx(UTxLoanSpec::no_payload(frame_metadata).expect("loan spec"))
        .await
        .expect("loan");
    publisher.send_zero_copy(tx).await.expect("send");

    let error =
        match receive_with_retry(|| async { subscriber.receive_zero_copy(&source, None).await })
            .await
        {
            Ok(_) => panic!("payload-family mismatch unexpectedly received"),
            Err(error) => error,
        };
    assert_eq!(error.get_code(), UCode::InvalidArgument);
}

#[tokio::test(flavor = "multi_thread")]
async fn uninit_tx_loan_commits_initialized_payload() {
    let _guard = iceoryx2_test_guard().await;
    let config = test_config();
    let publisher = core(&config).with_selected_wire(XcdrV2Wire);
    let subscriber = core(&config).with_selected_wire(XcdrV2Wire);
    let source = topic("uninit-tx");
    prime_subscriber(&subscriber, &source).await;
    let frame_metadata = metadata(source.clone(), XcdrV2Wire::encoding());
    let mut tx = publisher
        .loan_uninit_tx(UTxLoanSpec::payload(frame_metadata, 8, 8).expect("loan spec"))
        .await
        .expect("loan");
    for (slot, value) in tx.payload_uninit_mut().iter_mut().zip(0u8..8) {
        slot.write(value);
    }
    // SAFETY: all visible payload bytes are initialized immediately above.
    let tx = unsafe { tx.assume_payload_init() };
    publisher.send_zero_copy(tx).await.expect("send");

    let rx = receive_with_retry(|| async { subscriber.receive_zero_copy(&source, None).await })
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
    let frame_metadata = metadata_no_payload(source.clone());
    let tx = publisher
        .loan_tx(UTxLoanSpec::no_payload(frame_metadata).expect("loan spec"))
        .await
        .expect("loan");
    publisher.send_zero_copy(tx).await.expect("send");

    let rx = receive_with_retry(|| async { subscriber.receive_zero_copy(&source, None).await })
        .await
        .expect("receive");
    assert!(!rx.has_payload());
    assert_eq!(rx.payload_len(), 0);
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
        .register_zero_copy_listener(&source, None, listener.clone())
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
        .unregister_zero_copy_listener(&source, None, listener.clone())
        .await
        .expect("unregister");
    send_payload(&publisher, source, &[1, 2, 3]).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(listener.payloads(), vec![vec![9, 8, 7]]);
}

async fn send_payload(publisher: &XcdrV2Iceoryx2Transport, source: UUri, payload: &[u8]) {
    let frame_metadata = metadata(source, XcdrV2Wire::encoding());
    let mut tx = publisher
        .loan_tx(UTxLoanSpec::payload(frame_metadata, payload.len(), 1).expect("loan spec"))
        .await
        .expect("loan");
    tx.payload_mut().copy_from_slice(payload);
    publisher.send_zero_copy(tx).await.expect("send");
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
