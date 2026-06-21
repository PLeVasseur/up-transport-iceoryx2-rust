// ################################################################################
// Copyright (c) 2026 Contributors to the Eclipse Foundation
//
// SPDX-License-Identifier: Apache-2.0
// ################################################################################

use std::{sync::Arc, sync::Mutex as StdMutex, time::Duration};

use async_trait::async_trait;
use tokio::sync::{Mutex as TokioMutex, MutexGuard};
use up_rust::{
    NATIVE_PREFIX_METADATA_LAYOUT_ID, NativePrefixProtobufMetadataCodec,
    PROTOBUF_PAYLOAD_FAMILY_ID, PayloadEncoding, PayloadFormat, ProtobufWire,
    StableContainerWireFormat, UCode, UFrameMetadata, UFrameView, UMessageBuilder, UPayloadFormat,
    UStatus, UTxBuffer, UTxLoanSpec, UUninitTxBuffer, UUri, UWire, UWireMetadataCodec, UWireRx,
    UWireTransport, UZeroCopyListener, UZeroCopyTransport, UZeroCopyUninitTransport, WireIdentity,
    XCDR_V2_WIRE_ID,
};
use up_transport_iceoryx2_rust::{Iceoryx2PubSub, Iceoryx2RxLease};
use up_wire_xcdrv2::{VEHICLE_SIGNAL_V1_GOLDEN_BYTES, XCDR_V2_ENCODING_ID, XcdrV2Wire};

static ICEORYX2_TEST_MUTEX: TokioMutex<()> = TokioMutex::const_new(());

async fn iceoryx2_test_guard() -> MutexGuard<'static, ()> {
    ICEORYX2_TEST_MUTEX.lock().await
}

fn topic(test_name: &str) -> UUri {
    let authority = format!("iox-usr09i-{test_name}-{}", std::process::id());
    UUri::try_from_parts(&authority, 0x4210, 0x01, 0x9000).expect("topic URI")
}

fn metadata(topic: UUri, payload_encoding: PayloadEncoding) -> UFrameMetadata {
    let message = UMessageBuilder::publish(topic).build().expect("message");
    UFrameMetadata::new(message.attributes().clone(), Some(payload_encoding)).expect("metadata")
}

fn metadata_no_payload(topic: UUri) -> UFrameMetadata {
    let message = UMessageBuilder::publish(topic).build().expect("message");
    UFrameMetadata::new(message.attributes().clone(), None).expect("metadata")
}

async fn prime_subscriber<W>(
    transport: &UWireTransport<Iceoryx2PubSub, W, NativePrefixProtobufMetadataCodec>,
    source: &UUri,
) where
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

#[tokio::test(flavor = "multi_thread")]
async fn prepared_metadata_passes_through_for_required_wires() {
    let _guard = iceoryx2_test_guard().await;
    assert_prepared_metadata::<ProtobufWire>(
        "protobuf-prepared",
        PayloadEncoding::Standard(UPayloadFormat::Protobuf),
    )
    .await;
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
    let core = Iceoryx2PubSub::new();
    let transport = UWireTransport::new(core, W::default(), NativePrefixProtobufMetadataCodec);
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
    let decoded = NativePrefixProtobufMetadataCodec
        .decode_frame_metadata(W::metadata_context(), tx.encoded_metadata())
        .expect("decode");
    assert_eq!(decoded, frame_metadata);
}

#[tokio::test(flavor = "multi_thread")]
async fn external_xcdrv2_bytes_round_trip_through_real_pull_receive() {
    let _guard = iceoryx2_test_guard().await;
    let publisher = UWireTransport::new(
        Iceoryx2PubSub::new(),
        XcdrV2Wire,
        NativePrefixProtobufMetadataCodec,
    );
    let subscriber = UWireTransport::new(
        Iceoryx2PubSub::new(),
        XcdrV2Wire,
        NativePrefixProtobufMetadataCodec,
    );
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
async fn wrong_wire_is_rejected_before_public_receive() {
    let _guard = iceoryx2_test_guard().await;
    let source = topic("wrong-wire");
    let publisher = UWireTransport::new(
        Iceoryx2PubSub::new(),
        ProtobufWire,
        NativePrefixProtobufMetadataCodec,
    );
    let subscriber = UWireTransport::new(
        Iceoryx2PubSub::new(),
        XcdrV2Wire,
        NativePrefixProtobufMetadataCodec,
    );
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
    let source = topic("payload-family-mismatch");
    let publisher = UWireTransport::new(
        Iceoryx2PubSub::new(),
        XcdrV2Wire,
        NativePrefixProtobufMetadataCodec,
    );
    let subscriber = UWireTransport::new(
        Iceoryx2PubSub::new(),
        XcdrWireWrongPayloadFamily,
        NativePrefixProtobufMetadataCodec,
    );
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
    let publisher = UWireTransport::new(
        Iceoryx2PubSub::new(),
        XcdrV2Wire,
        NativePrefixProtobufMetadataCodec,
    );
    let subscriber = UWireTransport::new(
        Iceoryx2PubSub::new(),
        XcdrV2Wire,
        NativePrefixProtobufMetadataCodec,
    );
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
    let publisher = UWireTransport::new(
        Iceoryx2PubSub::new(),
        XcdrV2Wire,
        NativePrefixProtobufMetadataCodec,
    );
    let subscriber = UWireTransport::new(
        Iceoryx2PubSub::new(),
        XcdrV2Wire,
        NativePrefixProtobufMetadataCodec,
    );
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
    let source = topic("listener-unregister");
    let publisher = UWireTransport::new(
        Iceoryx2PubSub::new(),
        XcdrV2Wire,
        NativePrefixProtobufMetadataCodec,
    );
    let listener_transport = UWireTransport::new(
        Iceoryx2PubSub::new(),
        XcdrV2Wire,
        NativePrefixProtobufMetadataCodec,
    );
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

async fn send_payload(
    publisher: &UWireTransport<Iceoryx2PubSub, XcdrV2Wire, NativePrefixProtobufMetadataCodec>,
    source: UUri,
    payload: &[u8],
) {
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
impl UZeroCopyListener<UWireRx<Iceoryx2RxLease, XcdrV2Wire, NativePrefixProtobufMetadataCodec>>
    for CountingListener
{
    async fn on_receive_zero_copy(
        &self,
        frame: UWireRx<Iceoryx2RxLease, XcdrV2Wire, NativePrefixProtobufMetadataCodec>,
    ) {
        self.payloads
            .lock()
            .expect("payload lock")
            .push(frame.try_contiguous_payload().unwrap_or_default().to_vec());
    }
}
