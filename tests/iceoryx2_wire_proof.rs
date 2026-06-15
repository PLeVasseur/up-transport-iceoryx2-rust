use std::{sync::Arc, sync::Mutex as StdMutex};

use async_trait::async_trait;
use up_rust::{
    PayloadEncoding, PayloadFormat, ProtobufWire, StableContainerWireFormat, UCode, UFrameMetadata,
    UFrameView, UMessageBuilder, UPayloadFormat, UTxBuffer, UTxLoanSpec, UUri, UWireMetadata,
    UWireMetadataError, UWithWire, UZeroCopyListener, UZeroCopyTransport,
};
use up_transport_iceoryx2_rust::{
    Iceoryx2EncodedRxFrame, Iceoryx2WireCore, UPROTOCOL_MAJOR_VERSION, UProtocolHeader,
};
use up_wire_xcdrv2::{VEHICLE_SIGNAL_V1_GOLDEN_BYTES, XCDR_V2_ENCODING_ID, XcdrV2Wire};

fn topic() -> UUri {
    UUri::try_from_parts("vehicle", 0x4210, 0x01, 0x9000).expect("topic URI")
}

fn metadata(payload_encoding: PayloadEncoding) -> UFrameMetadata {
    let message = UMessageBuilder::publish(topic()).build().expect("message");
    UFrameMetadata::new(message.attributes().clone(), Some(payload_encoding)).expect("metadata")
}

fn source_filter() -> UUri {
    topic()
}

#[tokio::test]
async fn prepared_metadata_passes_through_for_required_wires() {
    assert_prepared_metadata::<ProtobufWire>(PayloadEncoding::Standard(UPayloadFormat::Protobuf))
        .await;
    assert_prepared_metadata::<StableContainerWireFormat>(
        PayloadEncoding::custom(
            "up.stable-container-test",
            "application/vnd.uprotocol.stable-container-test",
        )
        .expect("stable encoding"),
    )
    .await;
    assert_prepared_metadata::<XcdrV2Wire>(XcdrV2Wire::encoding()).await;
}

async fn assert_prepared_metadata<W>(payload_encoding: PayloadEncoding)
where
    W: UWireMetadata + Default + Send + Sync + 'static,
{
    let core = Iceoryx2WireCore::new();
    let transport = core.clone().with_wire(W::default());
    let frame_metadata = metadata(payload_encoding);
    let mut tx = transport
        .loan_tx(UTxLoanSpec::payload(frame_metadata.clone(), 4, 1).expect("loan spec"))
        .await
        .expect("loan");
    tx.payload_mut().copy_from_slice(b"data");

    let prepared = core.last_prepared().await.expect("prepared request");
    assert_eq!(prepared.metadata(), &frame_metadata);
    assert_eq!(prepared.encoded_metadata(), tx.encoded_metadata());
    assert_eq!(
        tx.header().metadata_len as usize,
        tx.encoded_metadata().len()
    );

    let decoded = W::decode_frame_metadata(prepared.encoded_metadata()).expect("decode");
    assert_eq!(decoded, frame_metadata);
}

#[tokio::test]
async fn external_xcdrv2_bytes_round_trip_through_pull_receive() {
    let core = Iceoryx2WireCore::new();
    let transport = core.clone().with_wire(XcdrV2Wire);
    let frame_metadata = metadata(XcdrV2Wire::encoding());
    let mut tx = transport
        .loan_tx(
            UTxLoanSpec::payload(frame_metadata, VEHICLE_SIGNAL_V1_GOLDEN_BYTES.len(), 1)
                .expect("loan spec"),
        )
        .await
        .expect("loan");
    tx.payload_mut()
        .copy_from_slice(&VEHICLE_SIGNAL_V1_GOLDEN_BYTES);
    transport.send_zero_copy(tx).await.expect("send");

    let rx = transport
        .receive_zero_copy(&source_filter(), None)
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

#[tokio::test]
async fn wrong_wire_and_payload_family_are_rejected_before_public_receive() {
    let core = Iceoryx2WireCore::new();
    let wrong_wire_metadata = ProtobufWire::encode_frame_metadata(&metadata(
        PayloadEncoding::Standard(UPayloadFormat::Protobuf),
    ))
    .expect("wrong wire metadata");
    let frame = Iceoryx2EncodedRxFrame::new(
        UProtocolHeader {
            uprotocol_major_version: UPROTOCOL_MAJOR_VERSION,
            metadata_len: wrong_wire_metadata.len() as u64,
            payload_len: 0,
            payload_alignment: 1,
        },
        wrong_wire_metadata,
        Vec::new(),
    )
    .expect("frame");
    core.push_encoded_rx(frame).await;
    let transport = core.with_wire(XcdrV2Wire);

    let result = transport.receive_zero_copy(&source_filter(), None).await;
    let error = result.err().expect("wrong metadata rejected");
    assert_eq!(error.get_code(), UCode::InvalidArgument);
}

#[tokio::test]
async fn physical_mirror_mismatch_rejected_before_wire_decode() {
    let encoded =
        XcdrV2Wire::encode_frame_metadata(&metadata(XcdrV2Wire::encoding())).expect("metadata");
    let frame = Iceoryx2EncodedRxFrame::new(
        UProtocolHeader {
            uprotocol_major_version: UPROTOCOL_MAJOR_VERSION,
            metadata_len: encoded.len() as u64 + 1,
            payload_len: 0,
            payload_alignment: 1,
        },
        encoded,
        Vec::new(),
    );
    assert!(frame.is_err());
}

#[tokio::test]
async fn malformed_listener_metadata_is_not_delivered() {
    let core = Iceoryx2WireCore::new();
    let listener = Arc::new(CountingListener::default());
    let transport = core.clone().with_wire(XcdrV2Wire);
    transport
        .register_zero_copy_listener(&source_filter(), None, listener.clone())
        .await
        .expect("register");

    let wrong_wire_metadata = ProtobufWire::encode_frame_metadata(&metadata(
        PayloadEncoding::Standard(UPayloadFormat::Protobuf),
    ))
    .expect("wrong wire metadata");
    let frame = Iceoryx2EncodedRxFrame::new(
        UProtocolHeader {
            uprotocol_major_version: UPROTOCOL_MAJOR_VERSION,
            metadata_len: wrong_wire_metadata.len() as u64,
            payload_len: 4,
            payload_alignment: 1,
        },
        wrong_wire_metadata,
        b"drop".to_vec(),
    )
    .expect("frame");
    core.deliver_encoded_rx(frame).await;

    assert_eq!(listener.payloads(), Vec::<Vec<u8>>::new());
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
impl<W> UZeroCopyListener<up_rust::UWireRx<Iceoryx2EncodedRxFrame, W>> for CountingListener
where
    W: UWireMetadata + Send + Sync + 'static,
{
    async fn on_receive_zero_copy(&self, frame: up_rust::UWireRx<Iceoryx2EncodedRxFrame, W>) {
        self.payloads
            .lock()
            .expect("payload lock")
            .push(frame.try_contiguous_payload().unwrap_or_default().to_vec());
    }
}

#[test]
fn wire_metadata_errors_are_owned_by_up_rust() {
    let error = XcdrV2Wire::decode_frame_metadata(b"not metadata").expect_err("malformed");
    assert!(matches!(error, UWireMetadataError::WrongMagic));
}
