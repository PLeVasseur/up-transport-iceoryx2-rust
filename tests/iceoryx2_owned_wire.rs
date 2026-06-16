// ################################################################################
// Copyright (c) 2026 Contributors to the Eclipse Foundation
//
// SPDX-License-Identifier: Apache-2.0
// ################################################################################

use bytes::Bytes;
use up_rust::{
    EncodedOwnedFrame, PayloadEncoding, ProtobufWire, UFrameMetadata, UMessageBuilder, UOwnedFrame,
    UOwnedTransport, UPayloadFormat, UUri, UWireMetadata, UWithWire,
};
use up_transport_iceoryx2_rust::Iceoryx2OwnedCore;

fn topic(test_name: &str) -> UUri {
    let authority = format!("iox-owned-{test_name}-{}", std::process::id());
    UUri::try_from_parts(&authority, 0x4210, 0x01, 0x9400).expect("topic URI")
}

fn metadata(topic: UUri) -> UFrameMetadata {
    let message = UMessageBuilder::publish(topic).build().expect("message");
    UFrameMetadata::new(
        message.attributes().clone(),
        Some(PayloadEncoding::Standard(UPayloadFormat::Protobuf)),
    )
    .expect("metadata")
}

#[tokio::test]
async fn owned_core_carries_prepared_metadata_behind_feature() {
    let core = Iceoryx2OwnedCore::new();
    let transport = core.clone().with_wire(ProtobufWire::default());
    let frame_metadata = metadata(topic("send"));
    let frame =
        UOwnedFrame::with_payload(frame_metadata.clone(), b"owned".to_vec()).expect("owned frame");

    transport.send_owned(frame).await.expect("send owned");

    let sent = core.last_sent().await.expect("sent frame");
    let decoded = ProtobufWire::decode_frame_metadata(sent.encoded_metadata()).expect("decode");
    assert_eq!(decoded, frame_metadata);
    assert_eq!(sent.payload(), Some(&b"owned"[..]));
}

#[tokio::test]
async fn owned_core_rejects_wrong_wire_before_exposure() {
    let source = topic("wrong-wire");
    let metadata = metadata(source.clone());
    let encoded = ProtobufWire::encode_frame_metadata(&metadata).expect("encode");
    let core = Iceoryx2OwnedCore::new();
    core.push_encoded_owned(EncodedOwnedFrame::new(
        encoded,
        Some(Bytes::from_static(b"owned")),
    ))
    .await;

    let transport = core.with_wire(up_wire_xcdrv2::XcdrV2Wire);
    let error = transport
        .receive_owned(&source, None)
        .await
        .expect_err("wrong wire rejected");
    assert_eq!(error.get_code(), up_rust::UCode::InvalidArgument);
}
