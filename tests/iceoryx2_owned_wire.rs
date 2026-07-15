// ################################################################################
// Copyright (c) 2026 Contributors to the Eclipse Foundation
//
// SPDX-License-Identifier: Apache-2.0
// ################################################################################

use bytes::Bytes;
use up_rust::transport_implementer_api::EncodedOwnedFrame;
use up_rust::wire_implementer_api::{
    NativePrefixFrameMetadataCodec, ProtobufWire, UWire, UWireMetadataCodec,
};
use up_rust::{PayloadEncoding, UFrameMetadata, UOwnedFrame, UOwnedTransport, UUri};
use up_transport_iceoryx2_rust::Iceoryx2OwnedCore;

fn topic(test_name: &str) -> UUri {
    let authority = format!("iox-owned-{test_name}-{}", std::process::id());
    UUri::try_from_parts(&authority, 0x4210, 0x01, 0x9400).expect("topic URI")
}

fn metadata(topic: UUri) -> UFrameMetadata {
    UFrameMetadata::publish(topic)
        .with_payload_encoding(PayloadEncoding::PROTOBUF)
        .build()
        .expect("metadata")
}

#[tokio::test]
async fn owned_core_carries_prepared_metadata_behind_feature() {
    let core = Iceoryx2OwnedCore::new();
    let transport = core.clone().with_selected_wire(ProtobufWire);
    let frame_metadata = metadata(topic("send"));
    let frame =
        UOwnedFrame::with_payload(frame_metadata.clone(), b"owned".to_vec()).expect("owned frame");

    transport.send_owned(frame).await.expect("send owned");

    let sent = core.last_sent().await.expect("sent frame");
    let decoded = NativePrefixFrameMetadataCodec
        .decode_frame_metadata(ProtobufWire::metadata_context(), sent.encoded_metadata())
        .expect("decode");
    assert_eq!(decoded, frame_metadata);
    assert_eq!(sent.payload(), Some(&b"owned"[..]));
}

#[tokio::test]
async fn owned_core_rejects_wrong_wire_before_exposure() {
    let source = topic("wrong-wire");
    let metadata = metadata(source.clone());
    let encoded = NativePrefixFrameMetadataCodec
        .encode_frame_metadata(ProtobufWire::metadata_context(), &metadata)
        .expect("encode");
    let core = Iceoryx2OwnedCore::new();
    core.push_encoded_owned(EncodedOwnedFrame::new(
        encoded,
        Some(Bytes::from_static(b"owned")),
    ))
    .await;

    let transport = core.with_selected_wire(up_wire_xcdrv2::XcdrV2Wire);
    let error = transport
        .receive_owned(&source, None)
        .await
        .expect_err("wrong wire rejected");
    assert_eq!(error.code(), up_rust::UCode::InvalidArgument);
}
