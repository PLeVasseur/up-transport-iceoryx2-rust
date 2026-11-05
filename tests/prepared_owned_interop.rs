// Copyright (c) 2026 Contributors to the Eclipse Foundation
// SPDX-License-Identifier: Apache-2.0
//! Actual prepared/owned service discovery and method-shared request interoperability.
#![cfg(feature = "benchmark-owned")]

#[path = "support/prepared_native.rs"]
#[allow(dead_code)] // This corpus uses setup helpers rather than stable fixtures.
mod common;
use std::{sync::Arc, time::Duration};
use up_rust::{
    HeaderAdmission, HeaderLimits, OpaqueTemplate, PayloadEncoding, PreparedTransportCore,
    ProtobufWire, RegistrationConfig, RegistrationInbox, RouteConfig, UFrameMetadata, UOwnedFrame,
    UOwnedListener, UOwnedTransport, UUri,
};
use up_transport_iceoryx2_rust::{
    BenchmarkOwnedIceoryx2Core, Iceoryx2PreparedCore, Iceoryx2PubSub, Iceoryx2RouteReservation,
};

struct OwnedReceiver(tokio::sync::mpsc::UnboundedSender<UOwnedFrame>);
#[async_trait::async_trait]
impl UOwnedListener for OwnedReceiver {
    async fn on_receive_owned(&self, frame: UOwnedFrame) {
        self.0.send(frame).unwrap();
    }
}

async fn round_trip(prepared_sender: bool, notification: bool) {
    let source = common::source();
    let sink = notification.then(|| UUri::try_from_parts("receiver", 0x0001_4211, 1, 0).unwrap());
    let config = common::config(4096, 2);
    let prepared = Arc::new(
        Iceoryx2PreparedCore::new(
            config.clone(),
            &[Iceoryx2RouteReservation {
                source: source.clone(),
                sink: sink.clone(),
                limits: common::limits(64),
            }],
            8,
            2,
        )
        .unwrap(),
    );
    let owned = BenchmarkOwnedIceoryx2Core::new(Iceoryx2PubSub::try_with_config(config).unwrap())
        .with_selected_wire(ProtobufWire);
    let builder = match &sink {
        Some(sink) => UFrameMetadata::notification(source.clone(), sink.clone()),
        None => UFrameMetadata::publish(source.clone()),
    };
    let metadata = builder
        .with_payload_encoding(PayloadEncoding::PROTOBUF)
        .build()
        .unwrap();
    if prepared_sender {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let listener: Arc<dyn UOwnedListener> = Arc::new(OwnedReceiver(tx));
        owned
            .register_owned_listener(&source, sink.as_ref(), listener.clone())
            .await
            .unwrap();
        let route = OpaqueTemplate::for_wire::<ProtobufWire>()
            .unwrap()
            .route(
                Arc::clone(&prepared),
                source.clone(),
                sink.clone(),
                common::limits(64),
                RouteConfig::default(),
            )
            .unwrap();
        route
            .send_bytes(
                up_rust::TxHeaderParts::from(&metadata),
                Some(b"native-interop"),
            )
            .unwrap();
        let frame = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(frame.metadata(), &metadata);
        assert_eq!(frame.payload_bytes(), b"native-interop");
        owned
            .unregister_owned_listener(&source, sink.as_ref(), listener)
            .await
            .unwrap();
    } else {
        let inbox = Arc::new(RegistrationInbox::new(common::registration()).unwrap());
        let mut registration = prepared
            .attach(&source, sink.as_ref(), inbox.clone())
            .await
            .unwrap();
        owned
            .send_owned(
                UOwnedFrame::with_payload(metadata.clone(), b"native-interop".to_vec()).unwrap(),
            )
            .await
            .unwrap();
        let frame = tokio::time::timeout(Duration::from_secs(5), inbox.recv())
            .await
            .unwrap()
            .unwrap();
        let frame = HeaderAdmission::for_wire::<ProtobufWire>(HeaderLimits::default())
            .unwrap()
            .admit(frame)
            .unwrap();
        assert_eq!(frame.header().unwrap().id, *metadata.id());
        assert_eq!(frame.header().unwrap().source, source);
        assert_eq!(frame.header().unwrap().sink, sink);
        assert_eq!(frame.payload().unwrap().bytes(), b"native-interop");
        prepared.detach(&mut registration).unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepared_publish_is_discovered_by_owned_receiver() {
    round_trip(true, false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn owned_publish_reaches_prepared_receiver() {
    round_trip(false, false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepared_notification_is_discovered_by_owned_receiver() {
    round_trip(true, true).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn owned_notification_reaches_prepared_receiver() {
    round_trip(false, true).await;
}

async fn shared_requests(prepared_sender: bool) {
    let config = common::config(4096, 4);
    let method = UUri::try_from_parts("service", 0x0001_4220, 1, 0x1000).unwrap();
    let clients: Vec<_> = (0..3)
        .map(|id| UUri::try_from_parts("client", 0x0001_0000 | (0x4300 + id), 1, 0).unwrap())
        .collect();
    let catalog: Vec<_> = clients
        .iter()
        .map(|client| Iceoryx2RouteReservation {
            source: client.clone(),
            sink: Some(method.clone()),
            limits: common::limits(64),
        })
        .collect();
    let prepared = Arc::new(Iceoryx2PreparedCore::new(config.clone(), &catalog, 16, 4).unwrap());
    let owned = BenchmarkOwnedIceoryx2Core::new(Iceoryx2PubSub::try_with_config(config).unwrap())
        .with_selected_wire(ProtobufWire);
    let filter = UUri::try_from_parts("client", u32::MAX, u8::MAX, u16::MAX).unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let listener: Arc<dyn UOwnedListener> = Arc::new(OwnedReceiver(tx));
    let inbox = Arc::new(
        RegistrationInbox::new(RegistrationConfig {
            capacity: 4,
            overflow: up_rust::OverflowPolicy::DropNewest,
        })
        .unwrap(),
    );
    let mut registration = None;
    if prepared_sender {
        owned
            .register_owned_listener(&filter, Some(&method), listener.clone())
            .await
            .unwrap();
    } else {
        registration = Some(
            prepared
                .attach(&filter, Some(&method), inbox.clone())
                .await
                .unwrap(),
        );
    }
    let admission = HeaderAdmission::for_wire::<ProtobufWire>(HeaderLimits::default()).unwrap();
    for client in clients {
        let metadata =
            UFrameMetadata::request(method.clone(), client.clone(), Duration::from_secs(5))
                .with_payload_encoding(PayloadEncoding::PROTOBUF)
                .build()
                .unwrap();
        if prepared_sender {
            let route = OpaqueTemplate::for_wire::<ProtobufWire>()
                .unwrap()
                .route(
                    Arc::clone(&prepared),
                    client.clone(),
                    Some(method.clone()),
                    common::limits(64),
                    RouteConfig::default(),
                )
                .unwrap();
            route
                .send_bytes(up_rust::TxHeaderParts::from(&metadata), Some(b"request"))
                .unwrap();
            let frame = tokio::time::timeout(Duration::from_secs(5), rx.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(frame.metadata(), &metadata);
            assert_eq!(frame.payload_bytes(), b"request");
        } else {
            owned
                .send_owned(
                    UOwnedFrame::with_payload(metadata.clone(), b"request".to_vec()).unwrap(),
                )
                .await
                .unwrap();
            let frame = admission
                .admit(
                    tokio::time::timeout(Duration::from_secs(5), inbox.recv())
                        .await
                        .unwrap()
                        .unwrap(),
                )
                .unwrap();
            assert_eq!(frame.header().unwrap().source, client);
            assert_eq!(frame.header().unwrap().id, *metadata.id());
            assert_eq!(frame.payload().unwrap().bytes(), b"request");
        }
    }
    if let Some(mut registration) = registration {
        prepared.detach(&mut registration).unwrap();
    } else {
        owned
            .unregister_owned_listener(&filter, Some(&method), listener)
            .await
            .unwrap();
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn three_prepared_clients_share_method_discovered_by_owned_receiver() {
    shared_requests(true).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn three_owned_clients_share_method_discovered_by_prepared_receiver() {
    shared_requests(false).await;
}
