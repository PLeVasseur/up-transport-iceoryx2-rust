// ################################################################################
// Copyright (c) 2026 Contributors to the Eclipse Foundation
//
// See the NOTICE file(s) distributed with this work for additional
// information regarding copyright ownership.
//
// This program and the accompanying materials are made available under the
// terms of the Apache License Version 2.0 which is available at
// https://www.apache.org/licenses/LICENSE-2.0
//
// SPDX-License-Identifier: Apache-2.0
// ################################################################################

use std::sync::Arc;

use protobuf::well_known_types::wrappers::StringValue;
use tokio::{sync::mpsc, time::Duration};
use up_rust::{
    ProtobufWire, UAttributes, UCode, UEncoding, UFrameMetadata, UMessageType, UPriority, UUID,
    UUri,
    wire::{UDeserializer, USerializer, UWireError, WireFormat},
    zero_copy::{
        UContiguousZeroCopyRxFrame, UTxBuffer, UZeroCopyListener, UZeroCopyRxFrame,
        UZeroCopyTransport, UZeroCopyTransportExt,
    },
};
use up_transport_iceoryx2_rust::{
    Iceoryx2PubSub, Iceoryx2RxLease, MessagingPattern, transport::UTransportIceoryx2,
};

#[derive(Clone, Debug, Eq, PartialEq)]
struct TestReading {
    sensor_id: u16,
    counter: u32,
}

struct TestReadingWire;

struct AlignedTestReadingWire;

impl WireFormat for TestReadingWire {
    fn name() -> &'static str {
        "test-reading-v1"
    }

    fn encoding() -> UEncoding {
        UEncoding::new(
            Self::name(),
            "application/x.up-test-reading",
            Some("urn:uprotocol:test:reading:v1"),
        )
    }
}

impl WireFormat for AlignedTestReadingWire {
    fn name() -> &'static str {
        "aligned-test-reading-v1"
    }

    fn encoding() -> UEncoding {
        UEncoding::new(
            Self::name(),
            "application/x.up-test-reading",
            Some("urn:uprotocol:test:reading:aligned:v1"),
        )
    }
}

impl USerializer<TestReadingWire> for TestReading {
    fn encoded_len(&self) -> usize {
        6
    }

    fn serialize_into(&self, dst: &mut [u8]) -> Result<usize, UWireError> {
        let expected = <Self as USerializer<TestReadingWire>>::encoded_len(self);
        let actual = dst.len();
        if actual < expected {
            return Err(UWireError::buffer_too_small(expected, actual));
        }
        let sensor_id = dst
            .get_mut(..2)
            .ok_or_else(|| UWireError::buffer_too_small(expected, actual))?;
        sensor_id.copy_from_slice(&self.sensor_id.to_be_bytes());
        let counter = dst
            .get_mut(2..6)
            .ok_or_else(|| UWireError::buffer_too_small(expected, actual))?;
        counter.copy_from_slice(&self.counter.to_be_bytes());
        Ok(expected)
    }
}

impl USerializer<AlignedTestReadingWire> for TestReading {
    const ALIGNMENT: usize = 64;

    fn encoded_len(&self) -> usize {
        6
    }

    fn serialize_into(&self, dst: &mut [u8]) -> Result<usize, UWireError> {
        <Self as USerializer<TestReadingWire>>::serialize_into(self, dst)
    }
}

impl<'a> UDeserializer<'a, TestReadingWire> for TestReading {
    fn deserialize_from(src: &'a [u8]) -> Result<Self, UWireError> {
        if src.len() != 6 {
            return Err(UWireError::invalid_payload(format!(
                "expected 6 bytes, got {}",
                src.len()
            )));
        }
        Ok(Self {
            sensor_id: u16::from_be_bytes(
                src.get(..2)
                    .ok_or_else(|| UWireError::invalid_payload("missing sensor_id"))?
                    .try_into()
                    .map_err(|_| UWireError::invalid_payload("invalid sensor_id"))?,
            ),
            counter: u32::from_be_bytes(
                src.get(2..6)
                    .ok_or_else(|| UWireError::invalid_payload("missing counter"))?
                    .try_into()
                    .map_err(|_| UWireError::invalid_payload("invalid counter"))?,
            ),
        })
    }
}

struct LeaseSender(mpsc::UnboundedSender<(Option<UEncoding>, TestReading)>);

#[async_trait::async_trait]
impl UZeroCopyListener<Iceoryx2RxLease> for LeaseSender {
    async fn on_receive_zero_copy(&self, frame: Iceoryx2RxLease) {
        let encoding = frame.metadata().encoding().cloned();
        let reading = frame
            .deserialize_borrowed::<TestReadingWire, TestReading>()
            .expect("failed to deserialize zero-copy payload");
        self.0
            .send((encoding, reading))
            .expect("failed to send zero-copy listener result");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn zero_copy_transport_round_trips_custom_wire_format()
-> Result<(), Box<dyn std::error::Error>> {
    let authority = format!("iox-test-{}", std::process::id());
    let topic = UUri::try_from_parts(&authority, 0x4210, 1, 0x9002)?;
    let subscriber = UTransportIceoryx2::build(MessagingPattern::PublishSubscribe)?;
    let publisher = UTransportIceoryx2::build(MessagingPattern::PublishSubscribe)?;

    // Create the subscriber port before publishing. An initial empty receive is expected.
    let _ = subscriber.receive_zero_copy(&topic, None).await;

    let reading = TestReading {
        sensor_id: 8,
        counter: 84,
    };
    let send_topic = topic.clone();
    let send_reading = reading.clone();
    let sender = tokio::spawn(async move {
        for _ in 0..50 {
            publisher
                .send_serialized_zero_copy::<TestReadingWire, _>(
                    UFrameMetadata::publish(send_topic.clone()),
                    &send_reading,
                )
                .await?;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Ok::<(), up_rust::UStatus>(())
    });

    for _ in 0..100 {
        match subscriber.receive_zero_copy(&topic, None).await {
            Ok(rx) => {
                assert_eq!(rx.metadata().encoding(), Some(&TestReadingWire::encoding()));
                assert_eq!(
                    rx.deserialize_borrowed::<TestReadingWire, TestReading>()?,
                    reading
                );
                sender.abort();
                return Ok(());
            }
            Err(status) if status.get_code() == UCode::NOT_FOUND => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(status) => return Err(status.into()),
        }
    }

    sender.abort();
    Err("timed out waiting for a zero-copy sample".into())
}

#[tokio::test(flavor = "multi_thread")]
async fn zero_copy_transport_round_trips_protobuf_wire_format()
-> Result<(), Box<dyn std::error::Error>> {
    let authority = format!("iox-pb-test-{}", std::process::id());
    let topic = UUri::try_from_parts(&authority, 0x4210, 1, 0x9006)?;
    let subscriber = UTransportIceoryx2::build(MessagingPattern::PublishSubscribe)?;
    let publisher = UTransportIceoryx2::build(MessagingPattern::PublishSubscribe)?;

    let _ = subscriber.receive_zero_copy(&topic, None).await;

    let mut payload = StringValue::new();
    payload.value = "protobuf over iceoryx2 zero-copy".to_string();
    let send_topic = topic.clone();
    let send_payload = payload.clone();
    let sender = tokio::spawn(async move {
        for _ in 0..50 {
            publisher
                .send_serialized_zero_copy::<ProtobufWire, _>(
                    UFrameMetadata::publish(send_topic.clone()),
                    &send_payload,
                )
                .await?;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Ok::<(), up_rust::UStatus>(())
    });

    for _ in 0..100 {
        match subscriber.receive_zero_copy(&topic, None).await {
            Ok(rx) => {
                assert_eq!(rx.metadata().encoding(), Some(&ProtobufWire::encoding()));
                let decoded: StringValue = rx.deserialize_borrowed::<ProtobufWire, _>()?;
                assert_eq!(decoded.value, payload.value);
                sender.abort();
                return Ok(());
            }
            Err(status) if status.get_code() == UCode::NOT_FOUND => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(status) => return Err(status.into()),
        }
    }

    sender.abort();
    Err("timed out waiting for a protobuf zero-copy sample".into())
}

#[tokio::test(flavor = "multi_thread")]
async fn zero_copy_reserve_honors_payload_alignment() -> Result<(), Box<dyn std::error::Error>> {
    let authority = format!("iox-align-test-{}", std::process::id());
    let topic = UUri::try_from_parts(&authority, 0x4210, 1, 0x9010)?;
    let subscriber = UTransportIceoryx2::build(MessagingPattern::PublishSubscribe)?;
    let publisher = UTransportIceoryx2::build(MessagingPattern::PublishSubscribe)?;

    let _ = subscriber.receive_zero_copy(&topic, None).await;

    let reading = TestReading {
        sensor_id: 10,
        counter: 64,
    };
    let mut loan = publisher
        .reserve(
            UFrameMetadata::publish(topic.clone())
                .with_encoding(AlignedTestReadingWire::encoding()),
            <TestReading as USerializer<AlignedTestReadingWire>>::encoded_len(&reading),
            <TestReading as USerializer<AlignedTestReadingWire>>::ALIGNMENT,
        )
        .await?;
    assert_eq!(loan.payload_mut().as_ptr() as usize % 64, 0);
    <TestReading as USerializer<AlignedTestReadingWire>>::serialize_into(
        &reading,
        loan.payload_mut(),
    )?;
    publisher.send_zero_copy(loan).await?;

    for _ in 0..100 {
        match subscriber.receive_zero_copy(&topic, None).await {
            Ok(rx) => {
                assert_eq!(rx.contiguous_payload().as_ptr() as usize % 64, 0);
                assert_eq!(rx.contiguous_payload(), &[0, 10, 0, 0, 0, 64]);
                return Ok(());
            }
            Err(status) if status.get_code() == UCode::NOT_FOUND => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(status) => return Err(status.into()),
        }
    }

    Err("timed out waiting for an aligned zero-copy sample".into())
}

#[tokio::test(flavor = "multi_thread")]
async fn zero_copy_transport_preserves_native_frame_metadata()
-> Result<(), Box<dyn std::error::Error>> {
    let authority = format!("iox-metadata-test-{}", std::process::id());
    let source = UUri::try_from_parts(&authority, 0x4210, 1, 0x9008)?;
    let sink = UUri::try_from_parts(&authority, 0x4210, 1, 0)?;
    let subscriber = UTransportIceoryx2::build(MessagingPattern::PublishSubscribe)?;
    let publisher = UTransportIceoryx2::build(MessagingPattern::PublishSubscribe)?;

    let _ = subscriber.receive_zero_copy(&source, Some(&sink)).await;

    let id = UUID::build();
    let attributes = UAttributes::new(
        id.clone(),
        source.clone(),
        Some(sink.clone()),
        UMessageType::Notification,
    )
    .with_priority(UPriority::CS6)
    .with_ttl(6_000)
    .with_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00");
    let header = UFrameMetadata::new(attributes, TestReadingWire::encoding());
    let reading = TestReading {
        sensor_id: 12,
        counter: 144,
    };
    let send_header = header.clone();
    let send_reading = reading.clone();
    let sender = tokio::spawn(async move {
        for _ in 0..50 {
            publisher
                .send_serialized_zero_copy::<TestReadingWire, _>(send_header.clone(), &send_reading)
                .await?;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Ok::<(), up_rust::UStatus>(())
    });

    for _ in 0..100 {
        match subscriber.receive_zero_copy(&source, Some(&sink)).await {
            Ok(rx) => {
                let received = rx.metadata().attributes();
                assert_eq!(received.id(), &id);
                assert_eq!(received.source(), &source);
                assert_eq!(received.sink(), Some(&sink));
                assert_eq!(received.message_type(), UMessageType::Notification);
                assert_eq!(received.priority(), UPriority::CS6);
                assert_eq!(received.ttl(), Some(6_000));
                assert_eq!(received.request_id(), None);
                assert_eq!(
                    received.traceparent(),
                    Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00")
                );
                assert_eq!(received.token(), None);
                assert_eq!(received.permission_level(), None);
                assert_eq!(received.commstatus(), None);
                assert_eq!(rx.metadata().encoding(), Some(&TestReadingWire::encoding()));
                assert_eq!(
                    rx.deserialize_borrowed::<TestReadingWire, TestReading>()?,
                    reading
                );
                sender.abort();
                return Ok(());
            }
            Err(status) if status.get_code() == UCode::NOT_FOUND => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(status) => return Err(status.into()),
        }
    }

    sender.abort();
    Err("timed out waiting for metadata zero-copy sample".into())
}

#[tokio::test(flavor = "multi_thread")]
async fn zero_copy_receive_filters_mismatched_sink() -> Result<(), Box<dyn std::error::Error>> {
    let authority = format!("iox-sink-filter-test-{}", std::process::id());
    let source = UUri::try_from_parts(&authority, 0x4210, 1, 0x9011)?;
    let sink_a = UUri::try_from_parts(&authority, 0x4211, 1, 0)?;
    let sink_b = UUri::try_from_parts(&authority, 0x4212, 1, 0)?;
    let subscriber = UTransportIceoryx2::build(MessagingPattern::PublishSubscribe)?;
    let publisher = UTransportIceoryx2::build(MessagingPattern::PublishSubscribe)?;

    let _ = subscriber.receive_zero_copy(&source, Some(&sink_a)).await;

    let header = UFrameMetadata::new(
        UAttributes::new(
            UUID::build(),
            source.clone(),
            Some(sink_b),
            UMessageType::Notification,
        ),
        TestReadingWire::encoding(),
    );
    publisher
        .send_serialized_zero_copy::<TestReadingWire, _>(
            header,
            &TestReading {
                sensor_id: 1,
                counter: 2,
            },
        )
        .await?;

    for _ in 0..100 {
        match subscriber.receive_zero_copy(&source, Some(&sink_a)).await {
            Err(status) if status.get_code() == UCode::NOT_FOUND => return Ok(()),
            Err(status) => return Err(status.into()),
            Ok(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }

    Err("sink A receive delivered a sink B sample".into())
}

#[tokio::test(flavor = "multi_thread")]
async fn zero_copy_listener_round_trips_custom_wire_format()
-> Result<(), Box<dyn std::error::Error>> {
    let authority = format!("iox-listener-test-{}", std::process::id());
    let topic = UUri::try_from_parts(&authority, 0x4210, 1, 0x9003)?;
    let subscriber = UTransportIceoryx2::build(MessagingPattern::PublishSubscribe)?;
    let publisher = UTransportIceoryx2::build(MessagingPattern::PublishSubscribe)?;
    let (tx, mut rx) = mpsc::unbounded_channel();
    let listener: Arc<dyn UZeroCopyListener<Iceoryx2RxLease>> = Arc::new(LeaseSender(tx));

    subscriber
        .register_zero_copy_listener(&topic, None, listener)
        .await?;

    let reading = TestReading {
        sensor_id: 9,
        counter: 126,
    };
    let send_topic = topic.clone();
    let send_reading = reading.clone();
    let sender = tokio::spawn(async move {
        for _ in 0..50 {
            publisher
                .send_serialized_zero_copy::<TestReadingWire, _>(
                    UFrameMetadata::publish(send_topic.clone()),
                    &send_reading,
                )
                .await?;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Ok::<(), up_rust::UStatus>(())
    });

    let (encoding, decoded) = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await?
        .expect("zero-copy listener result channel closed");
    sender.abort();

    assert_eq!(encoding, Some(TestReadingWire::encoding()));
    assert_eq!(decoded, reading);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn zero_copy_listener_filters_mismatched_sink() -> Result<(), Box<dyn std::error::Error>> {
    let authority = format!("iox-listener-sink-filter-test-{}", std::process::id());
    let source = UUri::try_from_parts(&authority, 0x4210, 1, 0x9012)?;
    let sink_a = UUri::try_from_parts(&authority, 0x4211, 1, 0)?;
    let sink_b = UUri::try_from_parts(&authority, 0x4212, 1, 0)?;
    let subscriber = UTransportIceoryx2::build(MessagingPattern::PublishSubscribe)?;
    let publisher = UTransportIceoryx2::build(MessagingPattern::PublishSubscribe)?;
    let (tx, mut rx) = mpsc::unbounded_channel();
    let listener: Arc<dyn UZeroCopyListener<Iceoryx2RxLease>> = Arc::new(LeaseSender(tx));

    subscriber
        .register_zero_copy_listener(&source, Some(&sink_a), listener)
        .await?;

    let header = UFrameMetadata::new(
        UAttributes::new(
            UUID::build(),
            source,
            Some(sink_b),
            UMessageType::Notification,
        ),
        TestReadingWire::encoding(),
    );
    publisher
        .send_serialized_zero_copy::<TestReadingWire, _>(
            header,
            &TestReading {
                sensor_id: 3,
                counter: 4,
            },
        )
        .await?;

    assert!(
        tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .is_err(),
        "sink A listener delivered a sink B sample"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn exposes_iceoryx2_service_names_for_streamer_discovery()
-> Result<(), Box<dyn std::error::Error>> {
    let authority = format!("iox-discovery-test-{}", std::process::id());
    let topic = UUri::try_from_parts(&authority, 0x4210, 1, 0x9004)?;
    let transport = UTransportIceoryx2::build(MessagingPattern::PublishSubscribe)?;
    let expected_service = Iceoryx2PubSub::publish_subscribe_service_name(&topic, None)?;

    let _ = transport.receive_zero_copy(&topic, None).await;
    let discovered_services = transport.discover_service_names()?;

    assert!(
        discovered_services
            .iter()
            .any(|name| name == &expected_service),
        "expected {expected_service} in {discovered_services:?}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn discovers_matching_iceoryx2_services_by_source_attributes()
-> Result<(), Box<dyn std::error::Error>> {
    let authority = format!("iox-attr-discovery-test-{}", std::process::id());
    let topic = UUri::try_from_parts(&authority, 0x1234_4210, 1, 0x9013)?;
    let wildcard_instance_filter = UUri::try_from_parts(&authority, 0xFFFF_4210, 1, 0x9013)?;
    let mismatched_filter = UUri::try_from_parts(&authority, 0xFFFF_4211, 1, 0x9013)?;
    let transport = UTransportIceoryx2::build(MessagingPattern::PublishSubscribe)?;
    let expected_service = Iceoryx2PubSub::publish_subscribe_service_name(&topic, None)?;

    transport
        .send_serialized_zero_copy::<TestReadingWire, _>(
            UFrameMetadata::publish(topic.clone()),
            &TestReading {
                sensor_id: 13,
                counter: 169,
            },
        )
        .await?;

    let matching = transport.discover_matching_service_names(&wildcard_instance_filter)?;
    assert!(
        matching.iter().any(|service| service == &expected_service),
        "expected {expected_service} in {matching:?}"
    );
    let mismatched = transport.discover_matching_service_names(&mismatched_filter)?;
    assert!(
        !mismatched
            .iter()
            .any(|service| service == &expected_service),
        "did not expect {expected_service} in {mismatched:?}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn zero_copy_listener_discovers_late_matching_publisher()
-> Result<(), Box<dyn std::error::Error>> {
    let authority = format!("iox-late-discovery-test-{}", std::process::id());
    let topic = UUri::try_from_parts(&authority, 0x2345_4210, 1, 0x9014)?;
    let wildcard_instance_filter = UUri::try_from_parts(&authority, 0xFFFF_4210, 1, 0x9014)?;
    let subscriber = UTransportIceoryx2::build(MessagingPattern::PublishSubscribe)?;
    let publisher = UTransportIceoryx2::build(MessagingPattern::PublishSubscribe)?;
    let (tx, mut rx) = mpsc::unbounded_channel();
    let listener: Arc<dyn UZeroCopyListener<Iceoryx2RxLease>> = Arc::new(LeaseSender(tx));

    subscriber
        .register_zero_copy_listener(&wildcard_instance_filter, None, listener)
        .await?;

    let reading = TestReading {
        sensor_id: 14,
        counter: 196,
    };
    let send_topic = topic.clone();
    let send_reading = reading.clone();
    let sender = tokio::spawn(async move {
        for _ in 0..50 {
            publisher
                .send_serialized_zero_copy::<TestReadingWire, _>(
                    UFrameMetadata::publish(send_topic.clone()),
                    &send_reading,
                )
                .await?;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Ok::<(), up_rust::UStatus>(())
    });

    let (encoding, decoded) = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await?
        .expect("zero-copy listener result channel closed");
    sender.abort();

    assert_eq!(encoding, Some(TestReadingWire::encoding()));
    assert_eq!(decoded, reading);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn zero_copy_listener_fanout_delivers_same_sample_to_two_listeners()
-> Result<(), Box<dyn std::error::Error>> {
    let authority = format!("iox-fanout-test-{}", std::process::id());
    let topic = UUri::try_from_parts(&authority, 0x4210, 1, 0x9015)?;
    let subscriber = UTransportIceoryx2::build(MessagingPattern::PublishSubscribe)?;
    let publisher = UTransportIceoryx2::build(MessagingPattern::PublishSubscribe)?;
    let (tx_a, mut rx_a) = mpsc::unbounded_channel();
    let (tx_b, mut rx_b) = mpsc::unbounded_channel();
    let listener_a: Arc<dyn UZeroCopyListener<Iceoryx2RxLease>> = Arc::new(LeaseSender(tx_a));
    let listener_b: Arc<dyn UZeroCopyListener<Iceoryx2RxLease>> = Arc::new(LeaseSender(tx_b));

    subscriber
        .register_zero_copy_listener(&topic, None, listener_a)
        .await?;
    subscriber
        .register_zero_copy_listener(&topic, None, listener_b)
        .await?;

    let reading = TestReading {
        sensor_id: 15,
        counter: 225,
    };
    let send_topic = topic.clone();
    let send_reading = reading.clone();
    let sender = tokio::spawn(async move {
        for _ in 0..50 {
            publisher
                .send_serialized_zero_copy::<TestReadingWire, _>(
                    UFrameMetadata::publish(send_topic.clone()),
                    &send_reading,
                )
                .await?;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Ok::<(), up_rust::UStatus>(())
    });

    let (_, decoded_a) = tokio::time::timeout(Duration::from_secs(5), rx_a.recv())
        .await?
        .expect("first listener result channel closed");
    let (_, decoded_b) = tokio::time::timeout(Duration::from_secs(5), rx_b.recv())
        .await?
        .expect("second listener result channel closed");
    sender.abort();

    assert_eq!(decoded_a, reading);
    assert_eq!(decoded_b, reading);
    Ok(())
}
