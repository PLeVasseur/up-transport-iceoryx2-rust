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

use std::{mem, sync::Arc};

use protobuf::well_known_types::wrappers::StringValue;
use tokio::{
    sync::{Mutex, MutexGuard, mpsc},
    time::Duration,
};
use up_rust::{
    PayloadEncoding, ProtobufPayload, UAttributes, UCode, UFrameMetadata, UMessageType, UPriority,
    UTxLoanSpec, UUID, UUri, UZeroCopyUninitTransportExt,
    payload::{
        PayloadFormat, PayloadLayout, PlacementDefault, RawBytes, StableContainerPayload,
        UDeserializer, USerializer, UWireError,
    },
    test_util::zero_copy_conformance,
    zero_copy::{
        PayloadLoanProvenance, UContiguousZeroCopyRxFrame, ULoanedContiguousZeroCopyRxFrame,
        UTxBuffer, UZeroCopyListener, UZeroCopyRxFrame, UZeroCopyTransport, UZeroCopyTransportExt,
    },
};

#[repr(C)]
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    Eq,
    PartialEq,
    PlacementDefault,
    up_rust::StablePayload,
    up_rust::ByteBackedStablePayload,
)]
#[stable_payload(type_name = "example.vehicle.VehiclePose")]
struct VehiclePose {
    x: u32,
    y: u32,
}

fn bytes_of_pose(pose: &VehiclePose) -> &[u8] {
    // SAFETY:
    // - `pose` is a valid shared reference to one `VehiclePose` and is therefore
    //   non-null, aligned, and valid for reads of `size_of::<VehiclePose>()`
    //   bytes.
    // - Per https://doc.rust-lang.org/stable/std/slice/fn.from_raw_parts.html#safety:
    //
    //   "data must be non-null, valid for reads for `len * size_of::<T>()` many
    //   bytes, and it must be properly aligned."
    unsafe {
        std::slice::from_raw_parts(
            (pose as *const VehiclePose).cast::<u8>(),
            mem::size_of::<VehiclePose>(),
        )
    }
}

use up_transport_iceoryx2_rust::{
    Iceoryx2PubSub, Iceoryx2PubSubConfig, Iceoryx2RxLease, MessagingPattern,
    transport::UTransportIceoryx2,
};

#[derive(Clone, Debug, Eq, PartialEq)]
struct TestReading {
    sensor_id: u16,
    counter: u32,
}

struct TestReadingWire;

struct AlignedTestReadingWire;

impl PayloadFormat for TestReadingWire {
    fn name() -> &'static str {
        "test-reading-v1"
    }

    fn encoding() -> PayloadEncoding {
        PayloadEncoding::custom(Self::name(), "application/x.up-test-reading")
    }
}

impl PayloadFormat for AlignedTestReadingWire {
    fn name() -> &'static str {
        "aligned-test-reading-v1"
    }

    fn encoding() -> PayloadEncoding {
        PayloadEncoding::custom(Self::name(), "application/x.up-test-reading")
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

struct LeaseSender(mpsc::UnboundedSender<(Option<PayloadEncoding>, TestReading)>);

static ICEORYX2_TEST_MUTEX: Mutex<()> = Mutex::const_new(());

async fn iceoryx2_test_guard() -> MutexGuard<'static, ()> {
    ICEORYX2_TEST_MUTEX.lock().await
}

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
async fn zero_copy_transport_round_trips_custom_payload_codec()
-> Result<(), Box<dyn std::error::Error>> {
    let _guard = iceoryx2_test_guard().await;
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
async fn zero_copy_transport_round_trips_protobuf_payload_codec()
-> Result<(), Box<dyn std::error::Error>> {
    let _guard = iceoryx2_test_guard().await;
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
                .send_serialized_zero_copy::<ProtobufPayload, _>(
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
                assert_eq!(rx.metadata().encoding(), Some(&ProtobufPayload::encoding()));
                let decoded: StringValue = rx.deserialize_borrowed::<ProtobufPayload, _>()?;
                let wrong_codec = rx.deserialize_borrowed::<TestReadingWire, TestReading>();
                assert_eq!(decoded.value, payload.value);
                assert!(matches!(
                    wrong_codec,
                    Err(UWireError::UnsupportedEncoding { .. })
                ));
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
async fn zero_copy_transport_round_trips_stable_container_payload()
-> Result<(), Box<dyn std::error::Error>> {
    let _guard = iceoryx2_test_guard().await;
    let authority = format!("iox-stable-test-{}", std::process::id());
    let topic = UUri::try_from_parts(&authority, 0x4210, 1, 0x9007)?;
    let subscriber = UTransportIceoryx2::build(MessagingPattern::PublishSubscribe)?;
    let publisher = UTransportIceoryx2::build(MessagingPattern::PublishSubscribe)?;

    let _ = subscriber.receive_zero_copy(&topic, None).await;

    let expected = VehiclePose { x: 55, y: 89 };
    let send_topic = topic.clone();
    let send_expected = expected;
    let sender = tokio::spawn(async move {
        for _ in 0..50 {
            publisher
                .send_loaned_payload_as::<StableContainerPayload<VehiclePose>, VehiclePose>(
                    UFrameMetadata::publish(send_topic.clone()),
                    |payload| {
                        payload.x = send_expected.x;
                        payload.y = send_expected.y;
                    },
                )
                .await?;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Ok::<(), up_rust::UStatus>(())
    });

    for _ in 0..100 {
        match subscriber.receive_zero_copy(&topic, None).await {
            Ok(rx) => {
                assert_eq!(
                    rx.metadata().encoding(),
                    Some(&StableContainerPayload::<VehiclePose>::encoding())
                );
                assert_eq!(
                    rx.contiguous_payload().as_ptr() as usize % mem::align_of::<VehiclePose>(),
                    0
                );
                zero_copy_conformance::verify_loaned_rx_payload_layout_for(
                    &rx,
                    mem::size_of::<VehiclePose>(),
                    mem::align_of::<VehiclePose>(),
                )?;
                let pose = zero_copy_conformance::borrow_stable_payload::<VehiclePose>(&rx)?;
                assert_eq!(pose, &expected);
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
    Err("timed out waiting for a stable-container zero-copy sample".into())
}

#[tokio::test(flavor = "multi_thread")]
async fn zero_copy_transport_round_trips_stable_container_uninit_payload()
-> Result<(), Box<dyn std::error::Error>> {
    let _guard = iceoryx2_test_guard().await;
    let authority = format!("iox-stable-uninit-test-{}", std::process::id());
    let topic = UUri::try_from_parts(&authority, 0x4210, 1, 0x9009)?;
    let subscriber = UTransportIceoryx2::build(MessagingPattern::PublishSubscribe)?;
    let publisher = UTransportIceoryx2::build(MessagingPattern::PublishSubscribe)?;

    let _ = subscriber.receive_zero_copy(&topic, None).await;

    let expected = VehiclePose { x: 144, y: 233 };
    let send_topic = topic.clone();
    let sender = tokio::spawn(async move {
        for _ in 0..50 {
            publisher
                .send_uninit_loaned_payload_as::<StableContainerPayload<VehiclePose>, VehiclePose>(
                    UFrameMetadata::publish(send_topic.clone()),
                    |slot| Ok(slot.write(expected)),
                )
                .await?;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Ok::<(), up_rust::UStatus>(())
    });

    for _ in 0..100 {
        match subscriber.receive_zero_copy(&topic, None).await {
            Ok(rx) => {
                assert_eq!(
                    rx.payload_loan_provenance()?,
                    PayloadLoanProvenance::SharedMemory
                );
                let pose = zero_copy_conformance::borrow_stable_payload::<VehiclePose>(&rx)?;
                assert_eq!(pose, &expected);
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
    Err("timed out waiting for a stable-container uninit zero-copy sample".into())
}

#[tokio::test(flavor = "multi_thread")]
async fn static_allocation_rejects_oversized_payload_without_growth()
-> Result<(), Box<dyn std::error::Error>> {
    let _guard = iceoryx2_test_guard().await;
    let authority = format!("iox-static-cap-test-{}", std::process::id());
    let topic = UUri::try_from_parts(&authority, 0x4210, 1, 0x9011)?;
    let publisher = UTransportIceoryx2::build_with_config(
        MessagingPattern::PublishSubscribe,
        Iceoryx2PubSubConfig::static_allocation(1),
    )?;

    let spec = UTxLoanSpec::payload(
        UFrameMetadata::publish(topic).with_encoding(RawBytes::encoding()),
        PayloadLayout::new(64, 1)?,
    )?;
    let result = publisher.loan_tx(spec).await;
    let Err(error) = result else {
        panic!("static iceoryx2 allocation should reject oversized sample");
    };

    assert_eq!(error.get_code(), UCode::RESOURCE_EXHAUSTED);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn zero_copy_loan_spec_rejects_payload_without_encoding()
-> Result<(), Box<dyn std::error::Error>> {
    let _guard = iceoryx2_test_guard().await;
    let authority = format!("iox-missing-encoding-test-{}", std::process::id());
    let topic = UUri::try_from_parts(&authority, 0x4210, 1, 0x9021)?;

    let result = UTxLoanSpec::payload(UFrameMetadata::publish(topic), PayloadLayout::new(1, 1)?);
    match result {
        Ok(_) => panic!("payload bytes without encoding must be rejected"),
        Err(error) => assert_eq!(error.get_code(), UCode::INVALID_ARGUMENT),
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn zero_copy_uninit_loan_spec_rejects_payload_without_encoding()
-> Result<(), Box<dyn std::error::Error>> {
    let _guard = iceoryx2_test_guard().await;
    let authority = format!("iox-uninit-missing-encoding-test-{}", std::process::id());
    let topic = UUri::try_from_parts(&authority, 0x4210, 1, 0x9022)?;

    let result = UTxLoanSpec::payload(UFrameMetadata::publish(topic), PayloadLayout::new(1, 1)?);
    match result {
        Ok(_) => panic!("uninit payload bytes without encoding must be rejected"),
        Err(error) => assert_eq!(error.get_code(), UCode::INVALID_ARGUMENT),
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn zero_copy_transport_preserves_present_empty_payload()
-> Result<(), Box<dyn std::error::Error>> {
    let _guard = iceoryx2_test_guard().await;
    let authority = format!("iox-present-empty-test-{}", std::process::id());
    let topic = UUri::try_from_parts(&authority, 0x4210, 1, 0x9023)?;
    let subscriber = UTransportIceoryx2::build(MessagingPattern::PublishSubscribe)?;
    let publisher = UTransportIceoryx2::build(MessagingPattern::PublishSubscribe)?;

    let _ = subscriber.receive_zero_copy(&topic, None).await;

    let spec = UTxLoanSpec::present_empty_payload(
        UFrameMetadata::publish(topic.clone()).with_encoding(RawBytes::encoding()),
    )?;
    let loan = publisher.loan_tx(spec).await?;
    publisher.send_zero_copy(loan).await?;

    for _ in 0..100 {
        match subscriber.receive_zero_copy(&topic, None).await {
            Ok(rx) => {
                assert!(rx.has_payload());
                assert_eq!(rx.payload_len(), 0);
                assert_eq!(rx.metadata().encoding(), Some(&RawBytes::encoding()));
                return Ok(());
            }
            Err(status) if status.get_code() == UCode::NOT_FOUND => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(status) => return Err(status.into()),
        }
    }

    Err("timed out waiting for a present-empty payload sample".into())
}

#[tokio::test(flavor = "multi_thread")]
async fn zero_copy_transport_preserves_no_payload() -> Result<(), Box<dyn std::error::Error>> {
    let _guard = iceoryx2_test_guard().await;
    let authority = format!("iox-no-payload-test-{}", std::process::id());
    let topic = UUri::try_from_parts(&authority, 0x4210, 1, 0x9024)?;
    let subscriber = UTransportIceoryx2::build(MessagingPattern::PublishSubscribe)?;
    let publisher = UTransportIceoryx2::build(MessagingPattern::PublishSubscribe)?;

    let _ = subscriber.receive_zero_copy(&topic, None).await;

    let spec = UTxLoanSpec::no_payload(UFrameMetadata::publish(topic.clone()))?;
    let loan = publisher.loan_tx(spec).await?;
    publisher.send_zero_copy(loan).await?;

    for _ in 0..100 {
        match subscriber.receive_zero_copy(&topic, None).await {
            Ok(rx) => {
                assert!(!rx.has_payload());
                assert_eq!(rx.payload_len(), 0);
                assert!(rx.metadata().encoding().is_none());
                return Ok(());
            }
            Err(status) if status.get_code() == UCode::NOT_FOUND => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(status) => return Err(status.into()),
        }
    }

    Err("timed out waiting for a no-payload sample".into())
}

#[tokio::test(flavor = "multi_thread")]
async fn static_allocation_round_trips_stable_container_uninit_payload()
-> Result<(), Box<dyn std::error::Error>> {
    let _guard = iceoryx2_test_guard().await;
    let authority = format!("iox-static-stable-test-{}", std::process::id());
    let topic = UUri::try_from_parts(&authority, 0x4210, 1, 0x9018)?;
    let subscriber = UTransportIceoryx2::build(MessagingPattern::PublishSubscribe)?;
    let publisher = UTransportIceoryx2::build_with_config(
        MessagingPattern::PublishSubscribe,
        Iceoryx2PubSubConfig::static_allocation(512),
    )?;

    let _ = subscriber.receive_zero_copy(&topic, None).await;

    let expected = VehiclePose { x: 377, y: 610 };
    let send_topic = topic.clone();
    let sender = tokio::spawn(async move {
        for _ in 0..50 {
            publisher
                .send_uninit_loaned_payload_as::<StableContainerPayload<VehiclePose>, VehiclePose>(
                    UFrameMetadata::publish(send_topic.clone()),
                    |slot| Ok(slot.write(expected)),
                )
                .await?;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Ok::<(), up_rust::UStatus>(())
    });

    for _ in 0..100 {
        match subscriber.receive_zero_copy(&topic, None).await {
            Ok(rx) => {
                assert_eq!(
                    rx.payload_loan_provenance()?,
                    PayloadLoanProvenance::SharedMemory
                );
                let pose = zero_copy_conformance::borrow_stable_payload::<VehiclePose>(&rx)?;
                assert_eq!(pose, &expected);
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
    Err("timed out waiting for a static stable-container zero-copy sample".into())
}

#[tokio::test(flavor = "multi_thread")]
async fn static_allocation_honors_high_alignment_padding_without_growth()
-> Result<(), Box<dyn std::error::Error>> {
    let _guard = iceoryx2_test_guard().await;
    let authority = format!("iox-static-align-test-{}", std::process::id());
    let topic = UUri::try_from_parts(&authority, 0x4210, 1, 0x9019)?;
    let subscriber = UTransportIceoryx2::build(MessagingPattern::PublishSubscribe)?;
    let publisher = UTransportIceoryx2::build_with_config(
        MessagingPattern::PublishSubscribe,
        Iceoryx2PubSubConfig::static_allocation(512),
    )?;

    let _ = subscriber.receive_zero_copy(&topic, None).await;

    let reading = TestReading {
        sensor_id: 19,
        counter: 512,
    };
    let layout = PayloadLayout::new(
        <TestReading as USerializer<AlignedTestReadingWire>>::encoded_len(&reading),
        <TestReading as USerializer<AlignedTestReadingWire>>::ALIGNMENT,
    )?;
    let spec = UTxLoanSpec::payload(
        UFrameMetadata::publish(topic.clone()).with_encoding(AlignedTestReadingWire::encoding()),
        layout,
    )?;
    let mut loan = publisher.loan_tx(spec).await?;
    assert_eq!(
        loan.payload_mut().as_ptr() as usize
            % <TestReading as USerializer<AlignedTestReadingWire>>::ALIGNMENT,
        0
    );
    <TestReading as USerializer<AlignedTestReadingWire>>::serialize_into(
        &reading,
        loan.payload_mut(),
    )?;
    publisher.send_zero_copy(loan).await?;

    for _ in 0..100 {
        match subscriber.receive_zero_copy(&topic, None).await {
            Ok(rx) => {
                assert_eq!(
                    rx.payload_loan_provenance()?,
                    PayloadLoanProvenance::SharedMemory
                );
                assert_eq!(
                    rx.contiguous_payload().as_ptr() as usize
                        % <TestReading as USerializer<AlignedTestReadingWire>>::ALIGNMENT,
                    0
                );
                assert_eq!(rx.contiguous_payload(), &[0, 19, 0, 0, 2, 0]);
                return Ok(());
            }
            Err(status) if status.get_code() == UCode::NOT_FOUND => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(status) => return Err(status.into()),
        }
    }

    Err("timed out waiting for a static aligned zero-copy sample".into())
}

#[tokio::test(flavor = "multi_thread")]
async fn static_allocation_rejects_metadata_heavy_frame_without_growth()
-> Result<(), Box<dyn std::error::Error>> {
    let _guard = iceoryx2_test_guard().await;
    let authority = format!("iox-static-metadata-cap-test-{}", std::process::id());
    let topic = UUri::try_from_parts(&authority, 0x4210, 1, 0x9020)?;
    let publisher = UTransportIceoryx2::build_with_config(
        MessagingPattern::PublishSubscribe,
        Iceoryx2PubSubConfig::static_allocation(1),
    )?;
    let attributes = UAttributes::new(UUID::build(), topic, None, UMessageType::Publish)
        .with_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00");
    let metadata = UFrameMetadata::without_payload_encoding(attributes);

    let result = publisher.loan_tx(UTxLoanSpec::no_payload(metadata)?).await;
    let Err(error) = result else {
        panic!("static iceoryx2 allocation should reject metadata-only frames over capacity");
    };

    assert_eq!(error.get_code(), UCode::RESOURCE_EXHAUSTED);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn zero_copy_transport_rejects_stable_container_wrong_type_name_metadata()
-> Result<(), Box<dyn std::error::Error>> {
    let _guard = iceoryx2_test_guard().await;
    let authority = format!("iox-stable-negative-test-{}", std::process::id());
    let topic = UUri::try_from_parts(&authority, 0x4210, 1, 0x9008)?;
    let subscriber = UTransportIceoryx2::build(MessagingPattern::PublishSubscribe)?;
    let publisher = UTransportIceoryx2::build(MessagingPattern::PublishSubscribe)?;

    let _ = subscriber.receive_zero_copy(&topic, None).await;

    let pose = VehiclePose { x: 55, y: 89 };
    let encoding = zero_copy_conformance::stable_container_encoding_for::<VehiclePose>(
        "example.vehicle.OtherPose",
        "fixed",
        mem::size_of::<VehiclePose>(),
        mem::align_of::<VehiclePose>(),
    );
    let spec = UTxLoanSpec::payload(
        UFrameMetadata::publish(topic.clone()).with_encoding(encoding),
        PayloadLayout::new(
            mem::size_of::<VehiclePose>(),
            mem::align_of::<VehiclePose>(),
        )?,
    )?;
    let mut loan = publisher.loan_tx(spec).await?;
    loan.payload_mut().copy_from_slice(bytes_of_pose(&pose));
    publisher.send_zero_copy(loan).await?;

    for _ in 0..100 {
        match subscriber.receive_zero_copy(&topic, None).await {
            Ok(rx) => {
                let error =
                    zero_copy_conformance::borrow_stable_payload::<VehiclePose>(&rx).unwrap_err();
                assert!(matches!(
                    error,
                    UWireError::IncompatibleStablePayload { actual, .. }
                        if actual.contains("OtherPose")
                ));
                return Ok(());
            }
            Err(status) if status.get_code() == UCode::NOT_FOUND => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(status) => return Err(status.into()),
        }
    }

    Err("timed out waiting for a malformed stable-container zero-copy sample".into())
}

#[tokio::test(flavor = "multi_thread")]
async fn zero_copy_loan_tx_honors_payload_alignment() -> Result<(), Box<dyn std::error::Error>> {
    let _guard = iceoryx2_test_guard().await;
    let authority = format!("iox-align-test-{}", std::process::id());
    let topic = UUri::try_from_parts(&authority, 0x4210, 1, 0x9010)?;
    let subscriber = UTransportIceoryx2::build(MessagingPattern::PublishSubscribe)?;
    let publisher = UTransportIceoryx2::build(MessagingPattern::PublishSubscribe)?;

    let _ = subscriber.receive_zero_copy(&topic, None).await;

    let reading = TestReading {
        sensor_id: 10,
        counter: 64,
    };
    let layout = PayloadLayout::new(
        <TestReading as USerializer<AlignedTestReadingWire>>::encoded_len(&reading),
        <TestReading as USerializer<AlignedTestReadingWire>>::ALIGNMENT,
    )?;
    let spec = UTxLoanSpec::payload(
        UFrameMetadata::publish(topic.clone()).with_encoding(AlignedTestReadingWire::encoding()),
        layout,
    )?;
    let mut loan = publisher.loan_tx(spec).await?;
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
    let _guard = iceoryx2_test_guard().await;
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
    let _guard = iceoryx2_test_guard().await;
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
            Some(sink_b.clone()),
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

    let mut sink_a_rejected = false;
    for _ in 0..100 {
        match subscriber.receive_zero_copy(&source, Some(&sink_a)).await {
            Err(status) if status.get_code() == UCode::NOT_FOUND => {
                sink_a_rejected = true;
                break;
            }
            Err(status) => return Err(status.into()),
            Ok(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
    if !sink_a_rejected {
        return Err("sink A receive delivered a sink B sample".into());
    }

    for _ in 0..100 {
        match subscriber.receive_zero_copy(&source, Some(&sink_b)).await {
            Ok(rx) => {
                assert_eq!(rx.metadata().attributes().sink(), Some(&sink_b));
                return Ok(());
            }
            Err(status) if status.get_code() == UCode::NOT_FOUND => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(status) => return Err(status.into()),
        }
    }

    Err("sink B sample was not preserved after mismatched sink A receive".into())
}

#[tokio::test(flavor = "multi_thread")]
async fn zero_copy_listener_round_trips_custom_payload_codec()
-> Result<(), Box<dyn std::error::Error>> {
    let _guard = iceoryx2_test_guard().await;
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
    let _guard = iceoryx2_test_guard().await;
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
    let _guard = iceoryx2_test_guard().await;
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
    let _guard = iceoryx2_test_guard().await;
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
    let _guard = iceoryx2_test_guard().await;
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
    let _guard = iceoryx2_test_guard().await;
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

#[tokio::test(flavor = "multi_thread")]
async fn zero_copy_publish_fanout_delivers_to_exact_and_source_wildcard_listeners()
-> Result<(), Box<dyn std::error::Error>> {
    let _guard = iceoryx2_test_guard().await;
    let authority = format!("iox-source-wildcard-fanout-{}", std::process::id());
    let topic = UUri::try_from_parts(&authority, 0x3456_4210, 1, 0x9016)?;
    let source_wildcard = UUri::try_from_parts(&authority, 0xFFFF_4210, 1, 0x9016)?;
    let subscriber = UTransportIceoryx2::build(MessagingPattern::PublishSubscribe)?;
    let publisher = UTransportIceoryx2::build(MessagingPattern::PublishSubscribe)?;
    let (exact_tx, mut exact_rx) = mpsc::unbounded_channel();
    let (wildcard_tx, mut wildcard_rx) = mpsc::unbounded_channel();
    let exact_listener: Arc<dyn UZeroCopyListener<Iceoryx2RxLease>> =
        Arc::new(LeaseSender(exact_tx));
    let wildcard_listener: Arc<dyn UZeroCopyListener<Iceoryx2RxLease>> =
        Arc::new(LeaseSender(wildcard_tx));

    subscriber
        .register_zero_copy_listener(&topic, None, exact_listener)
        .await?;
    subscriber
        .register_zero_copy_listener(&source_wildcard, None, wildcard_listener)
        .await?;

    let reading = TestReading {
        sensor_id: 16,
        counter: 256,
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

    let (_, exact_decoded) = tokio::time::timeout(Duration::from_secs(5), exact_rx.recv())
        .await?
        .expect("exact listener result channel closed");
    let (_, wildcard_decoded) = tokio::time::timeout(Duration::from_secs(5), wildcard_rx.recv())
        .await?
        .expect("wildcard listener result channel closed");
    sender.abort();

    assert_eq!(exact_decoded, reading);
    assert_eq!(wildcard_decoded, reading);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn zero_copy_targeted_fanout_delivers_to_exact_and_sink_wildcard_listeners()
-> Result<(), Box<dyn std::error::Error>> {
    let _guard = iceoryx2_test_guard().await;
    let authority = format!("iox-sink-wildcard-fanout-{}", std::process::id());
    let source = UUri::try_from_parts(&authority, 0x4210, 1, 0x9017)?;
    let sink = UUri::try_from_parts(&authority, 0x4220, 1, 0)?;
    let sink_wildcard = UUri::try_from_parts(&authority, 0xFFFF_FFFF, 0xFF, 0)?;
    let subscriber = UTransportIceoryx2::build(MessagingPattern::PublishSubscribe)?;
    let publisher = UTransportIceoryx2::build(MessagingPattern::PublishSubscribe)?;
    let (exact_tx, mut exact_rx) = mpsc::unbounded_channel();
    let (wildcard_tx, mut wildcard_rx) = mpsc::unbounded_channel();
    let exact_listener: Arc<dyn UZeroCopyListener<Iceoryx2RxLease>> =
        Arc::new(LeaseSender(exact_tx));
    let wildcard_listener: Arc<dyn UZeroCopyListener<Iceoryx2RxLease>> =
        Arc::new(LeaseSender(wildcard_tx));

    subscriber
        .register_zero_copy_listener(&source, Some(&sink), exact_listener)
        .await?;
    subscriber
        .register_zero_copy_listener(&source, Some(&sink_wildcard), wildcard_listener)
        .await?;

    let reading = TestReading {
        sensor_id: 17,
        counter: 289,
    };
    let send_source = source.clone();
    let send_sink = sink.clone();
    let send_reading = reading.clone();
    let sender = tokio::spawn(async move {
        for _ in 0..50 {
            let header = UFrameMetadata::new(
                UAttributes::new(
                    UUID::build(),
                    send_source.clone(),
                    Some(send_sink.clone()),
                    UMessageType::Notification,
                ),
                TestReadingWire::encoding(),
            );
            publisher
                .send_serialized_zero_copy::<TestReadingWire, _>(header, &send_reading)
                .await?;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Ok::<(), up_rust::UStatus>(())
    });

    let (_, exact_decoded) = tokio::time::timeout(Duration::from_secs(5), exact_rx.recv())
        .await?
        .expect("exact listener result channel closed");
    let (_, wildcard_decoded) = tokio::time::timeout(Duration::from_secs(5), wildcard_rx.recv())
        .await?
        .expect("wildcard listener result channel closed");
    sender.abort();

    assert_eq!(exact_decoded, reading);
    assert_eq!(wildcard_decoded, reading);
    Ok(())
}
