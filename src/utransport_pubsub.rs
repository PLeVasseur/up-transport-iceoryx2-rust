// // ################################################################################
// // Copyright (c) 2025 Contributors to the Eclipse Foundation
// //
// // See the NOTICE file(s) distributed with this work for additional
// // information regarding copyright ownership.
// //
// // This program and the accompanying materials are made available under the
// // terms of the Apache License Version 2.0 which is available at
// // https: //www.apache.org/licenses/LICENSE-2.0
// //
// // SPDX-License-Identifier: Apache-2.0
// // ################################################################################

use async_trait::async_trait;
use iceoryx2::port::LoanError;
use iceoryx2::prelude::{AllocationStrategy, MessagingPattern};
use iceoryx2::sample_mut::SampleMut;
use iceoryx2::sample_mut_uninit::SampleMutUninit;
use iceoryx2::{
    node::{Node, NodeBuilder},
    port::{publisher::Publisher, subscriber::Subscriber},
    prelude::ServiceName,
    service::ipc_threadsafe,
};
use iceoryx2_bb_container::vec::FixedSizeVec;
use std::{collections::HashMap, mem::MaybeUninit, sync::Arc};
use tokio::sync::RwLock;
use up_rust::{
    ComparableListener, ProtobufMappable, UCode, UFrameMetadata, UListener, UMessage, UStatus,
    UTransport, UTxBuffer, UUninitTxBuffer, UUri, UVecRxLease, UZeroCopyTransportImpl,
    UZeroCopyUninitTransportImpl, ValidatedTxLoanSpec,
};

use crate::UPROTOCOL_MAJOR_VERSION;
use crate::uprotocolheader::MAX_FEASIBLE_UATTRIBUTES_SERIALIZED_LENGTH;
use crate::workers::dispatcher::Iceoryx2WorkerDispatcher;
use crate::{
    ListenerMap, PublisherSet, SubscriberSet,
    service_name_mapping::compute_service_name,
    uprotocolheader::{UProtocolHeader, encode_frame_metadata, validate_metadata_prefix},
};

type IpcSampleMut = SampleMut<ipc_threadsafe::Service, [u8], UProtocolHeader>;
type IpcSampleMutUninit =
    SampleMutUninit<ipc_threadsafe::Service, [MaybeUninit<u8>], UProtocolHeader>;

#[derive(Debug)]
pub struct Iceoryx2PubSub {
    node: Node<ipc_threadsafe::Service>,
    pub publishers: PublisherSet<ipc_threadsafe::Service>,
    pub subscribers: SubscriberSet<ipc_threadsafe::Service>,
    pub listeners: ListenerMap,
}

impl Iceoryx2PubSub {
    pub fn new() -> Arc<Self> {
        let node = NodeBuilder::new()
            .create::<ipc_threadsafe::Service>()
            .expect("Failed to create Iceoryx2 Node");
        let transport = Arc::new(Self {
            node,
            publishers: RwLock::new(HashMap::new()),
            subscribers: RwLock::new(HashMap::new()),
            listeners: RwLock::new(HashMap::new()),
        });
        Iceoryx2WorkerDispatcher::start_listener_worker(transport.clone());
        transport
    }

    pub fn create_subscriber(
        &self,
        service_name: ServiceName,
    ) -> Result<Subscriber<ipc_threadsafe::Service, [u8], UProtocolHeader>, UStatus> {
        let service = self
            .node
            .service_builder(&service_name)
            .publish_subscribe::<[u8]>()
            .user_header::<UProtocolHeader>()
            .open_or_create()
            .map_err(|e| {
                UStatus::fail_with_code(UCode::Internal, format!("Failed to create service: {e}"))
            })?;
        let subscriber = service.subscriber_builder().create().map_err(|e| {
            UStatus::fail_with_code(UCode::Internal, format!("Failed to create subscriber: {e}"))
        })?;
        Ok(subscriber)
    }

    pub async fn get_or_create_publisher(
        &self,
        service_name: ServiceName,
    ) -> Result<Arc<Publisher<ipc_threadsafe::Service, [u8], UProtocolHeader>>, UStatus> {
        let publisher = self.get_publisher(service_name.clone()).await;
        if let Some(publisher) = publisher {
            return Ok(publisher);
        }
        self.create_publisher(service_name).await
    }

    async fn create_publisher(
        &self,
        service_name: ServiceName,
    ) -> Result<Arc<Publisher<ipc_threadsafe::Service, [u8], UProtocolHeader>>, UStatus> {
        let service = self
            .node
            .service_builder(&service_name)
            .publish_subscribe::<[u8]>()
            .user_header::<UProtocolHeader>()
            .open_or_create()
            .map_err(|e| {
                UStatus::fail_with_code(UCode::Internal, format!("Failed to create service: {e}"))
            })?;

        let publisher = service
            .publisher_builder()
            .allocation_strategy(AllocationStrategy::PowerOfTwo)
            .create()
            .map_err(|e| {
                UStatus::fail_with_code(UCode::Internal, format!("Failed to create publisher: {e}"))
            })?;
        let mut publishers = self.publishers.write().await;
        publishers.insert(service_name.clone(), Arc::new(publisher));
        let publisher = publishers.get(&service_name).unwrap();
        Ok(publisher.clone())
    }

    async fn get_publisher(
        &self,
        service_name: ServiceName,
    ) -> Option<Arc<Publisher<ipc_threadsafe::Service, [u8], UProtocolHeader>>> {
        let publishers = self.publishers.read().await;
        if publishers.contains_key(&service_name) {
            let publisher = publishers.get(&service_name).unwrap();
            return Some(publisher.clone());
        }
        None
    }

    pub async fn relay(&self) -> Result<(), UStatus> {
        let subscribers = self.subscribers.read().await;
        for (service_name, subscriber) in subscribers.iter() {
            match subscriber.receive() {
                Ok(Some(sample)) => {
                    let sample_payload = sample.payload();
                    let layout = sample
                        .user_header()
                        .payload_layout(sample_payload.len())
                        .map_err(frame_contract_error_to_status)?;
                    if layout.metadata_len() > 0 {
                        validate_metadata_prefix(
                            layout
                                .metadata_prefix(sample_payload)
                                .map_err(frame_contract_error_to_status)?,
                        )
                        .map_err(frame_contract_error_to_status)?;
                    }
                    let payload = layout
                        .payload(sample_payload)
                        .map_err(frame_contract_error_to_status)?;
                    let umessage = UMessage::parse_from_protobuf_bytes(payload).map_err(|e| {
                        UStatus::fail_with_code(
                            UCode::Internal,
                            format!("Failed to deserialize UMessage: {}", e),
                        )
                    })?;
                    if let Some(listeners_to_notify) = self.listeners.read().await.get(service_name)
                    {
                        for listener in listeners_to_notify.iter() {
                            let listener: &ComparableListener = listener;
                            let payload_clone = umessage.clone();
                            listener.on_receive(payload_clone).await;
                        }
                    }
                }
                Ok(None) => continue, // No sample available
                Err(e) => {
                    return Err(UStatus::fail_with_code(
                        UCode::Internal,
                        format!("Failed to receive sample: {e}"),
                    ));
                }
            }
        }
        Ok(())
    }

    pub fn write_message_to_sample(
        &self,
        publisher: &Publisher<ipc_threadsafe::Service, [u8], UProtocolHeader>,
        message: UMessage,
    ) -> Result<SampleMut<ipc_threadsafe::Service, [u8], UProtocolHeader>, UStatus> {
        let message_bytes = message
            .write_to_protobuf_bytes()
            .map_err(|e| UStatus::fail_with_code(UCode::Internal, e.to_string()))?;
        let mut sample = publisher
            .loan_slice_uninit(message_bytes.len())
            .map_err(|e| {
                UStatus::fail_with_code(UCode::Internal, format!("Failed to loan sample: {e}"))
            })?;
        let serialized_data = message_bytes.as_slice();
        let user_header: &mut UProtocolHeader = sample.user_header_mut();
        self.set_samples_user_header(user_header, &message, serialized_data.len())?;
        let sample_final = sample.write_from_slice(serialized_data);
        Ok(sample_final)
    }

    fn set_samples_user_header(
        &self,
        user_header: &mut UProtocolHeader,
        message: &UMessage,
        payload_len: usize,
    ) -> Result<(), UStatus> {
        user_header.uprotocol_major_version = UPROTOCOL_MAJOR_VERSION;
        let serialized_uattributes = message
            .attributes()
            .write_to_protobuf_bytes()
            .map_err(|e| UStatus::fail_with_code(UCode::Internal, e.to_string()))?;
        let mut fixed_sized_vec: FixedSizeVec<u8, MAX_FEASIBLE_UATTRIBUTES_SERIALIZED_LENGTH> =
            FixedSizeVec::new();
        for byte in serialized_uattributes.iter() {
            fixed_sized_vec.push(*byte);
        }
        user_header.uattributes_serialized = fixed_sized_vec;
        user_header
            .write_payload_layout(0, payload_len, 1)
            .map_err(frame_contract_error_to_status)?;
        Ok(())
    }
}

/// iceoryx2 transmit loan for one native uProtocol frame.
pub struct Iceoryx2TxLoan {
    metadata: UFrameMetadata,
    sample: IpcSampleMut,
    payload_offset: usize,
    payload_len: usize,
}

/// iceoryx2 transmit loan whose application payload bytes are not initialized yet.
pub struct Iceoryx2UninitTxLoan {
    metadata: UFrameMetadata,
    sample: IpcSampleMutUninit,
    payload_offset: usize,
    payload_len: usize,
}

impl UTxBuffer for Iceoryx2TxLoan {
    fn metadata(&self) -> &UFrameMetadata {
        &self.metadata
    }

    fn payload(&self) -> &[u8] {
        let end = self
            .payload_offset
            .checked_add(self.payload_len)
            .expect("loaned payload layout overflow");
        self.sample
            .payload()
            .get(self.payload_offset..end)
            .expect("loaned payload layout should be valid")
    }

    fn payload_mut(&mut self) -> &mut [u8] {
        let end = self
            .payload_offset
            .checked_add(self.payload_len)
            .expect("loaned payload layout overflow");
        self.sample
            .payload_mut()
            .get_mut(self.payload_offset..end)
            .expect("loaned payload layout should be valid")
    }
}

impl UUninitTxBuffer for Iceoryx2UninitTxLoan {
    type Initialized = Iceoryx2TxLoan;

    fn metadata(&self) -> &UFrameMetadata {
        &self.metadata
    }

    fn payload_len(&self) -> usize {
        self.payload_len
    }

    fn payload_uninit_mut(&mut self) -> &mut [MaybeUninit<u8>] {
        let end = self
            .payload_offset
            .checked_add(self.payload_len)
            .expect("loaned payload layout overflow");
        self.sample
            .payload_mut()
            .get_mut(self.payload_offset..end)
            .expect("loaned payload layout should be valid")
    }

    unsafe fn assume_payload_init(self) -> Self::Initialized {
        let mut sample = self.sample;
        let payload_end = self
            .payload_offset
            .checked_add(self.payload_len)
            .expect("loaned payload layout overflow");
        let trailing = sample
            .payload_mut()
            .get_mut(payload_end..)
            .expect("loaned trailing padding range should be valid");
        for byte in trailing {
            byte.write(0);
        }

        Iceoryx2TxLoan {
            metadata: self.metadata,
            // SAFETY: the caller guarantees the visible payload bytes were initialized;
            // this method initializes the remaining sample bytes before committing it.
            sample: unsafe { sample.assume_init() },
            payload_offset: self.payload_offset,
            payload_len: self.payload_len,
        }
    }
}

#[async_trait]
impl UZeroCopyTransportImpl for Iceoryx2PubSub {
    type Tx = Iceoryx2TxLoan;
    type Rx = UVecRxLease;

    async fn loan_validated_tx(&self, spec: ValidatedTxLoanSpec) -> Result<Self::Tx, UStatus> {
        let metadata = spec.metadata().clone();
        let payload_len = spec.payload_len();
        let alignment = spec.payload_alignment();
        validate_alignment(alignment)?;
        let source = metadata.attributes().source();
        let service_name = compute_service_name(
            source,
            metadata.attributes().sink(),
            MessagingPattern::PublishSubscribe,
        )?;
        let metadata_prefix = encode_frame_metadata(&metadata)?;
        let metadata_len = metadata_prefix.len();
        let publisher = self.get_or_create_publisher(service_name).await?;
        let sample_len = worst_case_aligned_sample_len(metadata_len, payload_len, alignment)?;
        let mut sample = publisher
            .loan_slice(sample_len)
            .map_err(|e| map_loan_error(e, "loan sample"))?;
        let payload_offset =
            aligned_payload_offset(sample.payload().as_ptr() as usize, metadata_len, alignment)?;
        let aligned_sample_len = payload_offset.checked_add(payload_len).ok_or_else(|| {
            UStatus::fail_with_code(UCode::InvalidArgument, "sample length overflow")
        })?;
        if aligned_sample_len > sample.payload().len() {
            return Err(UStatus::fail_with_code(
                UCode::Internal,
                "reserved sample is too small for aligned payload layout",
            ));
        }
        sample
            .payload_mut()
            .get_mut(..metadata_len)
            .ok_or_else(|| {
                UStatus::fail_with_code(UCode::Internal, "failed to access metadata prefix")
            })?
            .copy_from_slice(&metadata_prefix);
        let sample_payload_len = sample.payload().len();
        write_frame_user_header(
            sample.user_header_mut(),
            &metadata,
            metadata_len,
            payload_offset,
            payload_len,
            alignment,
            sample_payload_len,
        )?;
        Ok(Iceoryx2TxLoan {
            metadata,
            sample,
            payload_offset,
            payload_len,
        })
    }

    async fn send_validated_zero_copy(&self, buffer: Self::Tx) -> Result<(), UStatus> {
        buffer.sample.send().map_err(|e| {
            UStatus::fail_with_code(UCode::Internal, format!("Failed to send: {e}"))
        })?;
        Ok(())
    }
}

#[async_trait]
impl UZeroCopyUninitTransportImpl for Iceoryx2PubSub {
    type UninitTx = Iceoryx2UninitTxLoan;

    async fn loan_validated_uninit_tx(
        &self,
        spec: ValidatedTxLoanSpec,
    ) -> Result<Self::UninitTx, UStatus> {
        let metadata = spec.metadata().clone();
        let payload_len = spec.payload_len();
        let alignment = spec.payload_alignment();
        validate_alignment(alignment)?;
        let source = metadata.attributes().source();
        let service_name = compute_service_name(
            source,
            metadata.attributes().sink(),
            MessagingPattern::PublishSubscribe,
        )?;
        let metadata_prefix = encode_frame_metadata(&metadata)?;
        let metadata_len = metadata_prefix.len();
        let publisher = self.get_or_create_publisher(service_name).await?;
        let sample_len = worst_case_aligned_sample_len(metadata_len, payload_len, alignment)?;
        let mut sample = publisher
            .loan_slice_uninit(sample_len)
            .map_err(|e| map_loan_error(e, "loan uninitialized sample"))?;
        let payload_offset = aligned_payload_offset(
            sample.payload_mut().as_ptr() as usize,
            metadata_len,
            alignment,
        )?;
        let aligned_sample_len = payload_offset.checked_add(payload_len).ok_or_else(|| {
            UStatus::fail_with_code(UCode::InvalidArgument, "sample length overflow")
        })?;
        if aligned_sample_len > sample.payload_mut().len() {
            return Err(UStatus::fail_with_code(
                UCode::Internal,
                "reserved uninitialized sample is too small for aligned payload layout",
            ));
        }
        write_uninit_bytes(sample.payload_mut(), 0, &metadata_prefix)?;
        initialize_uninit_range(sample.payload_mut(), metadata_len, payload_offset)?;
        let sample_payload_len = sample.payload_mut().len();
        write_frame_user_header(
            sample.user_header_mut(),
            &metadata,
            metadata_len,
            payload_offset,
            payload_len,
            alignment,
            sample_payload_len,
        )?;
        Ok(Iceoryx2UninitTxLoan {
            metadata,
            sample,
            payload_offset,
            payload_len,
        })
    }
}

fn write_frame_user_header(
    user_header: &mut UProtocolHeader,
    metadata: &UFrameMetadata,
    metadata_len: usize,
    payload_offset: usize,
    payload_len: usize,
    payload_alignment: usize,
    sample_payload_len: usize,
) -> Result<(), UStatus> {
    user_header.uprotocol_major_version = UPROTOCOL_MAJOR_VERSION;
    let serialized_uattributes = metadata
        .attributes()
        .write_to_protobuf_bytes()
        .map_err(|e| UStatus::fail_with_code(UCode::Internal, e.to_string()))?;
    let mut fixed_sized_vec: FixedSizeVec<u8, MAX_FEASIBLE_UATTRIBUTES_SERIALIZED_LENGTH> =
        FixedSizeVec::new();
    for byte in serialized_uattributes.iter() {
        fixed_sized_vec.push(*byte);
    }
    user_header.uattributes_serialized = fixed_sized_vec;
    user_header
        .write_payload_layout_at_offset(
            metadata_len,
            payload_offset,
            payload_len,
            payload_alignment,
            sample_payload_len,
        )
        .map_err(frame_contract_error_to_status)?;
    Ok(())
}

fn validate_alignment(alignment: usize) -> Result<(), UStatus> {
    if alignment == 0 || !alignment.is_power_of_two() {
        return Err(UStatus::fail_with_code(
            UCode::InvalidArgument,
            "payload alignment must be a non-zero power of two",
        ));
    }
    Ok(())
}

fn map_loan_error(error: LoanError, operation: &str) -> UStatus {
    let code = match error {
        LoanError::OutOfMemory | LoanError::ExceedsMaxLoans | LoanError::ExceedsMaxLoanSize => {
            UCode::ResourceExhausted
        }
        LoanError::InternalFailure => UCode::Internal,
    };
    UStatus::fail_with_code(code, format!("Failed to {operation}: {error}"))
}

fn write_uninit_bytes(
    sample: &mut [MaybeUninit<u8>],
    offset: usize,
    bytes: &[u8],
) -> Result<(), UStatus> {
    let end = offset.checked_add(bytes.len()).ok_or_else(|| {
        UStatus::fail_with_code(UCode::InvalidArgument, "sample initialization overflow")
    })?;
    let dst = sample.get_mut(offset..end).ok_or_else(|| {
        UStatus::fail_with_code(
            UCode::Internal,
            "failed to access uninitialized sample range",
        )
    })?;
    for (dst, src) in dst.iter_mut().zip(bytes) {
        dst.write(*src);
    }
    Ok(())
}

fn initialize_uninit_range(
    sample: &mut [MaybeUninit<u8>],
    start: usize,
    end: usize,
) -> Result<(), UStatus> {
    let dst = sample.get_mut(start..end).ok_or_else(|| {
        UStatus::fail_with_code(
            UCode::Internal,
            "failed to access uninitialized sample range",
        )
    })?;
    for byte in dst {
        byte.write(0);
    }
    Ok(())
}

fn worst_case_aligned_sample_len(
    metadata_len: usize,
    payload_len: usize,
    alignment: usize,
) -> Result<usize, UStatus> {
    metadata_len
        .checked_add(alignment - 1)
        .and_then(|len| len.checked_add(payload_len))
        .ok_or_else(|| UStatus::fail_with_code(UCode::InvalidArgument, "sample length overflow"))
}

fn aligned_payload_offset(
    payload_base: usize,
    metadata_len: usize,
    alignment: usize,
) -> Result<usize, UStatus> {
    let payload_start = payload_base.checked_add(metadata_len).ok_or_else(|| {
        UStatus::fail_with_code(UCode::InvalidArgument, "payload address overflow")
    })?;
    let padding = (alignment - (payload_start & (alignment - 1))) & (alignment - 1);
    metadata_len
        .checked_add(padding)
        .ok_or_else(|| UStatus::fail_with_code(UCode::InvalidArgument, "payload offset overflow"))
}

#[async_trait]
impl UTransport for Iceoryx2PubSub {
    /// ## DISCLAIMER
    ///
    /// This code is a prototype to make UMessage work with iceoryx2's ZeroCopySend system
    ///
    /// UMessage is not ZeroCopySend compatible as-is. If UMessages are sent
    /// directly to an iceoryx2 publisher, it will compile. However, the
    /// subscriber will receive a segmentation fault when receiving theUMessage
    ///
    /// See [ZeroCopySend's safety requirements](https://docs.rs/iceoryx2/latest/iceoryx2/prelude/trait.ZeroCopySend.html#safety) for more details.
    ///
    /// This essentially defeats the purpose of using iceoryx2 and
    /// ZeroCopySend, as it copies the data into a fixed-size array and then out
    /// of the array and back into a UMessage inside the `UTransport.send()` method
    /// and the `UListener.on_receive()` method. The UTransport or UMessage
    /// definition needs to be adjusted for this to truly be a zero-copy transport.
    async fn send(&self, message: UMessage) -> Result<(), UStatus> {
        let service_name = {
            let source_filter = message.source();
            let sink_filter = message.sink();
            compute_service_name(
                source_filter,
                sink_filter,
                MessagingPattern::PublishSubscribe,
            )?
        };
        let publisher = self
            .get_or_create_publisher(service_name)
            .await
            .map_err(|e| {
                UStatus::fail_with_code(UCode::Internal, format!("Failed to get publisher: {e}"))
            })?;
        let sample_final = self.write_message_to_sample(publisher.as_ref(), message)?;
        sample_final.send().map_err(|e| {
            UStatus::fail_with_code(UCode::Internal, format!("Failed to send: {e}"))
        })?;
        Ok(())
    }

    async fn register_listener(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UListener>,
    ) -> Result<(), UStatus> {
        up_rust::verify_filter_criteria(source_filter, sink_filter).map_err(|err| *err)?;
        let service_name = compute_service_name(
            source_filter,
            sink_filter,
            MessagingPattern::PublishSubscribe,
        )?;
        let has_subscriber = {
            let subscribers = self.subscribers.read().await;
            subscribers.contains_key(&service_name)
        };
        // insert subscriber for service name if it does not already exist
        if !has_subscriber {
            let subscriber = self.create_subscriber(service_name.clone())?;
            let mut subscribers = self.subscribers.write().await;
            subscribers.insert(service_name.clone(), Arc::new(subscriber));
        }
        // insert listener for service name if it does not already exist
        if !self.listeners.read().await.contains_key(&service_name) {
            let mut listeners = self.listeners.write().await;
            listeners
                .entry(service_name)
                .or_default()
                .insert(ComparableListener::new(listener));
        }
        Ok(())
    }

    async fn unregister_listener(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UListener>,
    ) -> Result<(), UStatus> {
        up_rust::verify_filter_criteria(source_filter, sink_filter).map_err(|err| *err)?;
        let service_name = compute_service_name(
            source_filter,
            sink_filter,
            MessagingPattern::PublishSubscribe,
        )?;
        let comparable_listener = ComparableListener::new(listener.clone());
        let mut listeners = self.listeners.write().await;
        if let Some(existing_listeners) = listeners.get_mut(&service_name) {
            existing_listeners.retain(|l| !l.eq(&comparable_listener));
            if existing_listeners.is_empty() {
                let mut subscribers = self.subscribers.write().await;
                subscribers.remove(&service_name);
            }
        }
        Ok(())
    }
}

fn frame_contract_error_to_status(error: impl std::fmt::Display) -> UStatus {
    UStatus::fail_with_code(UCode::InvalidArgument, error.to_string())
}

#[cfg(test)]
mod zero_copy_tx_tests {
    use super::*;
    use tokio::sync::{Mutex, MutexGuard};
    use up_rust::{
        UMessageBuilder, UPayloadFormat, UTxLoanSpec, UZeroCopyTransport, UZeroCopyUninitTransport,
        try_project_umessage_to_frame_metadata,
    };

    static ICEORYX2_TEST_MUTEX: Mutex<()> = Mutex::const_new(());

    async fn iceoryx2_test_guard() -> MutexGuard<'static, ()> {
        ICEORYX2_TEST_MUTEX.lock().await
    }

    fn test_metadata(test_name: &str) -> Result<UFrameMetadata, Box<dyn std::error::Error>> {
        let authority = format!("iox-{test_name}-{}", std::process::id());
        let topic = UUri::try_from_parts(&authority, 0x4210, 1, 0x9002)?;
        let mut builder = UMessageBuilder::publish(topic);
        let message = builder.build_with_payload(Vec::<u8>::new(), UPayloadFormat::Raw)?;
        Ok(try_project_umessage_to_frame_metadata(&message)?)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn initialized_tx_loan_exposes_aligned_payload_range()
    -> Result<(), Box<dyn std::error::Error>> {
        let _guard = iceoryx2_test_guard().await;
        let transport = Iceoryx2PubSub::new();
        let metadata = test_metadata("tx-loan")?;
        let mut loan = transport
            .loan_tx(UTxLoanSpec::payload(metadata.clone(), 32, 64)?)
            .await?;

        assert_eq!(loan.metadata(), &metadata);
        assert_eq!(loan.payload().len(), 32);
        assert_eq!(loan.payload().as_ptr() as usize % 64, 0);
        loan.payload_mut().copy_from_slice(&[0x5a; 32]);

        transport.send_zero_copy(loan).await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn uninit_tx_loan_commits_initialized_payload_without_zero_fill()
    -> Result<(), Box<dyn std::error::Error>> {
        let _guard = iceoryx2_test_guard().await;
        let transport = Iceoryx2PubSub::new();
        let metadata = test_metadata("uninit-tx-loan")?;
        let mut loan = transport
            .loan_uninit_tx(UTxLoanSpec::payload(metadata.clone(), 48, 128)?)
            .await?;

        assert_eq!(loan.metadata(), &metadata);
        assert_eq!(loan.payload_len(), 48);
        let expected: Vec<u8> = (0..48).map(|value| value as u8).collect();
        {
            let payload = loan.payload_uninit_mut();
            assert_eq!(payload.as_ptr() as usize % 128, 0);
            for (slot, value) in payload.iter_mut().zip(&expected) {
                slot.write(*value);
            }
        }

        // SAFETY: every visible payload byte was initialized immediately above.
        let loan = unsafe { loan.assume_payload_init() };
        assert_eq!(loan.payload(), expected.as_slice());

        transport.send_zero_copy(loan).await?;
        Ok(())
    }
}
