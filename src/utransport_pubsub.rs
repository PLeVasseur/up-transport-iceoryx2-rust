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
use iceoryx2::prelude::{AllocationStrategy, CallbackProgression, MessagingPattern, Service};
use iceoryx2::sample::Sample;
use iceoryx2::sample_mut::SampleMut;
use iceoryx2::sample_mut_uninit::SampleMutUninit;
use iceoryx2::{
    node::{Node, NodeBuilder},
    port::{publisher::Publisher, subscriber::Subscriber},
    prelude::ServiceName,
    service::ipc_threadsafe,
};
use std::{
    collections::{HashMap, VecDeque},
    io::Cursor,
    mem::MaybeUninit,
    sync::Arc,
};
use tokio::sync::{Mutex, RwLock};
use up_rust::{
    ComparableListener, LoanedPayload, PayloadLoanProvenance, ProtobufMappable, UCode,
    UFrameMetadata, UFrameView, UListener, ULoanedContiguousZeroCopyRxFrame, UMessage, UStatus,
    UTransport, UTxBuffer, UUninitTxBuffer, UUri, UZeroCopyListener, UZeroCopyRxLease,
    UZeroCopyTransportImpl, UZeroCopyUninitTransportImpl, ValidatedTxLoanSpec,
};

use crate::UPROTOCOL_MAJOR_VERSION;
use crate::workers::dispatcher::Iceoryx2WorkerDispatcher;
use crate::{
    ListenerMap, PublisherSet, SubscriberSet,
    service_attributes::{attributes_match_source_filter, source_attribute_verifier},
    service_name_mapping::compute_service_name,
    uprotocolheader::{UProtocolHeader, encode_frame_metadata, validate_metadata_prefix},
};

type IpcSample = Sample<ipc_threadsafe::Service, [u8], UProtocolHeader>;
type IpcSampleMut = SampleMut<ipc_threadsafe::Service, [u8], UProtocolHeader>;
type IpcSampleMutUninit =
    SampleMutUninit<ipc_threadsafe::Service, [MaybeUninit<u8>], UProtocolHeader>;
type IpcSubscriber = Subscriber<ipc_threadsafe::Service, [u8], UProtocolHeader>;
type ZeroCopyListenerMap = RwLock<Vec<ZeroCopyListenerRegistration>>;

struct ZeroCopyListenerRegistration {
    source_filter: UUri,
    sink_filter: Option<UUri>,
    listener: Arc<dyn UZeroCopyListener<Iceoryx2RxLease>>,
    subscribers: HashMap<ServiceName, Arc<IpcSubscriber>>,
}

impl ZeroCopyListenerRegistration {
    fn new(
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UZeroCopyListener<Iceoryx2RxLease>>,
    ) -> Self {
        Self {
            source_filter: source_filter.clone(),
            sink_filter: sink_filter.cloned(),
            listener,
            subscribers: HashMap::new(),
        }
    }

    fn has_same_identity(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: &Arc<dyn UZeroCopyListener<Iceoryx2RxLease>>,
    ) -> bool {
        self.source_filter == *source_filter
            && self.sink_filter.as_ref() == sink_filter
            && Arc::ptr_eq(&self.listener, listener)
    }
}

pub struct Iceoryx2PubSub {
    node: Node<ipc_threadsafe::Service>,
    config: Iceoryx2PubSubConfig,
    pub publishers: PublisherSet<ipc_threadsafe::Service>,
    pub subscribers: SubscriberSet<ipc_threadsafe::Service>,
    pub listeners: ListenerMap,
    pull_subscribers: SubscriberSet<ipc_threadsafe::Service>,
    pull_receive_queue_state: Mutex<PullReceiveQueueState>,
    zero_copy_listeners: ZeroCopyListenerMap,
}

impl std::fmt::Debug for Iceoryx2PubSub {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Iceoryx2PubSub").finish_non_exhaustive()
    }
}

#[derive(Default)]
struct PullReceiveQueueState {
    queues: HashMap<ServiceName, VecDeque<Iceoryx2RxLease>>,
    dropped_mismatches: u64,
    rejected_mismatches: u64,
    last_mismatch_reason: Option<String>,
}

impl PullReceiveQueueState {
    fn current_depth(&self) -> usize {
        self.queues.values().map(VecDeque::len).sum()
    }

    fn diagnostics(&self) -> PullMismatchQueueDiagnostics {
        PullMismatchQueueDiagnostics {
            current_depth: self.current_depth(),
            dropped_mismatches: self.dropped_mismatches,
            rejected_mismatches: self.rejected_mismatches,
            last_mismatch_reason: self.last_mismatch_reason.clone(),
        }
    }
}

impl Iceoryx2PubSub {
    pub fn new() -> Arc<Self> {
        Self::with_config(Iceoryx2PubSubConfig::default())
    }

    pub fn with_config(config: Iceoryx2PubSubConfig) -> Arc<Self> {
        let node = NodeBuilder::new()
            .create::<ipc_threadsafe::Service>()
            .expect("Failed to create Iceoryx2 Node");
        let transport = Arc::new(Self {
            node,
            config,
            publishers: RwLock::new(HashMap::new()),
            subscribers: RwLock::new(HashMap::new()),
            listeners: RwLock::new(HashMap::new()),
            pull_subscribers: RwLock::new(HashMap::new()),
            pull_receive_queue_state: Mutex::new(PullReceiveQueueState::default()),
            zero_copy_listeners: RwLock::new(Vec::new()),
        });
        Iceoryx2WorkerDispatcher::start_listener_worker(transport.clone());
        transport
    }

    pub fn create_subscriber(
        &self,
        service_name: ServiceName,
        source: Option<&UUri>,
    ) -> Result<Subscriber<ipc_threadsafe::Service, [u8], UProtocolHeader>, UStatus> {
        let builder = self
            .node
            .service_builder(&service_name)
            .publish_subscribe::<[u8]>()
            .user_header::<UProtocolHeader>();
        let service = if let Some(source) = source {
            let attributes = source_attribute_verifier(source)?;
            builder
                .open_or_create_with_attributes(&attributes)
                .map_err(|e| {
                    UStatus::fail_with_code(
                        UCode::Internal,
                        format!("Failed to create service: {e}"),
                    )
                })?
        } else {
            builder.open().map_err(|e| {
                UStatus::fail_with_code(UCode::Internal, format!("Failed to open service: {e}"))
            })?
        };
        let subscriber = service.subscriber_builder().create().map_err(|e| {
            UStatus::fail_with_code(UCode::Internal, format!("Failed to create subscriber: {e}"))
        })?;
        Ok(subscriber)
    }

    pub fn publish_subscribe_service_name(
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
    ) -> Result<String, UStatus> {
        compute_service_name(
            source_filter,
            sink_filter,
            MessagingPattern::PublishSubscribe,
        )
        .map(|service_name| service_name.as_str().to_owned())
    }

    pub fn discover_service_names(&self) -> Result<Vec<String>, UStatus> {
        let mut services = Vec::new();
        ipc_threadsafe::Service::list(self.node.config(), |service| {
            services.push(service.static_details.name().as_str().to_owned());
            CallbackProgression::Continue
        })
        .map_err(|e| {
            UStatus::fail_with_code(UCode::Internal, format!("failed to list services: {e}"))
        })?;
        Ok(services)
    }

    pub fn discover_matching_service_names(
        &self,
        source_filter: &UUri,
    ) -> Result<Vec<String>, UStatus> {
        let mut services = Vec::new();
        ipc_threadsafe::Service::list(self.node.config(), |service| {
            if attributes_match_source_filter(service.static_details.attributes(), source_filter) {
                services.push(service.static_details.name().as_str().to_owned());
            }
            CallbackProgression::Continue
        })
        .map_err(|e| {
            UStatus::fail_with_code(UCode::Internal, format!("failed to list services: {e}"))
        })?;
        Ok(services)
    }

    pub async fn get_or_create_publisher(
        &self,
        service_name: ServiceName,
        source: &UUri,
    ) -> Result<Arc<Publisher<ipc_threadsafe::Service, [u8], UProtocolHeader>>, UStatus> {
        let publisher = self.get_publisher(service_name).await;
        if let Some(publisher) = publisher {
            return Ok(publisher);
        }
        self.create_publisher(service_name, source).await
    }

    async fn get_or_create_pull_subscriber(
        &self,
        service_name: ServiceName,
        source: Option<&UUri>,
    ) -> Result<Arc<IpcSubscriber>, UStatus> {
        let subscribers = self.pull_subscribers.read().await;
        if let Some(subscriber) = subscribers.get(&service_name) {
            return Ok(subscriber.clone());
        }
        drop(subscribers);

        let subscriber = Arc::new(self.create_subscriber(service_name, source)?);
        let mut subscribers = self.pull_subscribers.write().await;
        subscribers.insert(service_name, subscriber.clone());
        Ok(subscriber)
    }

    async fn create_publisher(
        &self,
        service_name: ServiceName,
        source: &UUri,
    ) -> Result<Arc<Publisher<ipc_threadsafe::Service, [u8], UProtocolHeader>>, UStatus> {
        let attributes = source_attribute_verifier(source)?;
        let service = self
            .node
            .service_builder(&service_name)
            .publish_subscribe::<[u8]>()
            .user_header::<UProtocolHeader>()
            .open_or_create_with_attributes(&attributes)
            .map_err(|e| {
                UStatus::fail_with_code(UCode::Internal, format!("Failed to create service: {e}"))
            })?;

        let publisher = service
            .publisher_builder()
            .initial_max_slice_len(self.config.publisher_initial_max_slice_len)
            .allocation_strategy(self.config.publisher_allocation_strategy)
            .create()
            .map_err(|e| {
                UStatus::fail_with_code(UCode::Internal, format!("Failed to create publisher: {e}"))
            })?;
        let mut publishers = self.publishers.write().await;
        publishers.insert(service_name, Arc::new(publisher));
        let publisher = publishers.get(&service_name).unwrap();
        Ok(publisher.clone())
    }

    async fn get_publisher(
        &self,
        service_name: ServiceName,
    ) -> Option<Arc<Publisher<ipc_threadsafe::Service, [u8], UProtocolHeader>>> {
        let publishers = self.publishers.read().await;
        publishers.get(&service_name).cloned()
    }

    pub async fn relay(&self) -> Result<(), UStatus> {
        self.relay_umessage_listeners().await?;
        self.relay_zero_copy_listeners().await
    }

    async fn relay_umessage_listeners(&self) -> Result<(), UStatus> {
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
                            format!("Failed to deserialize UMessage: {e}"),
                        )
                    })?;
                    if let Some(listeners_to_notify) = self.listeners.read().await.get(service_name)
                    {
                        for listener in listeners_to_notify.iter() {
                            let listener: &ComparableListener = listener;
                            listener.on_receive(umessage.clone()).await;
                        }
                    }
                }
                Ok(None) => continue,
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

    async fn relay_zero_copy_listeners(&self) -> Result<(), UStatus> {
        self.refresh_listener_subscriptions().await?;
        let mut received = Vec::new();
        {
            let registrations = self.zero_copy_listeners.read().await;
            for registration in registrations.iter() {
                for subscriber in registration.subscribers.values() {
                    match subscriber.receive() {
                        Ok(Some(sample)) => {
                            let lease = lease_from_sample(sample)?;
                            if !registration
                                .source_filter
                                .matches(lease.metadata().attributes().source())
                                || !sink_matches(
                                    lease.metadata().attributes().sink(),
                                    registration.sink_filter.as_ref(),
                                )
                            {
                                continue;
                            }
                            received.push((registration.listener.clone(), lease));
                        }
                        Ok(None) => continue,
                        Err(e) => {
                            return Err(UStatus::fail_with_code(
                                UCode::Internal,
                                format!("Failed to receive sample: {e}"),
                            ));
                        }
                    }
                }
            }
        }
        for (listener, lease) in received {
            listener.on_receive_zero_copy(lease).await;
        }
        Ok(())
    }

    pub async fn pull_mismatch_queue_diagnostics(&self) -> PullMismatchQueueDiagnostics {
        self.pull_receive_queue_state.lock().await.diagnostics()
    }

    async fn refresh_listener_subscriptions(&self) -> Result<(), UStatus> {
        if self.zero_copy_listeners.read().await.is_empty() {
            return Ok(());
        }

        let mut services = Vec::new();
        ipc_threadsafe::Service::list(self.node.config(), |service| {
            services.push((
                *service.static_details.name(),
                service.static_details.attributes().clone(),
            ));
            CallbackProgression::Continue
        })
        .map_err(|e| {
            UStatus::fail_with_code(UCode::Internal, format!("failed to list services: {e}"))
        })?;

        let mut registrations = self.zero_copy_listeners.write().await;
        for registration in registrations.iter_mut() {
            for (service_name, attributes) in &services {
                if registration.subscribers.contains_key(service_name)
                    || !attributes_match_source_filter(attributes, &registration.source_filter)
                {
                    continue;
                }
                let subscriber = self.create_subscriber(*service_name, None)?;
                registration
                    .subscribers
                    .insert(*service_name, Arc::new(subscriber));
            }
        }
        Ok(())
    }

    async fn pop_queued_pull_sample(
        &self,
        service_name: &ServiceName,
        sink_filter: Option<&UUri>,
    ) -> Option<Iceoryx2RxLease> {
        let mut state = self.pull_receive_queue_state.lock().await;
        let queue = state.queues.get_mut(service_name)?;
        let index = queue
            .iter()
            .position(|lease| sink_matches(lease.metadata().attributes().sink(), sink_filter))?;
        let lease = queue.remove(index);
        if queue.is_empty() {
            state.queues.remove(service_name);
        }
        lease
    }

    async fn queue_pull_sample(
        &self,
        service_name: ServiceName,
        lease: Iceoryx2RxLease,
    ) -> Result<(), UStatus> {
        let capacity = self.config.pull_mismatch_queue_capacity;
        let service_name_text = service_name.as_str().to_owned();
        let mut state = self.pull_receive_queue_state.lock().await;
        if capacity == 0 {
            state.dropped_mismatches = state.dropped_mismatches.saturating_add(1);
            state.last_mismatch_reason = Some(format!(
                "dropped mismatched pull sample for {service_name_text}; capacity is 0"
            ));
            return Ok(());
        }

        let is_full = state
            .queues
            .get(&service_name)
            .is_some_and(|queue| queue.len() >= capacity);
        if is_full
            && self.config.pull_mismatch_queue_full_policy
                == Iceoryx2PullMismatchQueueFullPolicy::RejectNewestAndReport
        {
            state.rejected_mismatches = state.rejected_mismatches.saturating_add(1);
            state.last_mismatch_reason = Some(format!(
                "rejected newest mismatched pull sample for {service_name_text}; capacity is {capacity}"
            ));
            return Err(UStatus::fail_with_code(
                UCode::ResourceExhausted,
                format!("pull mismatch queue full for {service_name_text}; capacity is {capacity}"),
            ));
        }

        let depth_after = {
            let queue = state.queues.entry(service_name).or_default();
            if is_full {
                queue.pop_front();
            }
            queue.push_back(lease);
            queue.len()
        };

        if is_full {
            state.dropped_mismatches = state.dropped_mismatches.saturating_add(1);
            state.last_mismatch_reason = Some(format!(
                "dropped oldest mismatched pull sample for {service_name_text}; capacity is {capacity}"
            ));
        } else {
            state.last_mismatch_reason = Some(format!(
                "queued mismatched pull sample for {service_name_text}; depth is {depth_after}"
            ));
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
        user_header.write_attributes(message.attributes())?;
        user_header
            .write_payload_layout(0, payload_len, 1)
            .map_err(frame_contract_error_to_status)?;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Iceoryx2PullMismatchQueueFullPolicy {
    DropOldestAndReport,
    RejectNewestAndReport,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PullMismatchQueueDiagnostics {
    pub current_depth: usize,
    pub dropped_mismatches: u64,
    pub rejected_mismatches: u64,
    pub last_mismatch_reason: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Iceoryx2PubSubConfig {
    pub publisher_initial_max_slice_len: usize,
    pub publisher_allocation_strategy: AllocationStrategy,
    pub pull_mismatch_queue_capacity: usize,
    pub pull_mismatch_queue_full_policy: Iceoryx2PullMismatchQueueFullPolicy,
}

impl Default for Iceoryx2PubSubConfig {
    fn default() -> Self {
        Self {
            publisher_initial_max_slice_len: 1,
            publisher_allocation_strategy: AllocationStrategy::PowerOfTwo,
            pull_mismatch_queue_capacity: 64,
            pull_mismatch_queue_full_policy:
                Iceoryx2PullMismatchQueueFullPolicy::DropOldestAndReport,
        }
    }
}

impl Iceoryx2PubSubConfig {
    #[must_use]
    pub fn static_allocation(max_slice_len: usize) -> Self {
        Self {
            publisher_initial_max_slice_len: max_slice_len,
            publisher_allocation_strategy: AllocationStrategy::Static,
            ..Self::default()
        }
    }

    #[must_use]
    pub fn with_publisher_initial_max_slice_len(mut self, value: usize) -> Self {
        self.publisher_initial_max_slice_len = value;
        self
    }

    #[must_use]
    pub fn with_publisher_allocation_strategy(mut self, value: AllocationStrategy) -> Self {
        self.publisher_allocation_strategy = value;
        self
    }

    #[must_use]
    pub fn with_pull_mismatch_queue_capacity(mut self, value: usize) -> Self {
        self.pull_mismatch_queue_capacity = value;
        self
    }

    #[must_use]
    pub fn with_pull_mismatch_queue_full_policy(
        mut self,
        value: Iceoryx2PullMismatchQueueFullPolicy,
    ) -> Self {
        self.pull_mismatch_queue_full_policy = value;
        self
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

/// iceoryx2 receive lease for one native uProtocol frame.
pub struct Iceoryx2RxLease {
    metadata: UFrameMetadata,
    sample: IpcSample,
    payload_offset: usize,
    payload_len: usize,
}

impl UFrameView for Iceoryx2RxLease {
    type PayloadReader<'a>
        = Cursor<&'a [u8]>
    where
        Self: 'a;
    type PayloadSlices<'a>
        = std::iter::Once<&'a [u8]>
    where
        Self: 'a;

    fn metadata(&self) -> &UFrameMetadata {
        &self.metadata
    }

    fn payload_len(&self) -> usize {
        self.payload_len
    }

    fn payload_reader(&self) -> Self::PayloadReader<'_> {
        Cursor::new(self.contiguous_payload())
    }

    fn payload_slices(&self) -> Self::PayloadSlices<'_> {
        std::iter::once(self.contiguous_payload())
    }

    fn try_contiguous_payload(&self) -> Option<&[u8]> {
        let end = self
            .payload_offset
            .checked_add(self.payload_len)
            .expect("received payload layout overflow");
        self.sample.payload().get(self.payload_offset..end)
    }
}

impl Iceoryx2RxLease {
    fn contiguous_payload(&self) -> &[u8] {
        self.try_contiguous_payload()
            .expect("iceoryx2 receive payload layout should be valid")
    }
}

impl UZeroCopyRxLease for Iceoryx2RxLease {}

impl ULoanedContiguousZeroCopyRxFrame for Iceoryx2RxLease {
    fn loaned_contiguous_payload(&self) -> Result<LoanedPayload<'_>, up_rust::payload::UWireError> {
        if !self.has_payload() {
            return Err(up_rust::payload::UWireError::MissingPayload);
        }
        let payload = self.try_contiguous_payload().ok_or_else(|| {
            up_rust::payload::UWireError::invalid_payload("payload is not contiguous")
        })?;
        // SAFETY: the bytes are borrowed directly from the live iceoryx2 sample
        // owned by this receive lease; no copy or coalescing is performed.
        Ok(unsafe {
            LoanedPayload::new_unchecked(payload, PayloadLoanProvenance::OpaqueTransportLoan)
        })
    }
}

#[async_trait]
impl UZeroCopyTransportImpl for Iceoryx2PubSub {
    type Tx = Iceoryx2TxLoan;
    type Rx = Iceoryx2RxLease;

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
        let publisher = self.get_or_create_publisher(service_name, source).await?;
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

    async fn receive_validated_zero_copy(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
    ) -> Result<Self::Rx, UStatus> {
        let service_name = compute_service_name(
            source_filter,
            sink_filter,
            MessagingPattern::PublishSubscribe,
        )?;
        let subscriber = self
            .get_or_create_pull_subscriber(service_name, Some(source_filter))
            .await?;
        if let Some(lease) = self
            .pop_queued_pull_sample(&service_name, sink_filter)
            .await
        {
            return Ok(lease);
        }
        loop {
            let sample = subscriber
                .receive()
                .map_err(|e| UStatus::fail_with_code(UCode::Internal, e.to_string()))?
                .ok_or_else(|| UStatus::fail_with_code(UCode::NotFound, "no sample available"))?;
            let lease = lease_from_sample(sample)?;
            if !sink_matches(lease.metadata().attributes().sink(), sink_filter) {
                self.queue_pull_sample(service_name, lease).await?;
                continue;
            }
            return Ok(lease);
        }
    }

    async fn register_validated_zero_copy_listener(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UZeroCopyListener<Self::Rx>>,
    ) -> Result<(), UStatus> {
        let mut listeners = self.zero_copy_listeners.write().await;
        if listeners.iter().any(|registration| {
            registration.has_same_identity(source_filter, sink_filter, &listener)
        }) {
            return Err(UStatus::fail_with_code(
                UCode::AlreadyExists,
                "zero-copy listener already registered for filter",
            ));
        }
        let mut registration =
            ZeroCopyListenerRegistration::new(source_filter, sink_filter, listener);
        if source_filter.verify_no_wildcards().is_ok() {
            let service_name = compute_service_name(
                source_filter,
                sink_filter,
                MessagingPattern::PublishSubscribe,
            )?;
            let subscriber = self.create_subscriber(service_name, Some(source_filter))?;
            registration
                .subscribers
                .insert(service_name, Arc::new(subscriber));
        }
        listeners.push(registration);
        Ok(())
    }

    async fn unregister_validated_zero_copy_listener(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UZeroCopyListener<Self::Rx>>,
    ) -> Result<(), UStatus> {
        let mut listeners = self.zero_copy_listeners.write().await;
        if let Some(index) = listeners.iter().position(|registration| {
            registration.has_same_identity(source_filter, sink_filter, &listener)
        }) {
            listeners.remove(index);
        }
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
        let publisher = self.get_or_create_publisher(service_name, source).await?;
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

fn lease_from_sample(sample: IpcSample) -> Result<Iceoryx2RxLease, UStatus> {
    let layout = sample
        .user_header()
        .payload_layout(sample.payload().len())
        .map_err(frame_contract_error_to_status)?;
    let payload_range = layout.payload_range();
    let payload_offset = payload_range.start;
    let payload_len = payload_range.end - payload_range.start;
    validate_payload_alignment(sample.user_header(), sample.payload(), payload_offset)?;
    let metadata = sample.user_header().frame_metadata(sample.payload())?;
    Ok(Iceoryx2RxLease {
        metadata,
        sample,
        payload_offset,
        payload_len,
    })
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
    user_header.write_attributes(metadata.attributes())?;
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

fn validate_payload_alignment(
    header: &UProtocolHeader,
    sample_payload: &[u8],
    payload_offset: usize,
) -> Result<(), UStatus> {
    let alignment = usize::try_from(header.payload_alignment).map_err(|_| {
        UStatus::fail_with_code(UCode::InvalidArgument, "payload alignment exceeds usize")
    })?;
    validate_alignment(alignment)?;
    let payload_address = (sample_payload.as_ptr() as usize)
        .checked_add(payload_offset)
        .ok_or_else(|| {
            UStatus::fail_with_code(UCode::InvalidArgument, "payload address overflow")
        })?;
    if payload_address & (alignment - 1) != 0 {
        return Err(UStatus::fail_with_code(
            UCode::InvalidArgument,
            "payload offset does not satisfy requested alignment",
        ));
    }
    Ok(())
}

fn sink_matches(actual: Option<&UUri>, filter: Option<&UUri>) -> bool {
    filter.is_none_or(|filter| actual.is_some_and(|actual| filter.matches(actual)))
}

#[async_trait]
impl UTransport for Iceoryx2PubSub {
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
            .get_or_create_publisher(service_name, message.source())
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
        if !has_subscriber {
            let subscriber = self.create_subscriber(service_name, Some(source_filter))?;
            let mut subscribers = self.subscribers.write().await;
            subscribers.insert(service_name, Arc::new(subscriber));
        }
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
mod zero_copy_tests {
    use super::*;
    use async_trait::async_trait;
    use tokio::sync::{Mutex as TokioMutex, MutexGuard};
    use up_rust::{
        UMessageBuilder, UPayloadFormat, UTxLoanSpec, UZeroCopyTransport, UZeroCopyUninitTransport,
        try_project_umessage_to_frame_metadata,
    };

    static ICEORYX2_TEST_MUTEX: TokioMutex<()> = TokioMutex::const_new(());

    async fn iceoryx2_test_guard() -> MutexGuard<'static, ()> {
        ICEORYX2_TEST_MUTEX.lock().await
    }

    fn test_topic(test_name: &str) -> Result<UUri, Box<dyn std::error::Error>> {
        let authority = format!("iox-{test_name}-{}", std::process::id());
        Ok(UUri::try_from_parts(&authority, 0x4210, 1, 0x9002)?)
    }

    fn test_metadata(test_name: &str) -> Result<UFrameMetadata, Box<dyn std::error::Error>> {
        let topic = test_topic(test_name)?;
        let mut builder = UMessageBuilder::publish(topic);
        let message = builder.build_with_payload(Vec::<u8>::new(), UPayloadFormat::Raw)?;
        Ok(try_project_umessage_to_frame_metadata(&message)?)
    }

    struct RecordingZeroCopyListener {
        payloads: TokioMutex<Vec<Vec<u8>>>,
    }

    impl RecordingZeroCopyListener {
        fn new() -> Self {
            Self {
                payloads: TokioMutex::new(Vec::new()),
            }
        }

        async fn payloads(&self) -> Vec<Vec<u8>> {
            self.payloads.lock().await.clone()
        }
    }

    #[async_trait]
    impl UZeroCopyListener<Iceoryx2RxLease> for RecordingZeroCopyListener {
        async fn on_receive_zero_copy(&self, frame: Iceoryx2RxLease) {
            self.payloads
                .lock()
                .await
                .push(frame.try_contiguous_payload().unwrap_or_default().to_vec());
        }
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

    #[tokio::test(flavor = "multi_thread")]
    async fn receive_zero_copy_returns_loan_backed_contiguous_payload()
    -> Result<(), Box<dyn std::error::Error>> {
        let _guard = iceoryx2_test_guard().await;
        let publisher = Iceoryx2PubSub::new();
        let subscriber = Iceoryx2PubSub::new();
        let topic = test_topic("rx-lease")?;
        match subscriber.receive_zero_copy(&topic, None).await {
            Ok(_) => panic!("subscriber unexpectedly received a frame before send"),
            Err(error) => assert_eq!(error.get_code(), UCode::NotFound),
        }
        let message = UMessageBuilder::publish(topic.clone())
            .build_with_payload(vec![1, 2, 3, 4], UPayloadFormat::Raw)?;
        let metadata = try_project_umessage_to_frame_metadata(&message)?;
        let mut loan = publisher
            .loan_tx(UTxLoanSpec::payload(metadata.clone(), 4, 8)?)
            .await?;
        loan.payload_mut().copy_from_slice(&[1, 2, 3, 4]);
        publisher.send_zero_copy(loan).await?;

        let mut last_error = None;
        for _ in 0..50 {
            match subscriber.receive_zero_copy(&topic, None).await {
                Ok(frame) => {
                    assert_eq!(frame.metadata(), &metadata);
                    assert_eq!(
                        frame.try_contiguous_payload(),
                        Some([1, 2, 3, 4].as_slice())
                    );
                    let payload = frame.loaned_contiguous_payload()?;
                    assert_eq!(payload.as_bytes(), [1, 2, 3, 4].as_slice());
                    assert_eq!(
                        payload.provenance(),
                        PayloadLoanProvenance::OpaqueTransportLoan
                    );
                    return Ok(());
                }
                Err(error) if error.get_code() == UCode::NotFound => {
                    last_error = Some(error);
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                Err(error) => return Err(Box::new(error) as Box<dyn std::error::Error>),
            }
        }
        Err(Box::new(
            last_error
                .unwrap_or_else(|| UStatus::fail_with_code(UCode::NotFound, "no sample available")),
        ) as Box<dyn std::error::Error>)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn receive_zero_copy_preserves_absent_payload() -> Result<(), Box<dyn std::error::Error>>
    {
        let _guard = iceoryx2_test_guard().await;
        let publisher = Iceoryx2PubSub::new();
        let subscriber = Iceoryx2PubSub::new();
        let topic = test_topic("rx-no-payload")?;
        let message = UMessageBuilder::publish(topic.clone()).build()?;
        let metadata = try_project_umessage_to_frame_metadata(&message)?;
        match subscriber.receive_zero_copy(&topic, None).await {
            Ok(_) => panic!("subscriber unexpectedly received a frame before send"),
            Err(error) => assert_eq!(error.get_code(), UCode::NotFound),
        }

        let loan = publisher
            .loan_tx(UTxLoanSpec::no_payload(metadata)?)
            .await?;
        publisher.send_zero_copy(loan).await?;

        for _ in 0..50 {
            match subscriber.receive_zero_copy(&topic, None).await {
                Ok(frame) => {
                    assert!(!frame.has_payload());
                    assert_eq!(frame.payload_len(), 0);
                    assert!(matches!(
                        frame.loaned_contiguous_payload(),
                        Err(up_rust::payload::UWireError::MissingPayload)
                    ));
                    return Ok(());
                }
                Err(error) if error.get_code() == UCode::NotFound => {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                Err(error) => return Err(Box::new(error) as Box<dyn std::error::Error>),
            }
        }
        Err(Box::new(UStatus::fail_with_code(
            UCode::NotFound,
            "no no-payload sample available",
        )) as Box<dyn std::error::Error>)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn zero_copy_listener_receives_loaned_payload() -> Result<(), Box<dyn std::error::Error>>
    {
        let _guard = iceoryx2_test_guard().await;
        let publisher = Iceoryx2PubSub::new();
        let listener_transport = Iceoryx2PubSub::new();
        let topic = test_topic("rx-listener")?;
        let listener = Arc::new(RecordingZeroCopyListener::new());
        listener_transport
            .register_zero_copy_listener(&topic, None, listener.clone())
            .await?;

        let message = UMessageBuilder::publish(topic.clone())
            .build_with_payload(vec![9, 8, 7], UPayloadFormat::Raw)?;
        let metadata = try_project_umessage_to_frame_metadata(&message)?;
        let mut loan = publisher
            .loan_tx(UTxLoanSpec::payload(metadata, 3, 1)?)
            .await?;
        loan.payload_mut().copy_from_slice(&[9, 8, 7]);
        publisher.send_zero_copy(loan).await?;

        for _ in 0..50 {
            if listener.payloads().await == vec![vec![9, 8, 7]] {
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        Err("listener did not receive the loaned payload".into())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn discovers_matching_iceoryx2_services_by_source_attributes()
    -> Result<(), Box<dyn std::error::Error>> {
        let _guard = iceoryx2_test_guard().await;
        let transport = Iceoryx2PubSub::new();
        let topic = test_topic("discovery")?;
        let metadata = test_metadata("discovery")?;
        let loan = transport
            .loan_tx(UTxLoanSpec::payload(metadata, 0, 1)?)
            .await?;
        transport.send_zero_copy(loan).await?;

        let expected = Iceoryx2PubSub::publish_subscribe_service_name(&topic, None)?;
        let discovered = transport.discover_service_names()?;
        assert!(
            discovered.iter().any(|service| service == &expected),
            "expected {expected} in {discovered:?}"
        );
        let matching = transport.discover_matching_service_names(&topic)?;
        assert!(
            matching.iter().any(|service| service == &expected),
            "expected {expected} in {matching:?}"
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn receive_zero_copy_queues_mismatched_sink() -> Result<(), Box<dyn std::error::Error>> {
        let _guard = iceoryx2_test_guard().await;
        let publisher = Iceoryx2PubSub::new();
        let subscriber = Iceoryx2PubSub::new();
        let source = test_topic("mismatch-source")?;
        let sink_a = UUri::try_from_parts("iox-mismatch-a", 0x4220, 1, 0)?;
        let sink_b = UUri::try_from_parts("iox-mismatch-b", 0x4220, 1, 0)?;
        match subscriber.receive_zero_copy(&source, Some(&sink_a)).await {
            Ok(_) => panic!("subscriber unexpectedly received a frame before send"),
            Err(error) => assert_eq!(error.get_code(), UCode::NotFound),
        }

        let message = UMessageBuilder::notification(source.clone(), sink_b.clone())
            .build_with_payload(vec![0x42], UPayloadFormat::Raw)?;
        let metadata = try_project_umessage_to_frame_metadata(&message)?;
        let mut loan = publisher
            .loan_tx(UTxLoanSpec::payload(metadata, 1, 1)?)
            .await?;
        loan.payload_mut()[0] = 0x42;
        publisher.send_zero_copy(loan).await?;

        match subscriber.receive_zero_copy(&source, Some(&sink_a)).await {
            Ok(_) => panic!("mismatched sink sample should not be returned"),
            Err(error) => assert_eq!(error.get_code(), UCode::NotFound),
        }
        let diagnostics = subscriber.pull_mismatch_queue_diagnostics().await;
        assert_eq!(diagnostics.current_depth, 1);
        assert_eq!(diagnostics.dropped_mismatches, 0);
        assert_eq!(diagnostics.rejected_mismatches, 0);

        let frame = subscriber.receive_zero_copy(&source, Some(&sink_b)).await?;
        assert_eq!(frame.try_contiguous_payload(), Some([0x42].as_slice()));
        assert_eq!(
            frame
                .metadata()
                .attributes()
                .sink()
                .map(UUri::authority_name),
            Some("iox-mismatch-b")
        );
        Ok(())
    }

    #[test]
    fn worst_case_aligned_sample_len_includes_max_alignment_padding() {
        assert_eq!(worst_case_aligned_sample_len(10, 6, 64).unwrap(), 79);
        assert_eq!(worst_case_aligned_sample_len(10, 6, 1).unwrap(), 16);
    }

    #[test]
    fn worst_case_aligned_sample_len_rejects_overflow() {
        let error = worst_case_aligned_sample_len(usize::MAX, 1, 2).unwrap_err();

        assert_eq!(error.get_code(), UCode::InvalidArgument);
    }
}
