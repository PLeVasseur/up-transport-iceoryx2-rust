// ################################################################################
// Copyright (c) 2025 Contributors to the Eclipse Foundation
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
use std::collections::{HashMap, VecDeque};
use std::io::Cursor;
use std::mem::MaybeUninit;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use up_rust::{
    UCode, UFrameMetadata, UStatus, UUri,
    transport::{ValidatedTxLoanSpec, validate_frame_view_for_transport},
    zero_copy::{
        LoanedPayload, LoanedPayloadUninitMut, PayloadLoanProvenance, UContiguousZeroCopyRxFrame,
        UFrameView, ULoanedContiguousZeroCopyRxFrame, UTxBuffer, UUninitTxBuffer,
        UZeroCopyListener, UZeroCopyRxLease, UZeroCopyTransportImpl, UZeroCopyUninitTransportImpl,
    },
};

use crate::workers::dispatcher::Iceoryx2WorkerDispatcher;
use crate::{
    PublisherSet, SubscriberSet,
    service_attributes::{attributes_match_source_filter, source_attribute_verifier},
    service_name_mapping::compute_service_name,
    uprotocolheader::{UProtocolHeader, encode_frame_metadata},
};

type IpcSampleMut = SampleMut<ipc_threadsafe::Service, [u8], UProtocolHeader>;
type IpcSampleMutUninit =
    SampleMutUninit<ipc_threadsafe::Service, [MaybeUninit<u8>], UProtocolHeader>;
type IpcSample = Sample<ipc_threadsafe::Service, [u8], UProtocolHeader>;
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

/// iceoryx2 publish-subscribe transport for native uProtocol frames.
///
/// This type is the concrete transport returned by
/// [`UTransportIceoryx2::build`](crate::transport::UTransportIceoryx2::build).
/// It implements [`UZeroCopyTransport`] using iceoryx2 loans and receive samples.
///
/// Variable native-frame metadata is stored in a hidden implementation metadata
/// prefix before the application payload. The payload views exposed through
/// [`Iceoryx2TxLoan`] and [`Iceoryx2RxLease`] exclude that prefix and any
/// alignment padding.
///
/// [`UZeroCopyTransport`]: up_rust::zero_copy::UZeroCopyTransport
pub struct Iceoryx2PubSub {
    node: Node<ipc_threadsafe::Service>,
    config: Iceoryx2PubSubConfig,
    /// Cached iceoryx2 publishers keyed by service name.
    ///
    /// This field is public for compatibility with existing tests and advanced
    /// integrations. Most applications should use the transport trait methods
    /// rather than manipulating publishers directly.
    pub publishers: PublisherSet<ipc_threadsafe::Service>,
    /// Cached pull-receive subscribers keyed by service name.
    ///
    /// Listener registrations maintain their own subscriber sets internally so
    /// multiple matching uProtocol listeners do not consume a single shared
    /// sample queue.
    pub subscribers: SubscriberSet<ipc_threadsafe::Service>,
    pull_receive_queue_state: Mutex<PullReceiveQueueState>,
    zero_copy_listeners: ZeroCopyListenerMap,
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

impl std::fmt::Debug for Iceoryx2PubSub {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Iceoryx2PubSub").finish_non_exhaustive()
    }
}

impl Iceoryx2PubSub {
    /// Creates a new publish-subscribe iceoryx2 transport and starts its listener
    /// discovery worker.
    pub fn new() -> Arc<Self> {
        Self::with_config(Iceoryx2PubSubConfig::default())
    }

    /// Creates a new publish-subscribe iceoryx2 transport with explicit allocation settings.
    pub fn with_config(config: Iceoryx2PubSubConfig) -> Arc<Self> {
        let node = NodeBuilder::new()
            .create::<ipc_threadsafe::Service>()
            .expect("Failed to create Iceoryx2 Node");
        let transport = Arc::new(Self {
            node,
            config,
            publishers: RwLock::new(HashMap::new()),
            subscribers: RwLock::new(HashMap::new()),
            pull_receive_queue_state: Mutex::new(PullReceiveQueueState::default()),
            zero_copy_listeners: RwLock::new(Vec::new()),
        });
        Iceoryx2WorkerDispatcher::start_listener_worker(transport.clone());
        transport
    }

    /// Creates an iceoryx2 subscriber for `service_name`.
    ///
    /// When `source` is provided, the service is opened or created with source
    /// attributes so wildcard listener discovery can match it later.
    ///
    /// # Errors
    ///
    /// Returns an error if the service or subscriber cannot be opened or created.
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
                        UCode::INTERNAL,
                        format!("Failed to create service: {e}"),
                    )
                })?
        } else {
            builder.open().map_err(|e| {
                UStatus::fail_with_code(UCode::INTERNAL, format!("Failed to open service: {e}"))
            })?
        };
        let subscriber = service.subscriber_builder().create().map_err(|e| {
            UStatus::fail_with_code(UCode::INTERNAL, format!("Failed to create subscriber: {e}"))
        })?;
        Ok(subscriber)
    }

    /// Computes the iceoryx2 service name for a uProtocol source/sink filter
    /// pair using the publish-subscribe mapping.
    ///
    /// This is primarily useful for diagnostics and tests that need to assert the
    /// service name visible to iceoryx2.
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

    /// Lists visible iceoryx2 service names for this node configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if iceoryx2 service listing fails.
    pub fn discover_service_names(&self) -> Result<Vec<String>, UStatus> {
        let mut services = Vec::new();
        ipc_threadsafe::Service::list(self.node.config(), |service| {
            services.push(service.static_details.name().as_str().to_owned());
            CallbackProgression::Continue
        })
        .map_err(|e| {
            UStatus::fail_with_code(UCode::INTERNAL, format!("failed to list services: {e}"))
        })?;
        Ok(services)
    }

    /// Lists service names whose source attributes match `source_filter`.
    ///
    /// This powers wildcard source listener registration for streamer-style
    /// subscriptions where the concrete iceoryx2 service name is not known ahead
    /// of time.
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
            UStatus::fail_with_code(UCode::INTERNAL, format!("failed to list services: {e}"))
        })?;
        Ok(services)
    }

    /// Returns an existing publisher for `service_name` or creates one with
    /// source attributes for discovery.
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

    /// Returns an existing pull subscriber for `service_name` or creates one.
    ///
    /// Listener registrations do not use this shared cache; they maintain
    /// independent subscribers so each listener can consume matching samples.
    pub async fn get_or_create_subscriber(
        &self,
        service_name: ServiceName,
        source: Option<&UUri>,
    ) -> Result<Arc<Subscriber<ipc_threadsafe::Service, [u8], UProtocolHeader>>, UStatus> {
        let subscribers = self.subscribers.read().await;
        if let Some(subscriber) = subscribers.get(&service_name) {
            return Ok(subscriber.clone());
        }
        drop(subscribers);

        let subscriber = self.create_subscriber(service_name, source)?;
        let mut subscribers = self.subscribers.write().await;
        let subscriber = Arc::new(subscriber);
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
                UStatus::fail_with_code(UCode::INTERNAL, format!("Failed to create service: {e}"))
            })?;

        let publisher = service
            .publisher_builder()
            .initial_max_slice_len(self.config.publisher_initial_max_slice_len)
            .allocation_strategy(self.config.publisher_allocation_strategy)
            .create()
            .map_err(|e| {
                UStatus::fail_with_code(UCode::INTERNAL, format!("Failed to create publisher: {e}"))
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
                                UCode::INTERNAL,
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

    /// Returns diagnostics for the bounded pull mismatch queue.
    pub async fn pull_mismatch_queue_diagnostics(&self) -> PullMismatchQueueDiagnostics {
        self.pull_receive_queue_state.lock().await.diagnostics()
    }

    async fn refresh_listener_subscriptions(&self) -> Result<(), UStatus> {
        let mut services = Vec::new();
        ipc_threadsafe::Service::list(self.node.config(), |service| {
            services.push((
                *service.static_details.name(),
                service.static_details.attributes().clone(),
            ));
            CallbackProgression::Continue
        })
        .map_err(|e| {
            UStatus::fail_with_code(UCode::INTERNAL, format!("failed to list services: {e}"))
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
                UCode::RESOURCE_EXHAUSTED,
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
}

/// Full-queue behavior for pull receive samples that do not match the requested sink.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Iceoryx2PullMismatchQueueFullPolicy {
    /// Preserve bounded pull receive behavior by dropping the oldest retained mismatch.
    DropOldestAndReport,
    /// Reject the newest mismatch and return [`UCode::RESOURCE_EXHAUSTED`] to the receive call.
    RejectNewestAndReport,
}

/// Snapshot of bounded pull mismatch queue state.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PullMismatchQueueDiagnostics {
    /// Total retained mismatch samples across all service queues.
    pub current_depth: usize,
    /// Number of mismatch samples dropped because a queue was full or capacity was zero.
    pub dropped_mismatches: u64,
    /// Number of mismatch samples rejected by [`Iceoryx2PullMismatchQueueFullPolicy::RejectNewestAndReport`].
    pub rejected_mismatches: u64,
    /// Human-readable reason recorded for the last mismatched pull sample.
    pub last_mismatch_reason: Option<String>,
}

/// Publisher allocation controls for [`Iceoryx2PubSub`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Iceoryx2PubSubConfig {
    /// Initial and static maximum slice length used for publisher loans.
    pub publisher_initial_max_slice_len: usize,
    /// iceoryx2 allocation strategy used when a publisher loan exceeds the initial length.
    pub publisher_allocation_strategy: AllocationStrategy,
    /// Maximum retained mismatched pull samples per iceoryx2 service.
    pub pull_mismatch_queue_capacity: usize,
    /// Policy applied when a per-service mismatch queue is full.
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
    /// Creates deterministic static-allocation settings that fail instead of growing.
    #[must_use]
    pub fn static_allocation(max_slice_len: usize) -> Self {
        Self {
            publisher_initial_max_slice_len: max_slice_len,
            publisher_allocation_strategy: AllocationStrategy::Static,
            ..Self::default()
        }
    }

    /// Sets the publisher initial/max slice length.
    #[must_use]
    pub fn with_publisher_initial_max_slice_len(mut self, value: usize) -> Self {
        self.publisher_initial_max_slice_len = value;
        self
    }

    /// Sets the publisher allocation strategy.
    #[must_use]
    pub fn with_publisher_allocation_strategy(mut self, value: AllocationStrategy) -> Self {
        self.publisher_allocation_strategy = value;
        self
    }

    /// Sets the maximum retained mismatched pull samples per iceoryx2 service.
    #[must_use]
    pub fn with_pull_mismatch_queue_capacity(mut self, value: usize) -> Self {
        self.pull_mismatch_queue_capacity = value;
        self
    }

    /// Sets the full-queue policy for retained mismatched pull samples.
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
///
/// Values are returned by [`UZeroCopyTransport::loan_tx`] for
/// [`Iceoryx2PubSub`]. The exposed payload range is aligned for the selected
/// serializer and excludes the hidden metadata prefix. After
/// [`UZeroCopyTransport::send_zero_copy`] consumes the loan, callers must treat
/// the underlying storage as no longer accessible.
///
/// [`UZeroCopyTransport::loan_tx`]: up_rust::zero_copy::UZeroCopyTransport::loan_tx
/// [`UZeroCopyTransport::send_zero_copy`]: up_rust::zero_copy::UZeroCopyTransport::send_zero_copy
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

    fn payload_loan_provenance(&self) -> PayloadLoanProvenance {
        PayloadLoanProvenance::SharedMemory
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

    fn payload_loan_provenance(&self) -> PayloadLoanProvenance {
        PayloadLoanProvenance::SharedMemory
    }

    fn payload_uninit_mut(&mut self) -> LoanedPayloadUninitMut<'_> {
        let end = self
            .payload_offset
            .checked_add(self.payload_len)
            .expect("loaned payload layout overflow");
        let payload = self
            .sample
            .payload_mut()
            .get_mut(self.payload_offset..end)
            .expect("loaned payload layout should be valid");
        // SAFETY:
        // - The range was checked against the iceoryx2 sample payload above and
        //   is the exact visible application payload range computed from the loan spec.
        // - `&mut self` gives exclusive access to the sample while the loaned
        //   uninit payload view exists.
        // - The backing storage is an iceoryx2 shared-memory sample.
        // - Per https://doc.rust-lang.org/stable/std/slice/fn.from_raw_parts_mut.html#safety,
        //   the backing slice must be valid for writes and "must not be accessed
        //   through any other pointer" for the returned lifetime; the mutable
        //   sample borrow supplies that exclusivity.
        unsafe {
            LoanedPayloadUninitMut::new_unchecked(payload, PayloadLoanProvenance::SharedMemory)
        }
    }

    unsafe fn assume_payload_init(self) -> Self::Initialized {
        // SAFETY CONTRACT:
        // - The caller of `UUninitTxBuffer::assume_payload_init` guarantees the
        //   visible application payload range returned by `payload_uninit_mut`
        //   was fully initialized before conversion.
        // - This implementation initializes the remaining sample tail bytes, so
        //   the iceoryx2 sample commit never observes uninitialized bytes.
        // - External contract: iceoryx2 preserves the sample allocation,
        //   alignment, and ownership semantics when `sample.assume_init()`
        //   commits the shared-memory loan.
        let mut sample = self.sample;
        let payload_end = self
            .payload_offset
            .checked_add(self.payload_len)
            .expect("loaned payload layout overflow");
        let payload = sample.payload_mut();
        let trailing = payload
            .get_mut(payload_end..)
            .expect("loaned trailing padding range should be valid");
        for byte in trailing {
            byte.write(0);
        }
        Iceoryx2TxLoan {
            metadata: self.metadata,
            // SAFETY:
            // - The caller of `assume_payload_init` guarantees the visible
            //   application payload bytes are initialized.
            // - This function initializes the trailing sample bytes before
            //   committing the iceoryx2 sample.
            // - The sample prefix/user-header bytes were initialized before the
            //   uninitialized payload loan was exposed.
            // - External contract: iceoryx2's `sample.assume_init()` commits the
            //   sample only after all sample bytes are initialized. This real
            //   shared-memory path is not Miri-feasible.
            sample: unsafe { sample.assume_init() },
            payload_offset: self.payload_offset,
            payload_len: self.payload_len,
        }
    }
}

/// iceoryx2 receive lease for one native uProtocol frame.
///
/// Dropping the lease releases the underlying iceoryx2 sample. The payload is
/// guaranteed contiguous in the current mapping, so this type implements both
/// [`UZeroCopyRxLease`] and [`UContiguousZeroCopyRxFrame`]. Borrowed decoded
/// values must not outlive the lease.
///
/// [`UZeroCopyRxLease`]: up_rust::zero_copy::UZeroCopyRxLease
/// [`UContiguousZeroCopyRxFrame`]: up_rust::zero_copy::UContiguousZeroCopyRxFrame
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

impl UZeroCopyRxLease for Iceoryx2RxLease {}

impl UContiguousZeroCopyRxFrame for Iceoryx2RxLease {
    fn contiguous_payload(&self) -> &[u8] {
        self.try_contiguous_payload()
            .expect("iceoryx2 receive payload layout should be valid")
    }
}

impl ULoanedContiguousZeroCopyRxFrame for Iceoryx2RxLease {
    fn loaned_contiguous_payload(&self) -> Result<LoanedPayload<'_>, up_rust::payload::UWireError> {
        let payload = self
            .try_contiguous_payload()
            .ok_or(up_rust::payload::UWireError::NotContiguous)?;
        // SAFETY:
        // - `payload` is borrowed directly from this receive lease's iceoryx2
        //   sample and remains valid for the lifetime of `&self`.
        // - No allocation or coalescing is performed to produce the slice, and
        //   the backing storage is shared memory.
        // - Per https://doc.rust-lang.org/stable/std/slice/fn.from_raw_parts.html#safety,
        //   a borrowed slice must be valid for reads and contained within one
        //   allocation; the iceoryx2 sample lease supplies that external
        //   provenance.
        Ok(unsafe { LoanedPayload::new_unchecked(payload, PayloadLoanProvenance::SharedMemory) })
    }
}

#[async_trait]
impl UZeroCopyTransportImpl for Iceoryx2PubSub {
    type Tx = Iceoryx2TxLoan;
    type Rx = Iceoryx2RxLease;

    async fn loan_validated_tx(&self, spec: ValidatedTxLoanSpec) -> Result<Self::Tx, UStatus> {
        let header = spec.metadata().clone();
        let payload_len = spec.payload_len();
        let alignment = spec.payload_alignment();
        validate_alignment(alignment)?;
        let source = header.attributes().source();
        let service_name = compute_service_name(
            source,
            header.attributes().sink(),
            MessagingPattern::PublishSubscribe,
        )?;
        let metadata = encode_frame_metadata(&header)?;
        let metadata_len = metadata.len();
        let publisher = self.get_or_create_publisher(service_name, source).await?;
        let sample_len = worst_case_aligned_sample_len(metadata_len, payload_len, alignment)?;
        let mut sample = publisher
            .loan_slice(sample_len)
            .map_err(|e| map_loan_error(e, "loan sample"))?;
        let payload_offset =
            aligned_payload_offset(sample.payload().as_ptr() as usize, metadata_len, alignment)?;
        let aligned_sample_len = payload_offset.checked_add(payload_len).ok_or_else(|| {
            UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "sample length overflow")
        })?;
        if aligned_sample_len > sample.payload().len() {
            return Err(UStatus::fail_with_code(
                UCode::INTERNAL,
                "reserved sample is too small for aligned payload layout",
            ));
        }
        sample
            .payload_mut()
            .get_mut(..metadata_len)
            .ok_or_else(|| {
                UStatus::fail_with_code(UCode::INTERNAL, "failed to access metadata prefix")
            })?
            .copy_from_slice(&metadata);
        sample.user_header_mut().write_frame_metadata(
            &header,
            metadata_len,
            payload_offset,
            payload_len,
            alignment,
        )?;
        Ok(Iceoryx2TxLoan {
            metadata: header,
            sample,
            payload_offset,
            payload_len,
        })
    }

    async fn send_validated_zero_copy(&self, buffer: Self::Tx) -> Result<(), UStatus> {
        buffer.sample.send().map_err(|e| {
            UStatus::fail_with_code(UCode::INTERNAL, format!("Failed to send: {e}"))
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
            .get_or_create_subscriber(service_name, Some(source_filter))
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
                .map_err(|e| UStatus::fail_with_code(UCode::INTERNAL, e.to_string()))?
                .ok_or_else(|| UStatus::fail_with_code(UCode::NOT_FOUND, "no sample available"))?;
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
                UCode::ALREADY_EXISTS,
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
        let Some(index) = listeners.iter().position(|registration| {
            registration.has_same_identity(source_filter, sink_filter, &listener)
        }) else {
            return Ok(());
        };
        listeners.remove(index);
        if listeners.is_empty() {
            self.subscribers.write().await.clear();
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
        let header = spec.metadata().clone();
        let payload_len = spec.payload_len();
        let alignment = spec.payload_alignment();
        validate_alignment(alignment)?;
        let source = header.attributes().source();
        let service_name = compute_service_name(
            source,
            header.attributes().sink(),
            MessagingPattern::PublishSubscribe,
        )?;
        let metadata = encode_frame_metadata(&header)?;
        let metadata_len = metadata.len();
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
            UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "sample length overflow")
        })?;
        if aligned_sample_len > sample.payload_mut().len() {
            return Err(UStatus::fail_with_code(
                UCode::INTERNAL,
                "reserved uninitialized sample is too small for aligned payload layout",
            ));
        }
        write_uninit_bytes(sample.payload_mut(), 0, &metadata)?;
        initialize_uninit_range(sample.payload_mut(), metadata_len, payload_offset)?;
        sample.user_header_mut().write_frame_metadata(
            &header,
            metadata_len,
            payload_offset,
            payload_len,
            alignment,
        )?;
        Ok(Iceoryx2UninitTxLoan {
            metadata: header,
            sample,
            payload_offset,
            payload_len,
        })
    }
}

fn lease_from_sample(sample: IpcSample) -> Result<Iceoryx2RxLease, UStatus> {
    let (payload_offset, payload_len) = sample
        .user_header()
        .payload_layout(sample.payload().len())?;
    let metadata = sample.user_header().frame_metadata(sample.payload())?;
    validate_payload_alignment(sample.user_header(), sample.payload(), payload_offset)?;
    let lease = Iceoryx2RxLease {
        metadata,
        sample,
        payload_offset,
        payload_len,
    };
    validate_frame_view_for_transport(&lease)?;
    Ok(lease)
}

fn validate_alignment(alignment: usize) -> Result<(), UStatus> {
    if alignment == 0 || !alignment.is_power_of_two() {
        return Err(UStatus::fail_with_code(
            UCode::INVALID_ARGUMENT,
            "payload alignment must be a non-zero power of two",
        ));
    }
    Ok(())
}

fn map_loan_error(error: LoanError, operation: &str) -> UStatus {
    let code = match error {
        LoanError::OutOfMemory | LoanError::ExceedsMaxLoans | LoanError::ExceedsMaxLoanSize => {
            UCode::RESOURCE_EXHAUSTED
        }
        LoanError::InternalFailure => UCode::INTERNAL,
    };
    UStatus::fail_with_code(code, format!("Failed to {operation}: {error}"))
}

fn write_uninit_bytes(
    sample: &mut [MaybeUninit<u8>],
    offset: usize,
    bytes: &[u8],
) -> Result<(), UStatus> {
    let end = offset.checked_add(bytes.len()).ok_or_else(|| {
        UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "sample initialization overflow")
    })?;
    let dst = sample.get_mut(offset..end).ok_or_else(|| {
        UStatus::fail_with_code(
            UCode::INTERNAL,
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
            UCode::INTERNAL,
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
        .ok_or_else(|| UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "sample length overflow"))
}

fn aligned_payload_offset(
    payload_base: usize,
    metadata_len: usize,
    alignment: usize,
) -> Result<usize, UStatus> {
    let payload_start = payload_base.checked_add(metadata_len).ok_or_else(|| {
        UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "payload address overflow")
    })?;
    let padding = (alignment - (payload_start & (alignment - 1))) & (alignment - 1);
    metadata_len
        .checked_add(padding)
        .ok_or_else(|| UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "payload offset overflow"))
}

fn validate_payload_alignment(
    header: &UProtocolHeader,
    sample_payload: &[u8],
    payload_offset: usize,
) -> Result<(), UStatus> {
    let alignment = usize::try_from(header.payload_alignment).map_err(|_| {
        UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "payload alignment exceeds usize")
    })?;
    validate_alignment(alignment)?;
    let payload_address = (sample_payload.as_ptr() as usize)
        .checked_add(payload_offset)
        .ok_or_else(|| {
            UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "payload address overflow")
        })?;
    if payload_address & (alignment - 1) != 0 {
        return Err(UStatus::fail_with_code(
            UCode::INVALID_ARGUMENT,
            "payload offset does not satisfy requested alignment",
        ));
    }
    Ok(())
}

fn sink_matches(actual: Option<&UUri>, filter: Option<&UUri>) -> bool {
    filter.is_none_or(|filter| actual.is_some_and(|actual| filter.matches(actual)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worst_case_aligned_sample_len_includes_max_alignment_padding() {
        assert_eq!(worst_case_aligned_sample_len(10, 6, 64).unwrap(), 79);
        assert_eq!(worst_case_aligned_sample_len(10, 6, 1).unwrap(), 16);
    }

    #[test]
    fn worst_case_aligned_sample_len_rejects_overflow() {
        let error = worst_case_aligned_sample_len(usize::MAX, 1, 2).unwrap_err();

        assert_eq!(error.get_code(), UCode::INVALID_ARGUMENT);
    }
}
