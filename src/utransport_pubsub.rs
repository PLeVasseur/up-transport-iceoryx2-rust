// ################################################################################
// Copyright (c) 2026 Contributors to the Eclipse Foundation
//
// SPDX-License-Identifier: Apache-2.0
// ################################################################################

use async_trait::async_trait;
use iceoryx2::port::LoanError;
use iceoryx2::prelude::{
    AllocationStrategy, CallbackProgression, Config, MessagingPattern, SemanticString, Service,
};
use iceoryx2::sample::Sample;
use iceoryx2::sample_mut::SampleMut;
use iceoryx2::sample_mut_uninit::SampleMutUninit;
use iceoryx2::{
    node::{Node, NodeBuilder},
    port::{publisher::Publisher, subscriber::Subscriber},
    prelude::ServiceName,
    service::builder::publish_subscribe::PublishSubscribeOpenError,
    service::ipc_threadsafe,
};
use iceoryx2_bb_system_types::{file_name::FileName, path::Path};
use std::{
    collections::{HashMap, VecDeque},
    io::Cursor,
    mem::MaybeUninit,
    sync::{Arc, Mutex as StdMutex, OnceLock, Weak},
};
use tokio::sync::{Mutex, RwLock};
use up_rust::selected_wire_user_api::{UNativePrefixWireTransport, UWithNativePrefixWire};
use up_rust::transport_implementer_api::{
    PreparedTxLoanSpec, UEncodedLoanedRxFrame, UEncodedRxFrame, UEncodedZeroCopyListener,
    UZeroCopyTransportCore, UZeroCopyUninitTransportCore,
};
use up_rust::wire_implementer_api::UWire;
use up_rust::{
    ExactUUri, LoanedPayload, PayloadLoanProvenance, UCode, UFrameMetadata, UStatus, UTxBuffer,
    UUninitTxBuffer, UUri,
};

use crate::service_attributes::{attributes_match_source_filter, source_attribute_verifier};
use crate::service_name_mapping::{
    compute_exact_source_publish_subscribe_service_name, compute_service_name,
};
use crate::uprotocolheader::{
    Iceoryx2PayloadLayout, UProtocolHeader, frame_contract_error_to_status,
};
use crate::workers::dispatcher::Iceoryx2WorkerDispatcher;

type IpcSample = Sample<ipc_threadsafe::Service, [u8], UProtocolHeader>;
type IpcSampleMut = SampleMut<ipc_threadsafe::Service, [u8], UProtocolHeader>;
type IpcSampleMutUninit =
    SampleMutUninit<ipc_threadsafe::Service, [MaybeUninit<u8>], UProtocolHeader>;
type IpcPublisher = Publisher<ipc_threadsafe::Service, [u8], UProtocolHeader>;
type IpcSubscriber = Subscriber<ipc_threadsafe::Service, [u8], UProtocolHeader>;
type PublisherSet = RwLock<HashMap<ServiceName, Arc<IpcPublisher>>>;
type SubscriberSet = RwLock<HashMap<ServiceName, Arc<IpcSubscriber>>>;
type BroadReceiverRegistry = StdMutex<Vec<Weak<Iceoryx2PubSubInner>>>;

static BROAD_RECEIVER_REGISTRY: OnceLock<BroadReceiverRegistry> = OnceLock::new();

#[derive(Clone)]
pub struct Iceoryx2PubSub {
    inner: Arc<Iceoryx2PubSubInner>,
}

impl std::fmt::Debug for Iceoryx2PubSub {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Iceoryx2PubSub").finish_non_exhaustive()
    }
}

pub(crate) struct Iceoryx2PubSubInner {
    node: Node<ipc_threadsafe::Service>,
    config: Iceoryx2PubSubConfig,
    publishers: PublisherSet,
    pull_subscribers: SubscriberSet,
    pending_pull_source_filters: RwLock<Vec<UUri>>,
    pull_receive_queue_state: Mutex<PullReceiveQueueState>,
    zero_copy_listeners: RwLock<Vec<ZeroCopyListenerRegistration>>,
}

#[derive(Default)]
struct PullReceiveQueueState {
    queues: HashMap<ServiceName, VecDeque<Iceoryx2RxLease>>,
    dropped_mismatches: u64,
    rejected_mismatches: u64,
    last_mismatch_reason: Option<String>,
}

impl PullReceiveQueueState {
    fn diagnostics(&self) -> PullMismatchQueueDiagnostics {
        PullMismatchQueueDiagnostics {
            current_depth: self.queues.values().map(VecDeque::len).sum(),
            dropped_mismatches: self.dropped_mismatches,
            rejected_mismatches: self.rejected_mismatches,
            last_mismatch_reason: self.last_mismatch_reason.clone(),
        }
    }
}

struct ZeroCopyListenerRegistration {
    source_filter: UUri,
    sink_filter: Option<UUri>,
    listener: Arc<dyn UEncodedZeroCopyListener<Iceoryx2RxLease>>,
    subscribers: HashMap<ServiceName, Arc<IpcSubscriber>>,
}

impl ZeroCopyListenerRegistration {
    fn new(
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UEncodedZeroCopyListener<Iceoryx2RxLease>>,
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
        listener: &Arc<dyn UEncodedZeroCopyListener<Iceoryx2RxLease>>,
    ) -> bool {
        self.source_filter == *source_filter
            && self.sink_filter.as_ref() == sink_filter
            && Arc::ptr_eq(&self.listener, listener)
    }
}

impl Iceoryx2PubSub {
    #[must_use]
    pub fn new() -> Self {
        Self::with_config(Iceoryx2PubSubConfig::default())
    }

    #[must_use]
    pub fn with_config(config: Iceoryx2PubSubConfig) -> Self {
        let env_config = if config.iceoryx2_config.is_none() {
            Self::env_iceoryx2_config()
        } else {
            None
        };
        let mut node_builder = NodeBuilder::new();
        let effective_iceoryx2_config = config.iceoryx2_config.as_ref().or(env_config.as_ref());
        if let Some(iceoryx2_config) = effective_iceoryx2_config {
            node_builder = node_builder.config(iceoryx2_config);
        }
        let node = match node_builder.create::<ipc_threadsafe::Service>() {
            Ok(node) => node,
            Err(error) if effective_iceoryx2_config.is_none() => {
                let fallback_config = Self::fallback_iceoryx2_config();
                NodeBuilder::new()
                    .config(&fallback_config)
                    .create::<ipc_threadsafe::Service>()
                    .unwrap_or_else(|fallback_error| {
                        panic!(
                            "failed to create iceoryx2 node: {error}; fallback config failed: {fallback_error}"
                        )
                    })
            }
            Err(error) => panic!("failed to create iceoryx2 node: {error}"),
        };
        let inner = Arc::new(Iceoryx2PubSubInner {
            node,
            config,
            publishers: RwLock::new(HashMap::new()),
            pull_subscribers: RwLock::new(HashMap::new()),
            pending_pull_source_filters: RwLock::new(Vec::new()),
            pull_receive_queue_state: Mutex::new(PullReceiveQueueState::default()),
            zero_copy_listeners: RwLock::new(Vec::new()),
        });
        Iceoryx2WorkerDispatcher::start_listener_worker(inner.clone());
        Self { inner }
    }

    /// Wraps this core in the generic selected-wire adapter.
    #[must_use]
    pub fn with_selected_wire<W>(self, wire: W) -> UNativePrefixWireTransport<Self, W>
    where
        W: UWire,
    {
        self.into_native_prefix_wire_transport(wire)
    }

    fn env_iceoryx2_config() -> Option<Config> {
        if std::env::var_os("UP_ICEORYX2_ROOT_PATH").is_none()
            && std::env::var_os("UP_ICEORYX2_PREFIX").is_none()
        {
            None
        } else {
            Some(Self::fallback_iceoryx2_config())
        }
    }

    fn fallback_iceoryx2_config() -> Config {
        let mut config = Config::default();
        if let Ok(root_path) = std::env::var("UP_ICEORYX2_ROOT_PATH") {
            if let Ok(root_path) = Path::new(root_path.as_bytes()) {
                config.global.set_root_path(&root_path);
            }
        } else {
            config
                .global
                .set_root_path(&Path::new(b"/tmp/up-iceoryx2").expect("fallback root path"));
        }
        if let Ok(prefix) = std::env::var("UP_ICEORYX2_PREFIX") {
            if let Ok(prefix) = FileName::new(prefix.as_bytes()) {
                config.global.prefix = prefix;
            }
        } else {
            config.global.prefix = FileName::new(b"up_iceoryx2_").expect("fallback prefix");
        }
        config
    }

    pub async fn pull_mismatch_queue_diagnostics(&self) -> PullMismatchQueueDiagnostics {
        self.inner
            .pull_receive_queue_state
            .lock()
            .await
            .diagnostics()
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

    pub fn exact_source_publish_subscribe_service_name(
        source: &ExactUUri,
    ) -> Result<String, UStatus> {
        compute_exact_source_publish_subscribe_service_name(source)
            .map(|service_name| service_name.as_str().to_owned())
    }

    pub fn discover_service_names(&self) -> Result<Vec<String>, UStatus> {
        self.inner.discover_service_names()
    }

    pub fn discover_matching_service_names(
        &self,
        source_filter: &UUri,
    ) -> Result<Vec<String>, UStatus> {
        self.inner.discover_matching_service_names(source_filter)
    }
}

impl Default for Iceoryx2PubSub {
    fn default() -> Self {
        Self::new()
    }
}

impl Iceoryx2PubSubInner {
    fn create_subscriber(
        &self,
        service_name: ServiceName,
        source: Option<&UUri>,
    ) -> Result<IpcSubscriber, UStatus> {
        let builder = self
            .node
            .service_builder(&service_name)
            .publish_subscribe::<[u8]>()
            .user_header::<UProtocolHeader>();
        let service = if let Some(source) = source {
            builder
                .open_or_create_with_attributes(&source_attribute_verifier(source)?)
                .map_err(|error| {
                    UStatus::fail_with_code(
                        UCode::Internal,
                        format!("failed to create iceoryx2 service: {error}"),
                    )
                })?
        } else {
            builder.open().map_err(|error| {
                if matches!(error, PublishSubscribeOpenError::DoesNotExist) {
                    return UStatus::fail_with_code(
                        UCode::NotFound,
                        format!("iceoryx2 service does not exist: {service_name}"),
                    );
                }
                UStatus::fail_with_code(
                    UCode::Internal,
                    format!("failed to open iceoryx2 service: {error}"),
                )
            })?
        };
        service.subscriber_builder().create().map_err(|error| {
            UStatus::fail_with_code(
                UCode::Internal,
                format!("failed to create iceoryx2 subscriber: {error}"),
            )
        })
    }

    async fn get_or_create_publisher(
        &self,
        service_name: ServiceName,
        source: &UUri,
    ) -> Result<Arc<IpcPublisher>, UStatus> {
        if let Some(publisher) = self.publishers.read().await.get(&service_name) {
            return Ok(publisher.clone());
        }

        let service = self
            .node
            .service_builder(&service_name)
            .publish_subscribe::<[u8]>()
            .user_header::<UProtocolHeader>()
            .open_or_create_with_attributes(&source_attribute_verifier(source)?)
            .map_err(|error| {
                UStatus::fail_with_code(
                    UCode::Internal,
                    format!("failed to create iceoryx2 service: {error}"),
                )
            })?;
        let publisher = service
            .publisher_builder()
            .initial_max_slice_len(self.config.publisher_initial_max_slice_len)
            .allocation_strategy(self.config.publisher_allocation_strategy)
            .create()
            .map_err(|error| {
                UStatus::fail_with_code(
                    UCode::Internal,
                    format!("failed to create iceoryx2 publisher: {error}"),
                )
            })?;
        let publisher = Arc::new(publisher);
        self.publishers
            .write()
            .await
            .insert(service_name, publisher.clone());
        Ok(publisher)
    }

    async fn get_or_create_pull_subscriber(
        &self,
        service_name: ServiceName,
        source: Option<&UUri>,
    ) -> Result<Arc<IpcSubscriber>, UStatus> {
        if let Some(subscriber) = self.pull_subscribers.read().await.get(&service_name) {
            return Ok(subscriber.clone());
        }
        let subscriber = Arc::new(self.create_subscriber(service_name, source)?);
        self.pull_subscribers
            .write()
            .await
            .insert(service_name, subscriber.clone());
        Ok(subscriber)
    }

    async fn register_pending_pull_source_filter(&self, source_filter: &UUri) {
        let mut source_filters = self.pending_pull_source_filters.write().await;
        if !source_filters.iter().any(|filter| filter == source_filter) {
            source_filters.push(source_filter.clone());
        }
    }

    async fn ensure_subscribers_for_source(
        &self,
        service_name: &ServiceName,
        source: &UUri,
    ) -> Result<(), UStatus> {
        let pending_pull_source_filters = self.pending_pull_source_filters.read().await.clone();
        if pending_pull_source_filters
            .iter()
            .any(|filter| filter.matches(source))
        {
            self.get_or_create_pull_subscriber(*service_name, None)
                .await?;
        }

        let mut registrations = self.zero_copy_listeners.write().await;
        for registration in registrations.iter_mut() {
            if !registration.source_filter.matches(source)
                || registration.subscribers.contains_key(service_name)
            {
                continue;
            }
            let subscriber = self.create_subscriber(*service_name, None)?;
            registration
                .subscribers
                .insert(*service_name, Arc::new(subscriber));
        }
        Ok(())
    }

    fn discover_service_names(&self) -> Result<Vec<String>, UStatus> {
        let mut services = Vec::new();
        ipc_threadsafe::Service::list(self.node.config(), |service| {
            services.push(service.static_details.name().as_str().to_owned());
            CallbackProgression::Continue
        })
        .map_err(|error| {
            UStatus::fail_with_code(
                UCode::Internal,
                format!("failed to list iceoryx2 services: {error}"),
            )
        })?;
        Ok(services)
    }

    fn discover_matching_service_names(
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
        .map_err(|error| {
            UStatus::fail_with_code(
                UCode::Internal,
                format!("failed to list iceoryx2 services: {error}"),
            )
        })?;
        Ok(services)
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
            .position(|lease| sink_matches(lease.sink_filter_hint(), sink_filter))?;
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
        let depth = {
            let queue = state.queues.entry(service_name).or_default();
            if is_full {
                queue.pop_front();
            }
            queue.push_back(lease);
            queue.len()
        };
        if is_full {
            state.dropped_mismatches = state.dropped_mismatches.saturating_add(1);
        }
        state.last_mismatch_reason = Some(format!(
            "queued mismatched pull sample for {service_name_text}; depth is {}",
            depth
        ));
        Ok(())
    }

    pub(crate) async fn relay_zero_copy_listeners(&self) -> Result<(), UStatus> {
        self.refresh_listener_subscriptions().await?;
        let mut received = Vec::new();
        {
            let registrations = self.zero_copy_listeners.read().await;
            for registration in registrations.iter() {
                for subscriber in registration.subscribers.values() {
                    match subscriber.receive() {
                        Ok(Some(sample)) => {
                            let lease = lease_from_sample(
                                sample,
                                registration.source_filter.clone(),
                                registration.sink_filter.clone(),
                            )?;
                            if !registration
                                .source_filter
                                .matches(lease.source_filter_hint())
                                || !sink_matches(
                                    lease.sink_filter_hint(),
                                    registration.sink_filter.as_ref(),
                                )
                            {
                                continue;
                            }
                            received.push((registration.listener.clone(), lease));
                        }
                        Ok(None) => continue,
                        Err(error) => {
                            return Err(UStatus::fail_with_code(
                                UCode::Internal,
                                format!("failed to receive iceoryx2 sample: {error}"),
                            ));
                        }
                    }
                }
            }
        }
        for (listener, lease) in received {
            listener.on_receive_encoded_zero_copy(lease).await;
        }
        Ok(())
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
        .map_err(|error| {
            UStatus::fail_with_code(
                UCode::Internal,
                format!("failed to list iceoryx2 services: {error}"),
            )
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
}

fn broad_receiver_registry() -> &'static BroadReceiverRegistry {
    BROAD_RECEIVER_REGISTRY.get_or_init(|| StdMutex::new(Vec::new()))
}

fn register_broad_receiver(inner: &Arc<Iceoryx2PubSubInner>) {
    let weak_inner = Arc::downgrade(inner);
    let mut receivers = broad_receiver_registry()
        .lock()
        .expect("broad receiver registry lock poisoned");
    receivers.retain(|receiver| receiver.upgrade().is_some());
    if !receivers
        .iter()
        .any(|receiver| receiver.ptr_eq(&weak_inner))
    {
        receivers.push(weak_inner);
    }
}

async fn notify_broad_receivers(service_name: &ServiceName, source: &UUri) -> Result<(), UStatus> {
    let receivers = {
        let mut receivers = broad_receiver_registry()
            .lock()
            .expect("broad receiver registry lock poisoned");
        let live_receivers = receivers
            .iter()
            .filter_map(Weak::upgrade)
            .collect::<Vec<_>>();
        receivers.retain(|receiver| receiver.upgrade().is_some());
        live_receivers
    };

    for receiver in receivers {
        receiver
            .ensure_subscribers_for_source(service_name, source)
            .await?;
    }
    Ok(())
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Iceoryx2PubSubConfig {
    pub publisher_initial_max_slice_len: usize,
    pub publisher_allocation_strategy: AllocationStrategy,
    pub pull_mismatch_queue_capacity: usize,
    pub pull_mismatch_queue_full_policy: Iceoryx2PullMismatchQueueFullPolicy,
    pub iceoryx2_config: Option<Config>,
}

impl Default for Iceoryx2PubSubConfig {
    fn default() -> Self {
        Self {
            publisher_initial_max_slice_len: 1,
            publisher_allocation_strategy: AllocationStrategy::PowerOfTwo,
            pull_mismatch_queue_capacity: 64,
            pull_mismatch_queue_full_policy:
                Iceoryx2PullMismatchQueueFullPolicy::DropOldestAndReport,
            iceoryx2_config: None,
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
    pub fn with_pull_mismatch_queue_capacity(mut self, value: usize) -> Self {
        self.pull_mismatch_queue_capacity = value;
        self
    }

    #[must_use]
    pub fn with_iceoryx2_config(mut self, value: Config) -> Self {
        self.iceoryx2_config = Some(value);
        self
    }
}

pub struct Iceoryx2TxLoan {
    metadata: UFrameMetadata,
    sample: IpcSampleMut,
    payload_offset: usize,
    payload_len: usize,
    encoded_metadata_len: usize,
}

pub struct Iceoryx2UninitTxLoan {
    metadata: UFrameMetadata,
    sample: IpcSampleMutUninit,
    payload_offset: usize,
    payload_len: usize,
}

pub struct Iceoryx2RxLease {
    sample: IpcSample,
    layout: Iceoryx2PayloadLayout,
    source_filter_hint: UUri,
    sink_filter_hint: Option<UUri>,
}

impl Iceoryx2TxLoan {
    #[must_use]
    pub fn encoded_metadata(&self) -> &[u8] {
        self.sample
            .payload()
            .get(..self.encoded_metadata_len)
            .expect("prepared metadata range should be valid")
    }

    #[must_use]
    pub fn header(&self) -> UProtocolHeader {
        *self.sample.user_header()
    }
}

impl UTxBuffer for Iceoryx2TxLoan {
    fn metadata(&self) -> &UFrameMetadata {
        &self.metadata
    }

    fn payload(&self) -> &[u8] {
        self.sample
            .payload()
            .get(self.payload_offset..self.payload_offset + self.payload_len)
            .expect("loaned payload layout should be valid")
    }

    fn payload_mut(&mut self) -> &mut [u8] {
        self.sample
            .payload_mut()
            .get_mut(self.payload_offset..self.payload_offset + self.payload_len)
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
        self.sample
            .payload_mut()
            .get_mut(self.payload_offset..self.payload_offset + self.payload_len)
            .expect("loaned payload layout should be valid")
    }

    unsafe fn assume_payload_init(self) -> Self::Initialized {
        let mut sample = self.sample;
        let payload_end = self
            .payload_offset
            .checked_add(self.payload_len)
            .expect("loaned payload layout overflow");
        for byte in sample
            .payload_mut()
            .get_mut(payload_end..)
            .expect("loaned trailing padding range should be valid")
        {
            byte.write(0);
        }
        let encoded_metadata_len = usize::try_from(sample.user_header().metadata_len).unwrap_or(0);
        Iceoryx2TxLoan {
            metadata: self.metadata,
            // SAFETY: the caller guarantees all visible payload bytes were initialized;
            // metadata and padding bytes were initialized before commit.
            sample: unsafe { sample.assume_init() },
            payload_offset: self.payload_offset,
            payload_len: self.payload_len,
            encoded_metadata_len,
        }
    }
}

impl Iceoryx2RxLease {
    fn source_filter_hint(&self) -> &UUri {
        &self.source_filter_hint
    }

    fn sink_filter_hint(&self) -> Option<&UUri> {
        self.sink_filter_hint.as_ref()
    }
}

impl UEncodedRxFrame for Iceoryx2RxLease {
    type PayloadReader<'a>
        = Cursor<&'a [u8]>
    where
        Self: 'a;
    type PayloadSlices<'a>
        = std::iter::Once<&'a [u8]>
    where
        Self: 'a;

    fn encoded_metadata(&self) -> &[u8] {
        self.layout
            .metadata_prefix(self.sample.payload())
            .expect("validated metadata prefix should be valid")
    }

    fn payload_len(&self) -> usize {
        self.layout.payload_len()
    }

    fn payload_reader(&self) -> Self::PayloadReader<'_> {
        Cursor::new(self.try_contiguous_payload().unwrap_or_default())
    }

    fn payload_slices(&self) -> Self::PayloadSlices<'_> {
        std::iter::once(self.try_contiguous_payload().unwrap_or_default())
    }

    fn try_contiguous_payload(&self) -> Option<&[u8]> {
        self.layout.payload(self.sample.payload()).ok()
    }
}

impl UEncodedLoanedRxFrame for Iceoryx2RxLease {
    fn loaned_contiguous_payload(&self) -> Result<LoanedPayload<'_>, up_rust::UWireError> {
        let payload = self
            .try_contiguous_payload()
            .ok_or_else(|| up_rust::UWireError::invalid_payload("payload is not contiguous"))?;
        if payload.is_empty() {
            return Err(up_rust::UWireError::MissingPayload);
        }
        // SAFETY: the slice is borrowed directly from the live iceoryx2 sample;
        // no allocation, copy, or coalescing is performed.
        Ok(unsafe {
            LoanedPayload::new_unchecked(payload, PayloadLoanProvenance::OpaqueTransportLoan)
        })
    }
}

#[async_trait]
impl UZeroCopyTransportCore for Iceoryx2PubSub {
    type Tx = Iceoryx2TxLoan;
    type Rx = Iceoryx2RxLease;

    async fn loan_prepared_tx(&self, spec: PreparedTxLoanSpec) -> Result<Self::Tx, UStatus> {
        let payload_alignment = spec.payload_alignment_proof();
        let (metadata, encoded_metadata, payload_len, _) = spec.into_parts();
        let source = metadata.source();
        let service_name =
            compute_service_name(source, metadata.sink(), MessagingPattern::PublishSubscribe)?;
        let publisher = self
            .inner
            .get_or_create_publisher(service_name, source)
            .await?;
        notify_broad_receivers(&service_name, source).await?;
        let alignment = payload_alignment.as_usize();
        let sample_len =
            worst_case_aligned_sample_len(encoded_metadata.len(), payload_len, alignment)?;
        let mut sample = publisher
            .loan_slice(sample_len)
            .map_err(|error| map_loan_error(error, "loan sample"))?;
        let payload_offset = aligned_payload_offset(
            sample.payload().as_ptr() as usize,
            encoded_metadata.len(),
            alignment,
        )?;
        let layout = Iceoryx2PayloadLayout::from_validated_parts(
            encoded_metadata.len(),
            payload_offset,
            payload_len,
            alignment,
            sample.payload().len(),
        )
        .map_err(frame_contract_error_to_status)?;
        sample
            .payload_mut()
            .get_mut(..encoded_metadata.len())
            .ok_or_else(|| UStatus::fail_with_code(UCode::Internal, "metadata range missing"))?
            .copy_from_slice(&encoded_metadata);
        sample
            .user_header_mut()
            .write_payload_layout(layout)
            .map_err(frame_contract_error_to_status)?;
        Ok(Iceoryx2TxLoan {
            metadata,
            sample,
            payload_offset,
            payload_len,
            encoded_metadata_len: encoded_metadata.len(),
        })
    }

    async fn send_prepared_zero_copy(&self, buffer: Self::Tx) -> Result<(), UStatus> {
        buffer.sample.send().map_err(|error| {
            UStatus::fail_with_code(UCode::Internal, format!("failed to send sample: {error}"))
        })?;
        Ok(())
    }

    async fn receive_encoded_zero_copy(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
    ) -> Result<Self::Rx, UStatus> {
        let exact_source_filter = source_filter.verify_no_wildcards().is_ok();
        let service_names = if exact_source_filter {
            vec![compute_service_name(
                source_filter,
                sink_filter,
                MessagingPattern::PublishSubscribe,
            )?]
        } else {
            self.inner
                .register_pending_pull_source_filter(source_filter)
                .await;
            register_broad_receiver(&self.inner);
            self.inner
                .discover_matching_service_names(source_filter)?
                .into_iter()
                .map(|service_name| {
                    ServiceName::new(service_name.as_str()).map_err(|error| {
                        UStatus::fail_with_code(
                            UCode::Internal,
                            format!(
                                "discovered invalid iceoryx2 service name {service_name}: {error}"
                            ),
                        )
                    })
                })
                .collect::<Result<Vec<_>, _>>()?
        };

        for service_name in service_names {
            if let Some(lease) = self
                .inner
                .pop_queued_pull_sample(&service_name, sink_filter)
                .await
            {
                return Ok(lease);
            }

            let subscriber = self
                .inner
                .get_or_create_pull_subscriber(
                    service_name,
                    exact_source_filter.then_some(source_filter),
                )
                .await?;
            while let Some(sample) = subscriber
                .receive()
                .map_err(|error| UStatus::fail_with_code(UCode::Internal, error.to_string()))?
            {
                let lease = lease_from_sample(sample, source_filter.clone(), sink_filter.cloned())?;
                if !sink_matches(lease.sink_filter_hint(), sink_filter) {
                    self.inner.queue_pull_sample(service_name, lease).await?;
                    continue;
                }
                return Ok(lease);
            }
        }

        Err(UStatus::fail_with_code(
            UCode::NotFound,
            "no sample available",
        ))
    }

    async fn register_encoded_zero_copy_listener(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UEncodedZeroCopyListener<Self::Rx>>,
    ) -> Result<(), UStatus> {
        let exact_source_filter = source_filter.verify_no_wildcards().is_ok();
        let mut listeners = self.inner.zero_copy_listeners.write().await;
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
        if exact_source_filter {
            let service_name = compute_service_name(
                source_filter,
                sink_filter,
                MessagingPattern::PublishSubscribe,
            )?;
            let subscriber = self
                .inner
                .create_subscriber(service_name, Some(source_filter))?;
            registration
                .subscribers
                .insert(service_name, Arc::new(subscriber));
        } else {
            register_broad_receiver(&self.inner);
        }
        listeners.push(registration);
        Ok(())
    }

    async fn unregister_encoded_zero_copy_listener(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UEncodedZeroCopyListener<Self::Rx>>,
    ) -> Result<(), UStatus> {
        let mut listeners = self.inner.zero_copy_listeners.write().await;
        let Some(index) = listeners.iter().position(|registration| {
            registration.has_same_identity(source_filter, sink_filter, &listener)
        }) else {
            return Err(UStatus::fail_with_code(
                UCode::NotFound,
                "zero-copy listener not registered for filter",
            ));
        };
        listeners.remove(index);
        Ok(())
    }
}

#[async_trait]
impl UZeroCopyUninitTransportCore for Iceoryx2PubSub {
    type UninitTx = Iceoryx2UninitTxLoan;

    async fn loan_prepared_uninit_tx(
        &self,
        spec: PreparedTxLoanSpec,
    ) -> Result<Self::UninitTx, UStatus> {
        let payload_alignment = spec.payload_alignment_proof();
        let (metadata, encoded_metadata, payload_len, _) = spec.into_parts();
        let source = metadata.source();
        let service_name =
            compute_service_name(source, metadata.sink(), MessagingPattern::PublishSubscribe)?;
        let publisher = self
            .inner
            .get_or_create_publisher(service_name, source)
            .await?;
        notify_broad_receivers(&service_name, source).await?;
        let alignment = payload_alignment.as_usize();
        let sample_len =
            worst_case_aligned_sample_len(encoded_metadata.len(), payload_len, alignment)?;
        let mut sample = publisher
            .loan_slice_uninit(sample_len)
            .map_err(|error| map_loan_error(error, "loan uninitialized sample"))?;
        let payload_offset = aligned_payload_offset(
            sample.payload_mut().as_ptr() as usize,
            encoded_metadata.len(),
            alignment,
        )?;
        let layout = Iceoryx2PayloadLayout::from_validated_parts(
            encoded_metadata.len(),
            payload_offset,
            payload_len,
            alignment,
            sample.payload_mut().len(),
        )
        .map_err(frame_contract_error_to_status)?;
        write_uninit_bytes(sample.payload_mut(), 0, &encoded_metadata)?;
        initialize_uninit_range(sample.payload_mut(), encoded_metadata.len(), payload_offset)?;
        sample
            .user_header_mut()
            .write_payload_layout(layout)
            .map_err(frame_contract_error_to_status)?;
        Ok(Iceoryx2UninitTxLoan {
            metadata,
            sample,
            payload_offset,
            payload_len,
        })
    }
}

fn lease_from_sample(
    sample: IpcSample,
    source_filter_hint: UUri,
    sink_filter_hint: Option<UUri>,
) -> Result<Iceoryx2RxLease, UStatus> {
    let layout = sample
        .user_header()
        .payload_layout(sample.payload().len())
        .map_err(frame_contract_error_to_status)?;
    validate_payload_address_alignment(
        sample.payload(),
        layout.payload_range().start,
        layout.payload_alignment(),
    )?;
    Ok(Iceoryx2RxLease {
        sample,
        layout,
        source_filter_hint,
        sink_filter_hint,
    })
}

fn map_loan_error(error: LoanError, operation: &str) -> UStatus {
    let code = match error {
        LoanError::OutOfMemory | LoanError::ExceedsMaxLoans | LoanError::ExceedsMaxLoanSize => {
            UCode::ResourceExhausted
        }
        LoanError::InternalFailure => UCode::Internal,
    };
    UStatus::fail_with_code(code, format!("failed to {operation}: {error}"))
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

fn validate_payload_address_alignment(
    sample_payload: &[u8],
    payload_offset: usize,
    alignment: usize,
) -> Result<(), UStatus> {
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
