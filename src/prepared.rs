// Copyright (c) 2026 Contributors to the Eclipse Foundation
// SPDX-License-Identifier: Apache-2.0

//! Setup-reserved native services, synchronous transmit and event-driven delivery.

use crate::service_attributes::source_attribute_verifier;
use crate::service_name_mapping::compute_service_name;
use crate::{Iceoryx2PayloadLayout, Iceoryx2PubSubConfig, UProtocolHeader};
use iceoryx2::port::update_connections::UpdateConnections;
use iceoryx2::service::port_factory::PortFactory as _;
use iceoryx2::{
    port::{listener::Listener, notifier::Notifier, publisher::Publisher, subscriber::Subscriber},
    prelude::{
        AllocationStrategy, Config, FileDescriptorBased, MessagingPattern, Node, NodeBuilder,
        ServiceName, ipc_threadsafe,
    },
    sample::Sample,
    sample_mut_uninit::SampleMutUninit,
};
use iceoryx2_bb_posix::file_descriptor_set::{FileDescriptorSet, FileEvent};
use std::{
    mem::MaybeUninit,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread::JoinHandle,
    time::Duration,
};
use up_rust::{
    Frame, FrameSink, InitializedTx, LoanError, LoanedPayload, PayloadLoanProvenance,
    PreparedTransportCore, PreparedTxStorage, RouteLimits, SendError, SetupError, TxFrameLayout,
    UCode, UStatus, UUri, WrittenHeader,
};

type NativeService = iceoryx2::service::port_factory::publish_subscribe::PortFactory<
    ipc_threadsafe::Service,
    [u8],
    UProtocolHeader,
>;
type NativePublisher = Publisher<ipc_threadsafe::Service, [u8], UProtocolHeader>;
type NativeSubscriber = Subscriber<ipc_threadsafe::Service, [u8], UProtocolHeader>;
type NativeSample = Sample<ipc_threadsafe::Service, [u8], UProtocolHeader>;
type NativeTx = SampleMutUninit<ipc_threadsafe::Service, [MaybeUninit<u8>], UProtocolHeader>;
type NativeListener = Listener<ipc_threadsafe::Service>;
type NativeNotifier = Notifier<ipc_threadsafe::Service>;

/// Physical addressing and storage reserved before prepared message admission.
///
/// Include response destinations before starting an RPC server. Its first request
/// from each client still creates the SDK cache entry inside the measured window;
/// resolving the already reserved native route does not create a publisher there.
#[derive(Clone, Debug)]
pub struct Iceoryx2RouteReservation {
    /// Exact source address carried by this route.
    pub source: UUri,
    /// Exact destination, or None for sinkless publications.
    pub sink: Option<UUri>,
    /// Largest metadata/payload layout admitted by this physical reservation.
    pub limits: RouteLimits,
}

struct Endpoint {
    definition: Iceoryx2RouteReservation,
    name: ServiceName,
    publisher: Option<Arc<NativePublisher>>,
    notifier: Arc<NativeNotifier>,
    sample_len: usize,
    // Own the service even when no publisher exists to retain its native metadata.
    _service: NativeService,
}

struct Inner {
    endpoints: Box<[Endpoint]>,
    routes: Box<[AtomicBool]>,
    registrations: Box<[AtomicBool]>,
    config: Iceoryx2PubSubConfig,
    stats: Iceoryx2PreparedStats,
    node: Arc<Node<ipc_threadsafe::Service>>,
}

/// Native prepared-path counters, independent of SDK/application inbox admission.
#[derive(Debug, Default)]
pub struct Iceoryx2PreparedStats {
    /// Samples successfully published, before notification or application admission.
    pub published: AtomicU64,
    /// Physical publication errors after a checked initialized loan was supplied.
    pub publish_failed: AtomicU64,
    /// Native event notification errors; bounded data polling still runs.
    pub notification_failed: AtomicU64,
    /// Received native samples, including those rejected for physical layout.
    pub received: AtomicU64,
    /// Samples rejected before push for malformed placement or address alignment.
    pub rejected_layout: AtomicU64,
    /// Native receive errors; retained-credit exhaustion is reported separately.
    pub receive_failed: AtomicU64,
    /// Native receive attempts blocked by the configured borrowed-sample ceiling.
    pub borrowed_capacity: AtomicU64,
}

/// Real native prepared core with a finite physical route catalog.
///
/// Native pools, publishers and notifiers are created by [`Self::new`]. Route
/// handles use reserved table entries; attach reserves an independent native
/// subscriber per matching service and a single delivery thread. The SDK owns
/// the generic asynchronous application worker. No per-message task is spawned.
#[derive(Clone)]
pub struct Iceoryx2PreparedCore {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for Iceoryx2PreparedCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Iceoryx2PreparedCore")
            .field("physical_routes", &self.inner.endpoints.len())
            .finish_non_exhaustive()
    }
}

fn setup_error(message: &'static str) -> UStatus {
    UStatus::fail_with_code(UCode::FailedPrecondition, message)
}

fn frame_capacity(limits: RouteLimits) -> Option<usize> {
    if limits.max_metadata_len == 0 || !limits.payload_alignment.is_power_of_two() {
        return None;
    }
    limits
        .max_metadata_len
        .checked_add(limits.payload_alignment - 1)?
        .checked_add(limits.max_payload_len)
}

fn table(capacity: usize) -> Result<Box<[AtomicBool]>, UStatus> {
    if capacity == 0 {
        return Err(setup_error("prepared table capacity must be positive"));
    }
    let mut slots = Vec::new();
    slots
        .try_reserve_exact(capacity)
        .map_err(|_| setup_error("prepared table allocation failed"))?;
    slots.resize_with(capacity, || AtomicBool::new(false));
    Ok(slots.into_boxed_slice())
}

fn reserve(slots: &[AtomicBool]) -> Result<usize, SetupError> {
    slots
        .iter()
        .position(|slot| {
            slot.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        })
        .ok_or(SetupError::TableFull)
}

fn verify_limits(
    config: &Iceoryx2PubSubConfig,
    actual: &iceoryx2::service::static_config::publish_subscribe::StaticConfig,
    native: &Config,
) -> Result<(), UStatus> {
    if config.require_exact_service_limits {
        let expected = &native.defaults.publish_subscribe;
        if actual.max_subscribers() != expected.max_subscribers
            || actual.subscriber_max_buffer_size() != expected.subscriber_max_buffer_size
            || actual.subscriber_max_borrowed_samples() != expected.subscriber_max_borrowed_samples
            || actual.history_size() != expected.publisher_history_size
            || actual.has_safe_overflow() != expected.enable_safe_overflow
        {
            return Err(setup_error(
                "existing native service has incompatible exact capacities",
            ));
        }
    }
    Ok(())
}

impl Iceoryx2PreparedCore {
    /// Creates every physical publisher/data segment and both finite handle tables.
    ///
    /// The catalog is closed: new logical response cache entries can resolve only
    /// predeclared physical destinations. Static slice capacity must fit each
    /// reservation; it is never silently enlarged. Configuration retains the
    /// existing native subscriber/buffer/borrow/history/TX limits and namespace.
    /// Publisher readiness is checked by [`Self::wait_ready`] during setup.
    /// Services carry the same immutable discovery attributes as owned controls.
    /// Method-addressed requests use a shared-request marker rather than freezing
    /// one client's source; decoded SDK filters remain authoritative.
    ///
    /// # Errors
    /// Rejects invalid/duplicate addresses, dynamic allocation, undersized slices,
    /// arithmetic overflow, incompatible services or native resource exhaustion.
    ///
    /// ```no_run
    /// use up_rust::{RouteLimits, UUri};
    /// use up_transport_iceoryx2_rust::{Iceoryx2PreparedCore, Iceoryx2PubSubConfig,
    ///     Iceoryx2RouteReservation};
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let source = UUri::try_from_parts("sensor", 0x4210, 1, 0x9000)?;
    /// let config = Iceoryx2PubSubConfig::static_allocation(4096);
    /// let core = Iceoryx2PreparedCore::new(config, &[Iceoryx2RouteReservation {
    ///     source, sink: None,
    ///     limits: RouteLimits { max_metadata_len: 2048, max_payload_len: 1024,
    ///         payload_alignment: 8 },
    /// }], 8, 2)?;
    /// # let _ = core;
    /// # Ok(()) }
    /// ```
    pub fn new(
        config: Iceoryx2PubSubConfig,
        catalog: &[Iceoryx2RouteReservation],
        route_capacity: usize,
        registration_capacity: usize,
    ) -> Result<Self, UStatus> {
        Self::new_masked(config, catalog, None, route_capacity, registration_capacity)
    }

    /// Reserves physical services with an explicit per-address TX capability.
    ///
    /// A false mask entry permits native receive/attach but creates no publisher
    /// or data segment and rejects TX reservation. True entries eagerly reserve
    /// the same static pools and limits as [`Self::new`]. Call during setup; no
    /// publisher is ever created by loan, publish or the receive callback.
    /// The core retains the native service descriptor for its entire lifetime,
    /// allowing receive attachment before any remote publisher is created.
    ///
    /// # Errors
    /// Rejects a mask length differing from the catalog and all invalid setup
    /// accepted by neither this constructor nor [`Self::new`].
    pub fn new_with_transmit_mask(
        config: Iceoryx2PubSubConfig,
        catalog: &[Iceoryx2RouteReservation],
        transmit: &[bool],
        route_capacity: usize,
        registration_capacity: usize,
    ) -> Result<Self, UStatus> {
        if transmit.len() != catalog.len() {
            return Err(setup_error("transmit mask must match catalog"));
        }
        Self::new_masked(
            config,
            catalog,
            Some(transmit),
            route_capacity,
            registration_capacity,
        )
    }

    fn new_masked(
        config: Iceoryx2PubSubConfig,
        catalog: &[Iceoryx2RouteReservation],
        transmit: Option<&[bool]>,
        route_capacity: usize,
        registration_capacity: usize,
    ) -> Result<Self, UStatus> {
        config.validate()?;
        if config.publisher_allocation_strategy != AllocationStrategy::Static || catalog.is_empty()
        {
            return Err(setup_error(
                "prepared core requires a nonempty catalog and static native allocation",
            ));
        }
        let routes = table(route_capacity)?;
        let registrations = table(registration_capacity)?;
        let native = config.iceoryx2_config.clone().unwrap_or_default();
        let node = Arc::new(
            NodeBuilder::new()
                .config(&native)
                .create::<ipc_threadsafe::Service>()
                .map_err(|_| setup_error("native node creation failed"))?,
        );
        let mut endpoints: Vec<Endpoint> = Vec::new();
        endpoints
            .try_reserve_exact(catalog.len())
            .map_err(|_| setup_error("catalog allocation failed"))?;
        for (index, definition) in catalog.iter().enumerate() {
            definition
                .source
                .verify_no_wildcards()
                .map_err(|_| setup_error("catalog source must be exact"))?;
            if let Some(sink) = &definition.sink {
                sink.verify_no_wildcards()
                    .map_err(|_| setup_error("catalog sink must be exact"))?;
            }
            if endpoints.iter().any(|entry| {
                entry.definition.source == definition.source
                    && entry.definition.sink == definition.sink
            }) {
                return Err(setup_error("duplicate prepared physical route"));
            }
            let sample_len = frame_capacity(definition.limits)
                .ok_or_else(|| setup_error("invalid prepared frame capacity"))?;
            if sample_len > config.publisher_initial_max_slice_len {
                return Err(setup_error(
                    "prepared frame exceeds fixed native slice capacity",
                ));
            }
            let name = compute_service_name(
                &definition.source,
                definition.sink.as_ref(),
                MessagingPattern::PublishSubscribe,
            )?;
            let service = node
                .service_builder(&name)
                .publish_subscribe::<[u8]>()
                .user_header::<UProtocolHeader>()
                .open_or_create_with_attributes(&source_attribute_verifier(&definition.source)?)
                .map_err(|_| setup_error("native prepared service cannot be opened"))?;
            verify_limits(&config, service.static_config(), &native)?;
            let publisher = if transmit.is_none_or(|mask| mask[index]) {
                if let Some(previous) = endpoints
                    .iter()
                    .find(|entry| entry.name == name && entry.publisher.is_some())
                {
                    previous.publisher.clone()
                } else {
                    let publisher = service
                        .publisher_builder()
                        .initial_max_slice_len(config.publisher_initial_max_slice_len)
                        .allocation_strategy(AllocationStrategy::Static)
                        .create()
                        .map_err(|_| setup_error("native prepared publisher cannot be reserved"))?;
                    let initial = publisher
                        .loan_slice_uninit(config.publisher_initial_max_slice_len)
                        .map_err(|_| setup_error("native static segment cannot be materialized"))?;
                    if config.publisher_prewarm {
                        crate::pool_prewarm::preallocate(initial.payload().as_ptr() as usize)?;
                    }
                    drop(initial);
                    Some(Arc::new(publisher))
                }
            } else {
                None
            };
            let notifier = if let Some(previous) = endpoints.iter().find(|entry| entry.name == name)
            {
                Arc::clone(&previous.notifier)
            } else {
                let event = node
                    .service_builder(&name)
                    .event()
                    .open_or_create()
                    .map_err(|_| setup_error("native prepared event cannot be opened"))?;
                Arc::new(
                    event
                        .notifier_builder()
                        .create()
                        .map_err(|_| setup_error("native event notifier cannot be reserved"))?,
                )
            };
            endpoints.push(Endpoint {
                definition: definition.clone(),
                name,
                publisher,
                notifier,
                sample_len,
                _service: service,
            });
        }
        Ok(Self {
            inner: Arc::new(Inner {
                endpoints: endpoints.into_boxed_slice(),
                routes,
                registrations,
                config,
                stats: Iceoryx2PreparedStats::default(),
                node,
            }),
        })
    }

    /// Borrows physical counters without conflating native failure with app drops.
    #[must_use]
    pub fn stats(&self) -> &Iceoryx2PreparedStats {
        &self.inner.stats
    }

    /// Resolves current native peer connections before enabling measured traffic.
    ///
    /// Call after receiver registrations are attached. This does not send an
    /// application sample or populate an SDK RPC client's first-contact cache.
    ///
    /// # Errors
    /// Returns native connection errors or the configured bounded readiness timeout.
    pub async fn wait_ready(&self) -> Result<(), UStatus> {
        let deadline = tokio::time::Instant::now()
            .checked_add(self.inner.config.publisher_readiness_timeout)
            .ok_or_else(|| setup_error("publisher readiness deadline overflows"))?;
        loop {
            let mut ready = true;
            for endpoint in &self.inner.endpoints {
                let Some(publisher) = &endpoint.publisher else {
                    continue;
                };
                publisher
                    .update_connections()
                    .map_err(|_| setup_error("native publisher connection failed"))?;
                endpoint
                    .notifier
                    .update_connections()
                    .map_err(|_| setup_error("native notifier connection failed"))?;
                let service = self
                    .inner
                    .node
                    .service_builder(&endpoint.name)
                    .publish_subscribe::<[u8]>()
                    .user_header::<UProtocolHeader>()
                    .open()
                    .map_err(|_| setup_error("prepared service disappeared"))?;
                ready &= service.dynamic_config().number_of_subscribers()
                    >= self.inner.config.publisher_min_subscribers;
            }
            if ready {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(UStatus::fail_with_code(
                    UCode::DeadlineExceeded,
                    "prepared subscriber readiness timed out",
                ));
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

/// Instance-bound logical handle backed by an already reserved native publisher.
pub struct Iceoryx2PreparedRoute {
    inner: Arc<Inner>,
    endpoint: usize,
    slot: usize,
    limits: RouteLimits,
}

impl Drop for Iceoryx2PreparedRoute {
    fn drop(&mut self) {
        self.inner.routes[self.slot].store(false, Ordering::Release);
    }
}

/// Exclusive native uninitialized loan with immutable, disjoint SDK byte extents.
pub struct Iceoryx2PreparedTx<'a> {
    route: &'a Iceoryx2PreparedRoute,
    sample: Option<NativeTx>,
    layout: Iceoryx2PayloadLayout,
    present: bool,
    written: Option<WrittenHeader>,
}

// SAFETY: one unpublished native sample owns both ranges exclusively. Layout is
// checked before construction; accessors neither move nor resize the allocation.
// The route borrow retains its publisher/core until the loan is consumed.
unsafe impl PreparedTxStorage for Iceoryx2PreparedTx<'_> {
    fn metadata_uninit_mut(&mut self) -> &mut [MaybeUninit<u8>] {
        &mut self
            .sample
            .as_mut()
            .expect("live native loan")
            .payload_mut()[..self.layout.metadata_len()]
    }
    fn payload_uninit_mut(&mut self) -> Option<&mut [MaybeUninit<u8>]> {
        self.present.then(|| {
            &mut self
                .sample
                .as_mut()
                .expect("live native loan")
                .payload_mut()[self.layout.payload_range()]
        })
    }
    fn record_header(&mut self, header: WrittenHeader) -> Result<(), SendError> {
        if header.len != self.layout.metadata_len()
            || header.token.is_some_and(|(offset, len)| {
                offset.checked_add(len).is_none_or(|end| end > header.len)
            })
        {
            return Err(SendError::PayloadStorage);
        }
        self.written = Some(header);
        Ok(())
    }
}

impl Drop for Iceoryx2PreparedTx<'_> {
    fn drop(&mut self) {
        if let Some(sample) = &mut self.sample {
            // Only unpublished storage is ours to wipe. Published native slots
            // have no last-reader/reclaim callback and cannot be wiped here.
            for byte in &mut sample.payload_mut()[..self.layout.metadata_len()] {
                byte.write(0);
            }
        }
    }
}

/// Immutable native sample retaining its actual subscriber after detach/core drop.
pub struct Iceoryx2PreparedRx {
    // Declaration order releases the native sample before the subscriber port.
    sample: NativeSample,
    _subscriber: Arc<NativeSubscriber>,
    // Keep the native node's lifecycle handle until the sample and port have
    // released their tags; retaining only SharedNodeState permits early cleanup.
    _node: Arc<Node<ipc_threadsafe::Service>>,
    layout: Iceoryx2PayloadLayout,
}

impl Frame for Iceoryx2PreparedRx {
    fn encoded_metadata(&self) -> &[u8] {
        &self.sample.payload()[..self.layout.metadata_len()]
    }
    fn payload(&self) -> Option<LoanedPayload<'_>> {
        let metadata = self.encoded_metadata();
        if !crate::uprotocolheader::payload_present(metadata)? {
            return None;
        }
        Some(LoanedPayload::new(
            &self.sample.payload()[self.layout.payload_range()],
            PayloadLoanProvenance::OpaqueTransportLoan,
        ))
    }
}

struct Receiver {
    subscriber: Arc<NativeSubscriber>,
    listener: NativeListener,
}

/// Exact native registration handle. Drop closes admission and stops its pump.
///
/// Independently retained frames keep their own subscriber ports alive. Detach
/// never revokes their storage; SDK worker drain/cancellation is a separate step.
pub struct Iceoryx2PreparedSubscription {
    inner: Arc<Inner>,
    stop: Arc<AtomicBool>,
    sink: Arc<dyn FrameSink<Iceoryx2PreparedRx>>,
    pump: Option<JoinHandle<()>>,
}

impl Iceoryx2PreparedSubscription {
    fn halt(&mut self) {
        self.sink.close();
        self.stop.store(true, Ordering::Release);
        if let Some(pump) = self.pump.take()
            && pump.thread().id() != std::thread::current().id()
        {
            let _ = pump.join();
        }
    }
}

impl Drop for Iceoryx2PreparedSubscription {
    fn drop(&mut self) {
        self.halt();
    }
}

impl PreparedTransportCore for Iceoryx2PreparedCore {
    type Route = Iceoryx2PreparedRoute;
    type Tx<'a> = Iceoryx2PreparedTx<'a>;
    type Rx = Iceoryx2PreparedRx;
    type Subscription = Iceoryx2PreparedSubscription;

    fn prepare_route(
        &self,
        source: &UUri,
        sink: Option<&UUri>,
        limits: RouteLimits,
    ) -> Result<Self::Route, SetupError> {
        let endpoint = self
            .inner
            .endpoints
            .iter()
            .position(|entry| {
                &entry.definition.source == source && entry.definition.sink.as_ref() == sink
            })
            .ok_or(SetupError::InvalidRoute("physical route was not reserved"))?;
        if self.inner.endpoints[endpoint].publisher.is_none() {
            return Err(SetupError::InvalidRoute("receiver-only native address"));
        }
        let reserved = self.inner.endpoints[endpoint].definition.limits;
        if frame_capacity(limits).is_none()
            || limits.max_metadata_len > reserved.max_metadata_len
            || limits.max_payload_len > reserved.max_payload_len
            || limits.payload_alignment > reserved.payload_alignment
        {
            return Err(SetupError::InvalidConfig(
                "route exceeds physical reservation",
            ));
        }
        let slot = reserve(&self.inner.routes)?;
        Ok(Iceoryx2PreparedRoute {
            inner: Arc::clone(&self.inner),
            endpoint,
            slot,
            limits,
        })
    }

    fn loan<'a>(
        &'a self,
        route: &'a Self::Route,
        layout: TxFrameLayout,
    ) -> Result<Self::Tx<'a>, LoanError> {
        if !Arc::ptr_eq(&self.inner, &route.inner) {
            return Err(LoanError::InvalidRoute);
        }
        if layout.metadata_len > route.limits.max_metadata_len
            || layout.payload_len.unwrap_or(0) > route.limits.max_payload_len
            || !layout.payload_alignment.is_power_of_two()
            || layout.payload_alignment > route.limits.payload_alignment
        {
            return Err(LoanError::SlotTooSmall);
        }
        let endpoint = &self.inner.endpoints[route.endpoint];
        let sample_len = frame_capacity(route.limits).ok_or(LoanError::SlotTooSmall)?;
        if sample_len > endpoint.sample_len {
            return Err(LoanError::SlotTooSmall);
        }
        let mut sample = endpoint
            .publisher
            .as_ref()
            .ok_or(LoanError::InvalidRoute)?
            .loan_slice_uninit(sample_len)
            .map_err(|error| match error {
                iceoryx2::port::LoanError::ExceedsMaxLoanSize => LoanError::SlotTooSmall,
                _ => LoanError::PoolExhausted,
            })?;
        let base = sample.payload().as_ptr() as usize;
        let start = base
            .checked_add(layout.metadata_len)
            .ok_or(LoanError::SlotTooSmall)?;
        let padding = (layout.payload_alignment - (start & (layout.payload_alignment - 1)))
            & (layout.payload_alignment - 1);
        let offset = layout
            .metadata_len
            .checked_add(padding)
            .ok_or(LoanError::SlotTooSmall)?;
        let physical = Iceoryx2PayloadLayout::from_validated_parts(
            layout.metadata_len,
            offset,
            layout.payload_len.unwrap_or(0),
            layout.payload_alignment,
            sample_len,
        )
        .map_err(|_| LoanError::SlotTooSmall)?;
        sample
            .user_header_mut()
            .write_payload_layout(physical)
            .map_err(|_| LoanError::SlotTooSmall)?;
        for byte in &mut sample.payload_mut()[layout.metadata_len..offset] {
            byte.write(0);
        }
        for byte in &mut sample.payload_mut()[physical.payload_range().end..] {
            byte.write(0);
        }
        Ok(Iceoryx2PreparedTx {
            route,
            sample: Some(sample),
            layout: physical,
            present: layout.payload_len.is_some(),
            written: None,
        })
    }

    fn publish<'a>(
        &'a self,
        route: &'a Self::Route,
        frame: InitializedTx<Self::Tx<'a>>,
    ) -> Result<(), SendError> {
        let mut tx = frame.into_storage();
        if !Arc::ptr_eq(&self.inner, &route.inner)
            || !std::ptr::eq(tx.route, route)
            || tx.written.is_none()
        {
            return Err(SendError::PayloadStorage);
        }
        let sample = tx.sample.take().ok_or(SendError::PayloadStorage)?;
        // SAFETY: InitializedTx proves both SDK views initialized and record_header
        // verified their exact metadata extent. loan initialized native framing and
        // every gap/tail byte; the route borrow pins the exclusive native sample.
        let sample = unsafe { sample.assume_init() };
        sample.send().map_err(|_| {
            self.inner
                .stats
                .publish_failed
                .fetch_add(1, Ordering::Relaxed);
            SendError::Delivery
        })?;
        self.inner.stats.published.fetch_add(1, Ordering::Relaxed);
        // Publication already succeeded. A lost notification cannot roll it back:
        // the bounded event wait also checks native data on its timeout.
        if self.inner.endpoints[route.endpoint]
            .notifier
            .notify()
            .is_err()
        {
            self.inner
                .stats
                .notification_failed
                .fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    async fn attach(
        &self,
        source: &UUri,
        sink: Option<&UUri>,
        inbox: Arc<dyn FrameSink<Self::Rx>>,
    ) -> Result<Self::Subscription, SetupError> {
        let slot = reserve(&self.inner.registrations)?;
        let stop = Arc::new(AtomicBool::new(false));
        let mut subscription = Iceoryx2PreparedSubscription {
            inner: Arc::clone(&self.inner),
            stop: Arc::clone(&stop),
            sink: Arc::clone(&inbox),
            pump: None,
        };
        let setup = (|| {
            let mut names = Vec::new();
            let mut receivers = Vec::new();
            for endpoint in &self.inner.endpoints {
                let matches_sink = match (sink, endpoint.definition.sink.as_ref()) {
                    (None, None) => true,
                    (Some(filter), Some(actual)) => filter.matches(actual),
                    _ => false,
                };
                if !source.matches(&endpoint.definition.source)
                    || !matches_sink
                    || names.contains(&endpoint.name)
                {
                    continue;
                }
                let service = self
                    .inner
                    .node
                    .service_builder(&endpoint.name)
                    .publish_subscribe::<[u8]>()
                    .user_header::<UProtocolHeader>()
                    .open()
                    .map_err(|_| SetupError::InvalidConfig("native service unavailable"))?;
                let subscriber = Arc::new(service.subscriber_builder().create().map_err(|error| match error {
                    iceoryx2::port::subscriber::SubscriberCreateError::ExceedsMaxSupportedSubscribers => SetupError::TableFull,
                    iceoryx2::port::subscriber::SubscriberCreateError::BufferSizeExceedsMaxSupportedBufferSizeOfService => SetupError::InvalidConfig("native subscriber buffer exceeds service capacity"),
                    _ => SetupError::InvalidConfig("native subscriber resource creation failed"),
                })?);
                subscriber.update_connections().map_err(|_| {
                    SetupError::InvalidConfig("native subscriber connection failed")
                })?;
                let event = self
                    .inner
                    .node
                    .service_builder(&endpoint.name)
                    .event()
                    .open_or_create()
                    .map_err(|_| SetupError::InvalidConfig("native event unavailable"))?;
                let listener = event.listener_builder().create().map_err(|error| match error {
                    iceoryx2::port::listener::ListenerCreateError::ExceedsMaxSupportedListeners => SetupError::TableFull,
                    _ => SetupError::InvalidConfig("native event listener resource creation failed"),
                })?;
                names.push(endpoint.name);
                receivers.push(Receiver {
                    subscriber,
                    listener,
                });
            }
            if receivers.is_empty() {
                return Err(SetupError::InvalidRoute(
                    "no reserved physical service matches filters",
                ));
            }
            Ok(receivers)
        })();
        let receivers = match setup {
            Ok(receivers) => receivers,
            Err(error) => {
                self.inner.registrations[slot].store(false, Ordering::Release);
                return Err(error);
            }
        };
        let inner = Arc::clone(&self.inner);
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
        let pump = std::thread::Builder::new()
            .name("iceoryx2-delivery".into())
            .spawn(move || {
                // The native WaitSet's dynamic event/deadline Vecs allocate on every
                // wake in 0.9.2. This fixed native descriptor wait set reserves its
                // attachments once and invokes readiness callbacks without collecting.
                let waitset = FileDescriptorSet::new();
                let guards = match receivers
                    .iter()
                    .map(|receiver| {
                        // SAFETY: the listener owns this live descriptor throughout this
                        // pump. Inspecting its number does not take ownership or close it.
                        let fd = unsafe { receiver.listener.file_descriptor().native_handle() };
                        if fd < 0 || fd as usize >= FileDescriptorSet::capacity() {
                            return Err(SetupError::Worker);
                        }
                        waitset
                            .add(&receiver.listener)
                            .map_err(|_| SetupError::Worker)
                    })
                    .collect::<Result<Vec<_>, _>>()
                {
                    Ok(guards) => guards,
                    Err(error) => {
                        let _ = ready_tx.send(Err(error));
                        inner.registrations[slot].store(false, Ordering::Release);
                        return;
                    }
                };
                let _ = ready_tx.send(Ok(()));
                drop(ready_tx);
                while !stop.load(Ordering::Acquire) {
                    for receiver in &receivers {
                        while !stop.load(Ordering::Acquire) {
                            // A continuously nonempty data queue must not starve
                            // its wake channel. Drain hints before each sample,
                            // including before borrowed-credit retry, rather than
                            // only once per outer idle/wait cycle.
                            let _ = receiver.listener.try_wait_all(|_| {});
                            let sample = match receiver.subscriber.receive() {
                                Ok(Some(sample)) => sample,
                                Ok(None) => break,
                                Err(iceoryx2::port::ReceiveError::ExceedsMaxBorrows) => {
                                    inner
                                        .stats
                                        .borrowed_capacity
                                        .fetch_add(1, Ordering::Relaxed);
                                    break;
                                }
                                Err(_) => {
                                    inner.stats.receive_failed.fetch_add(1, Ordering::Relaxed);
                                    break;
                                }
                            };
                            inner.stats.received.fetch_add(1, Ordering::Relaxed);
                            let layout =
                                match sample.user_header().payload_layout(sample.payload().len()) {
                                    Ok(layout) => layout,
                                    Err(_) => {
                                        inner.stats.rejected_layout.fetch_add(1, Ordering::Relaxed);
                                        continue;
                                    }
                                };
                            let address = (sample.payload().as_ptr() as usize)
                                .checked_add(layout.payload_range().start);
                            if address.is_none_or(|address| {
                                address & (layout.payload_alignment() - 1) != 0
                            }) {
                                inner.stats.rejected_layout.fetch_add(1, Ordering::Relaxed);
                                continue;
                            }
                            // Exactly one transient native frame is held here. No
                            // callback/queue/worker staging credit is added around push.
                            inbox.push(Iceoryx2PreparedRx {
                                sample,
                                _subscriber: Arc::clone(&receiver.subscriber),
                                _node: Arc::clone(&inner.node),
                                layout,
                            });
                        }
                    }
                    if !stop.load(Ordering::Acquire) {
                        let _ =
                            waitset.timed_wait(Duration::from_millis(10), FileEvent::Read, |_| {});
                    }
                }
                drop(guards);
                drop(receivers);
                inner.registrations[slot].store(false, Ordering::Release);
            })
            .map_err(|_| {
                self.inner.registrations[slot].store(false, Ordering::Release);
                SetupError::Worker
            })?;
        subscription.pump = Some(pump);
        ready_rx.recv().map_err(|_| SetupError::Worker)??;
        Ok(subscription)
    }

    fn detach(&self, subscription: &mut Self::Subscription) -> Result<(), SetupError> {
        if !Arc::ptr_eq(&self.inner, &subscription.inner) || subscription.pump.is_none() {
            return Err(SetupError::InvalidConfig(
                "foreign or detached native subscription",
            ));
        }
        subscription.halt();
        Ok(())
    }
}
