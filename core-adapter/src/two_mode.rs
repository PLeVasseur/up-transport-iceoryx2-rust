// Copyright (c) 2026 Contributors to the Eclipse Foundation
// SPDX-License-Identifier: Apache-2.0

//! The iceoryx2 lending binding of the two-mode transport API.
//!
//! [`Iceoryx2Setup`] is the initialization half and implements [`BindingSetup`]:
//! each role asks it to reserve one route, at which point the native service,
//! the publisher or subscriber port, and the event port are created and every
//! QoS setting is mapped to iceoryx2 or refused. [`Iceoryx2Running`] is the
//! runtime half and implements [`LendingRuntime`] in the pull design: `receive`
//! takes the next sample straight from iceoryx2's queue, so the middleware's
//! queue is the queue, and `loan`/`publish` work on reserved native storage.
//!
//! A small wait thread stays. Its only jobs are to drain the event listeners and
//! wake the SDK through [`RuntimePort::readable`], and to call
//! `update_connections` so that connection work lands on it rather than on an
//! application thread. It cannot guarantee that: `send`, `receive` and `notify`
//! run the same check themselves. This is the middleware behaviour the permit
//! [`ICEORYX2_PEER_CHANGE_PERMIT`] declares, and any deployment on this binding
//! must waive it to enter runtime mode (R4, R40, R47).
//!
//! Peer identity is stable across restarts: the frame's source URI gives the id,
//! and a local incarnation is assigned when a new native origin appears for a
//! known peer.

#![allow(clippy::result_large_err)] // Setup-only: `into_transport` never runs in runtime mode.

use std::mem::MaybeUninit;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use iceoryx2::port::backpressure_strategy::BackpressureStrategy;
use iceoryx2::port::notifier::{NotifierCreateError, NotifierNotifyError};
use iceoryx2::port::publisher::PublisherCreateError;
use iceoryx2::port::update_connections::UpdateConnections;
use iceoryx2::port::{
    BackpressureAction, BackpressureInfo, LoanError as NativeLoanError,
    ReceiveError as NativeReceiveError, SendError as NativeSendError,
};
use iceoryx2::prelude::{
    AllocationStrategy, EventId, FileDescriptorBased, LogLevel, MessagingPattern, Node,
    NodeBuilder, ServiceName, set_log_level,
};
use iceoryx2::sample::Sample;
use iceoryx2::sample_mut_uninit::SampleMutUninit;
use iceoryx2::service::builder::publish_subscribe::{
    PublishSubscribeOpenError, PublishSubscribeOpenOrCreateError,
};
use iceoryx2::service::port_factory::PortFactory as _;
use iceoryx2::{
    port::{listener::Listener, notifier::Notifier, publisher::Publisher, subscriber::Subscriber},
    prelude::{AttributeVerifier, ipc_threadsafe},
};
use iceoryx2_bb_posix::clock::{ClockType, Time};
use iceoryx2_bb_posix::file_descriptor_set::{FileDescriptorSet, FileEvent};

use up_rust_core::transport::HeapOperation;
use up_rust_core::transport::{
    BindingRefusal, BindingRoute, BindingSetup, Capabilities, Envelope, Frame, InboundRoute,
    InitializedTx, LendingRuntime, LoanError, LoanedPayload, Overflow, PayloadLoanProvenance,
    PeerRef, PreparedRoute, PreparedTxStorage, Qos, QosSupport, ReceiveBudget, ReceiveError,
    RouteDeclaration, RouteDirection, RouteKind, RuntimePermit, RuntimePort, SendError, SendReport,
    StartupError, TransportSetup, TxFrameLayout,
};
use up_rust_core::{
    frame::header::WrittenHeader, memory::StartupMemory, platform::PlatformInit,
    uattributes::UMessageType, uri::UUri, wire::RuntimeHeap,
};

use crate::peer_identity::{peer_ref, uri_peer_id};
use crate::service_attributes::{shared_request_attribute_verifier, source_attribute_verifier};
use crate::service_name_mapping::{compute_service_name, request_service_name};
use crate::sizing::{self, MAX_FRAME_LEN};
use crate::uprotocolheader::frame_facts;
use crate::{Iceoryx2Config, Iceoryx2PayloadLayout, UProtocolHeader};

type S = ipc_threadsafe::Service;
type NativePubSubFactory =
    iceoryx2::service::port_factory::publish_subscribe::PortFactory<S, [u8], UProtocolHeader>;
type NativeEventFactory = iceoryx2::service::port_factory::event::PortFactory<S>;
type NativePublisher = Publisher<S, [u8], UProtocolHeader>;
type NativeSubscriber = Subscriber<S, [u8], UProtocolHeader>;
type NativeSample = Sample<S, [u8], UProtocolHeader>;
type NativeTx = SampleMutUninit<S, [MaybeUninit<u8>], UProtocolHeader>;
type NativeListener = Listener<S>;
type NativeNotifier = Notifier<S>;

/// Retained declaration of the middleware peer-change heap deviation.
///
/// Earlier delivery tests observed connection-update allocation/free. They are
/// historical observations, not source-bound qualification of this crate's
/// resolved 0.9.2/0.9.3 graph. This permit does not establish zero heap in steady
/// state or on error/drop, and cannot waive the product's no-heap/free/panic rule.
pub static ICEORYX2_PEER_CHANGE_PERMIT: RuntimePermit = RuntimePermit {
    id: "UPW-iceoryx2-1",
    rule: "runtime.no_heap_acquire, runtime.no_heap_release",
    binding: "up-transport-iceoryx2 with iceoryx2 =0.9.2",
    operations: &[HeapOperation::PeerChange],
    bound: "per peer connection change, in iceoryx2's connection update on the next send, \
            receive, notify or update_connections of an affected port, on the wait thread or \
            the application thread, whichever gets there first; current dependency graph \
            has no source-bound steady-state allocation qualification",
};

/// Peers this transport waits for while entering runtime mode.
///
/// Waiting here lets the first frame travel on already-open connections. A peer
/// that arrives after this point is handled by iceoryx2 inside the data path.
#[derive(Clone, Copy, Debug)]
pub struct Iceoryx2Peers {
    /// Subscribers each send route must see before runtime begins.
    pub subscribers: usize,
    /// Publishers each receive route must see before runtime begins.
    pub publishers: usize,
    /// Longest to wait for those peers.
    pub timeout: Duration,
    /// Time after forcing connections for the wait thread to observe them.
    pub settle: Duration,
}

impl Default for Iceoryx2Peers {
    fn default() -> Self {
        Self {
            subscribers: 0,
            publishers: 0,
            timeout: Duration::from_secs(5),
            settle: Duration::from_millis(30),
        }
    }
}

/// Deployment sizing that peers of a service must agree on. Fixed at setup.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Iceoryx2Limits {
    /// Samples any subscriber on a service may hold borrowed at once. A route's
    /// `depth + max_held + 1` must fit; a larger route is refused at setup.
    pub max_borrowed_samples: usize,
    /// Largest route depth this binding accepts, for [`QosSupport::max_depth`].
    pub max_depth: u16,
    /// Largest route history this binding accepts, for [`QosSupport::max_history`].
    pub max_history: u16,
    /// How often the wait thread drains listeners and updates connections when idle.
    pub wait_period: Duration,
    /// Longest requested `Wait` timeout accepted at setup. Concurrent sends can
    /// reset the shared start time (R2); this is not a proven per-send bound.
    pub max_wait: Duration,
}

impl Default for Iceoryx2Limits {
    fn default() -> Self {
        Self {
            max_borrowed_samples: 16,
            max_depth: 64,
            max_history: 64,
            wait_period: Duration::from_millis(10),
            max_wait: Duration::from_secs(60),
        }
    }
}

/// Native counters of a transport, for tests, reports and tools.
#[derive(Debug, Default)]
pub struct Iceoryx2Stats {
    /// Samples published.
    pub published: AtomicU64,
    /// Publishes that failed after a loan was initialized.
    pub publish_failed: AtomicU64,
    /// `Overflow::Wait` decisions ended because the monotonic clock failed: it
    /// could not be read when the send began or while it waited, or it read
    /// earlier than when the send began. Each ended the send as timed out.
    pub clock_failed: AtomicU64,
    /// Failed event notifications; receivers still find the data on their next pull.
    pub notification_failed: AtomicU64,
    /// Notifications sent after the event service's deadline. They were delivered.
    pub notification_late: AtomicU64,
    /// Peers reported gone after cleanup announced a dead notifier.
    pub peers_reported_lost: AtomicU64,
    /// Publishes with a positive post-send connection-count gap. Concurrent peer
    /// changes make this diagnostic unsuitable as an exact per-send drop count.
    pub undelivered: AtomicU64,
    /// Samples received and handed to the SDK.
    pub received: AtomicU64,
    /// Samples discarded for a malformed physical layout or a misaligned payload.
    pub rejected_layout: AtomicU64,
    /// Samples discarded for metadata that does not parse.
    pub rejected_malformed: AtomicU64,
    /// Native receive errors other than the borrow limit.
    pub receive_failed: AtomicU64,
    /// Receives refused because this route already holds its full credit of samples.
    pub borrowed_capacity: AtomicU64,
    /// Tail bytes zeroed by truncation of a variable-size payload, across all sends.
    pub zeroed_tail_bytes: AtomicU64,
    /// Failed `update_connections` calls on the wait thread.
    pub connection_update_failed: AtomicU64,
    /// Dead nodes cleaned while initializing (R38).
    pub dead_nodes_cleaned: AtomicU64,
    /// Address of the last loaned payload, for zero-copy tests.
    pub last_loan_payload: AtomicUsize,
    /// Address of the last received payload, for zero-copy tests.
    pub last_received_payload: AtomicUsize,
}

/// Read access to a transport's native counters.
#[derive(Clone, Copy, Debug)]
pub struct Iceoryx2Diagnostics {
    stats: &'static Iceoryx2Stats,
}

impl Iceoryx2Diagnostics {
    /// The counters. Available before and after the transport enters runtime mode.
    #[must_use]
    pub fn stats(&self) -> &'static Iceoryx2Stats {
        self.stats
    }
}

/// A send route's native resources.
struct TxEntry {
    factory: NativePubSubFactory,
    event: NativeEventFactory,
    publisher: Arc<NativePublisher>,
    notifier: Arc<NativeNotifier>,
    limits: FrameLimits,
    wait: Option<WaitBudget>,
    /// Loans out on this route now. iceoryx2 keeps its own count privately; this
    /// one is what `has_capacity` can read.
    loans: AtomicUsize,
    /// iceoryx2's `max_loaned_samples` for this route.
    max_loans: usize,
}

/// Retained route-wide Wait epoch. Concurrent send reset is open finding R2.
struct WaitBudget {
    /// The clock's reading when the current send began, or `NO_READING`.
    started: Arc<AtomicU64>,
    clock: WaitClock,
}

impl WaitBudget {
    fn begin_send(&self) {
        self.started
            .store((self.clock)().unwrap_or(NO_READING), Ordering::Release);
    }
}

/// How the `Wait` path reads its clock: nanoseconds on a clock that never goes
/// back, or `None` when it cannot be read. Routes read `monotonic_nanos`; the
/// tests inject clocks that fail or go back (R67 B1).
type WaitClock = fn() -> Option<u64>;

/// What `started` holds before a send's first reading, or after a failed one.
const NO_READING: u64 = u64::MAX;

const NANOS_PER_SECOND: u64 = 1_000_000_000;

/// The monotonic clock in nanoseconds, asked for by name, or `None`.
///
/// Never a panic. std's `Instant::now` unwraps `clock_gettime`. iceoryx2's
/// `Time::now` returns the error, but reads `ClockType::default()`, which is
/// `Realtime`, a clock that can step back, where a platform has no monotonic
/// clock (`iceoryx2-bb-posix` 0.9.2 `src/clock.rs:80-87`, `:265-267`). The
/// conversion is checked, unlike `Time::as_duration`'s sum, and a reading equal
/// to `NO_READING` counts as a failed read, so the marker is never a reading
/// (R67 B1).
fn monotonic_nanos() -> Option<u64> {
    let time = Time::now_with_clock(ClockType::Monotonic).ok()?;
    time.seconds()
        .checked_mul(NANOS_PER_SECOND)?
        .checked_add(u64::from(time.nanoseconds()))
        .filter(|&nanos| nanos != NO_READING)
}

/// The monotonic clock as a `Duration`, for setup's bounded waits.
fn monotonic_now() -> Option<Duration> {
    monotonic_nanos().map(Duration::from_nanos)
}

/// The `Wait` path's setup read: a route is refused, by name, where the
/// platform has no monotonic clock, or where it cannot be read (R67 B1).
fn check_wait_clock(clock: WaitClock, supported: &[ClockType]) -> Result<(), BindingRefusal> {
    if !supported.contains(&ClockType::Monotonic) {
        return Err(BindingRefusal(
            "an Overflow::Wait route needs a monotonic clock, and this platform has none",
        ));
    }
    if clock().is_none() {
        return Err(BindingRefusal(
            "an Overflow::Wait route needs the monotonic clock, which cannot be read",
        ));
    }
    Ok(())
}

/// What a `Wait` send does when a subscriber's queue is full (R44): retry until
/// `timeout` has passed since the send began, then fail as timed out.
///
/// It fails closed (R67 B1). A clock that cannot be read now, a send whose start
/// was not read, or a reading earlier than the start ends the wait as expired,
/// and is counted in `clock_failed`. The time waited is a `checked_sub` of two
/// readings, never `Time::elapsed`, which panics when the clock went back
/// (`iceoryx2-bb-posix` 0.9.2 `src/clock.rs:282-288`).
fn wait_action(
    started: &AtomicU64,
    clock: WaitClock,
    timeout: Duration,
    stats: &Iceoryx2Stats,
) -> BackpressureAction {
    let start = started.load(Ordering::Acquire);
    let waited = match clock() {
        Some(now) if start != NO_READING => now.checked_sub(start),
        _ => None,
    };
    match waited {
        Some(waited) if u128::from(waited) < timeout.as_nanos() => BackpressureAction::Retry,
        Some(_) => BackpressureAction::DiscardDataAndFail,
        None => {
            stats.clock_failed.fetch_add(1, Ordering::Relaxed);
            BackpressureAction::DiscardDataAndFail
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct FrameLimits {
    max_metadata_len: usize,
    max_payload_len: usize,
    alignment: usize,
}

/// A receive route's native resources.
struct RxEntry {
    factory: NativePubSubFactory,
    subscriber: Arc<NativeSubscriber>,
    listener: Arc<NativeListener>,
    inbound: InboundRoute,
    credit: RetainedBudget,
    faulted: AtomicBool,
    /// The configured dead-notifier event, if the deployment set one.
    dead_event: Option<EventId>,
    /// Who to report gone on that event. `None` on a route with many sources.
    lost_peer: Option<PeerRef>,
}

/// A receiver's retained-sample budget: the upper bound on its borrows.
///
/// Setup keeps this at most half the expired-connection buffer as a retained
/// mitigation for iceoryx2 0.9.2 duplicate bookkeeping. This local credit bound
/// is not a proof that every middleware churn/panic path is unreachable.
#[derive(Clone, Copy, Debug)]
struct RetainedBudget {
    outstanding: &'static AtomicUsize,
    limit: usize,
}

impl RetainedBudget {
    fn take(&self) -> Option<RetainedCredit> {
        let current = self.outstanding.load(Ordering::Acquire);
        if current >= self.limit {
            return None;
        }
        self.outstanding
            .compare_exchange(current, current + 1, Ordering::AcqRel, Ordering::Acquire)
            .ok()?;
        Some(RetainedCredit(self.outstanding))
    }
}

/// Returns one retained-sample credit after its native sample has been released.
struct RetainedCredit(&'static AtomicUsize);

impl Drop for RetainedCredit {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

enum RouteEntry {
    Tx(TxEntry),
    Rx(RxEntry),
}

/// The initialization half of the iceoryx2 binding.
///
/// Native resources are reserved route by route, in `prepare_route`. The node is
/// created in `recover_predecessor`, which also cleans up after a crashed
/// predecessor (R38). Construction with no configuration file is direct; use
/// [`Self::from_config`] for the configuration path. Both produce this type (R6).
pub struct Iceoryx2Setup {
    config: Iceoryx2Config,
    native: iceoryx2::prelude::Config,
    peers: Iceoryx2Peers,
    limits: Iceoryx2Limits,
    quiet_log: bool,
    stats: &'static Iceoryx2Stats,
    node: Option<Arc<Node<S>>>,
    routes: Vec<RouteEntry>,
    any_wait_route: bool,
    /// Every declared client seen so far: its peer id and its URI (setup only).
    declared_clients: Vec<(u64, UUri)>,
}

impl std::fmt::Debug for Iceoryx2Setup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Iceoryx2Setup")
            .field("routes", &self.routes.len())
            .finish_non_exhaustive()
    }
}

impl Iceoryx2Setup {
    /// Prepares a setup from a native configuration and expected peers.
    ///
    /// Direct construction in code; [`Self::from_config`] is the configuration
    /// path. Both produce the same type (R6).
    #[must_use]
    pub fn new(config: Iceoryx2Config, peers: Iceoryx2Peers) -> Self {
        let stats: &'static Iceoryx2Stats = Box::leak(Box::new(Iceoryx2Stats::default()));
        let native = config.native.clone();
        Self {
            config,
            native,
            peers,
            limits: Iceoryx2Limits::default(),
            quiet_log: true,
            stats,
            node: None,
            routes: Vec::new(),
            any_wait_route: false,
            declared_clients: Vec::new(),
        }
    }

    /// Reads a deployment description of `key = value` lines.
    ///
    /// Required: `root`, `prefix`. Optional: `max_subscribers`,
    /// `max_publishers`, `max_notifiers`, `min_subscribers`, `min_publishers`,
    /// `timeout_ms`, `settle_ms`, `max_borrowed_samples`. Deployment data is
    /// validated fully here (R43): every problem is named, and unknown keys are
    /// refused. An RPC server needs `max_publishers` and `max_notifiers` of at
    /// least its peer count, one per client; its setup refuses otherwise.
    ///
    /// # Errors
    /// Returns every problem found in the description.
    pub fn from_config(text: &str) -> Result<Self, Vec<String>> {
        let mut problems = Vec::new();
        let mut root = None;
        let mut prefix = None;
        let mut max_subscribers = None;
        let mut max_publishers = None;
        let mut max_notifiers = None;
        let mut peers = Iceoryx2Peers::default();
        let mut limits = Iceoryx2Limits::default();
        for (number, line) in text.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let Some((key, value)) = line.split_once('=') else {
                problems.push(format!("line {}: expected key = value", number + 1));
                continue;
            };
            let (key, value) = (key.trim(), value.trim());
            let mut number_value = |slot: &mut Option<usize>| match value.parse::<usize>() {
                Ok(parsed) => *slot = Some(parsed),
                Err(_) => problems.push(format!("line {}: {key} is not a number", number + 1)),
            };
            match key {
                "root" => root = Some(value.to_owned()),
                "prefix" => prefix = Some(value.to_owned()),
                "max_subscribers" => number_value(&mut max_subscribers),
                "max_publishers" => number_value(&mut max_publishers),
                "max_notifiers" => number_value(&mut max_notifiers),
                "max_borrowed_samples" => {
                    let mut parsed = None;
                    number_value(&mut parsed);
                    limits.max_borrowed_samples = parsed.unwrap_or(limits.max_borrowed_samples);
                }
                "min_subscribers" => {
                    let mut parsed = None;
                    number_value(&mut parsed);
                    peers.subscribers = parsed.unwrap_or(0);
                }
                "min_publishers" => {
                    let mut parsed = None;
                    number_value(&mut parsed);
                    peers.publishers = parsed.unwrap_or(0);
                }
                "timeout_ms" => {
                    let mut parsed = None;
                    number_value(&mut parsed);
                    peers.timeout = Duration::from_millis(parsed.unwrap_or(5000) as u64);
                }
                "settle_ms" => {
                    let mut parsed = None;
                    number_value(&mut parsed);
                    peers.settle = Duration::from_millis(parsed.unwrap_or(30) as u64);
                }
                other => problems.push(format!("line {}: unknown key {other}", number + 1)),
            }
        }
        for (present, name) in [(root.is_some(), "root"), (prefix.is_some(), "prefix")] {
            if !present {
                problems.push(format!("missing required key {name}"));
            }
        }
        if !problems.is_empty() {
            return Err(problems);
        }
        let (Some(root), Some(prefix)) = (root, prefix) else {
            return Err(problems);
        };
        let mut config = Iceoryx2Config::default()
            .with_namespace(&root, &prefix)
            .map_err(|error| vec![format!("invalid namespace: {}", error.0)])?;
        {
            let native = &mut config.native;
            let defaults = &mut native.defaults.publish_subscribe;
            if let Some(value) = max_subscribers {
                defaults.max_subscribers = value;
            }
            if let Some(value) = max_publishers {
                defaults.max_publishers = value;
            }
            if let Some(value) = max_notifiers {
                native.defaults.event.max_notifiers = value;
            }
        }
        let mut setup = Self::new(config, peers);
        setup.limits = limits;
        Ok(setup)
    }

    /// Replaces the deployment sizing.
    #[must_use]
    pub fn with_limits(mut self, limits: Iceoryx2Limits) -> Self {
        self.limits = limits;
        self
    }

    /// Whether entering runtime lowers iceoryx2's log level to `Fatal`.
    ///
    /// iceoryx2's console logger formats onto the heap for warnings on data
    /// paths, so the default is to silence those. The setting is process-wide.
    #[must_use]
    pub fn with_quiet_log(mut self, quiet: bool) -> Self {
        self.quiet_log = quiet;
        self
    }

    /// Records one declared client, refusing a second URI with the same peer id.
    fn note_declared_client(&mut self, client: &UUri) -> Result<(), BindingRefusal> {
        let id = uri_peer_id(client);
        if declared_id_collides(&self.declared_clients, id, client) {
            return Err(BindingRefusal(
                "two declared clients have the same peer id; rename one of them",
            ));
        }
        self.declared_clients.push((id, client.clone()));
        Ok(())
    }

    /// A handle for reading this transport's native counters.
    #[must_use]
    pub fn diagnostics(&self) -> Iceoryx2Diagnostics {
        Iceoryx2Diagnostics { stats: self.stats }
    }

    /// Wraps this binding in caller-supplied core startup memory and platform.
    ///
    /// # Errors
    /// Returns the core startup error if initialization or route capacity fails.
    pub fn into_transport_on(
        self,
        memory: &StartupMemory,
        platform: &'static dyn PlatformInit,
    ) -> Result<TransportSetup, StartupError> {
        TransportSetup::lending_on(memory, platform, self)
    }

    fn node(&self) -> Result<Arc<Node<S>>, BindingRefusal> {
        self.node
            .clone()
            .ok_or(BindingRefusal("the iceoryx2 node was not created"))
    }

    fn request_verifier(route: &RouteDeclaration<'_>) -> Result<AttributeVerifier, BindingRefusal> {
        let verifier = if route.kind == Some(UMessageType::Request) {
            shared_request_attribute_verifier()
        } else {
            source_attribute_verifier(route.source)
        };
        verifier.map_err(|_| BindingRefusal("invalid iceoryx2 service attributes for this route"))
    }

    /// Opens the service as a receiver: every capacity is a hard requirement.
    fn open_receive_service(
        &self,
        node: &Node<S>,
        name: &ServiceName,
        route: &RouteDeclaration<'_>,
        verifier: &AttributeVerifier,
        borrowed: usize,
    ) -> Result<NativePubSubFactory, BindingRefusal> {
        let qos = route.qos;
        // A client sizes its response queue to its calls in flight, while the
        // server's route for those responses declares the SDK's default depth.
        // Whichever side opens first, the service must hold what the server
        // declares; this client's own queue stays `qos.depth`.
        let capacity = if route.kind == Some(UMessageType::Response) {
            qos.depth.max(Qos::DEFAULT.depth)
        } else {
            qos.depth
        };
        self.service_builder(node, name, &qos)
            .subscriber_max_buffer_size(usize::from(capacity))
            .subscriber_max_borrowed_samples(borrowed)
            .open_or_create_with_attributes(verifier)
            .map_err(|error| map_open_or_create_error(&error))
    }

    /// Opens the service as a sender, demanding the depth this route declares.
    ///
    /// A service that cannot hold that depth per receiver refuses the route, naming
    /// it and both depths (R44). This is what keeps `routes()` honest: the SDK
    /// reports the depth a route declared and cannot learn of another one. An
    /// admitted send route's depth is therefore a floor the service really offers;
    /// a receiver's own route reports the queue that receiver has.
    fn open_send_service(
        &self,
        node: &Node<S>,
        name: &ServiceName,
        route: &RouteDeclaration<'_>,
        verifier: &AttributeVerifier,
    ) -> Result<NativePubSubFactory, BindingRefusal> {
        let qos = route.qos;
        let borrowed = self.limits.max_borrowed_samples;
        let demand_depth = || {
            self.service_builder(node, name, &qos)
                .subscriber_max_buffer_size(usize::from(qos.depth))
        };
        let result = match demand_depth()
            .subscriber_max_borrowed_samples(borrowed)
            .open_or_create_with_attributes(verifier)
        {
            // The borrow limit is the receivers' own sizing, not this route's
            // contract. Its depth still is.
            Err(PublishSubscribeOpenOrCreateError::PublishSubscribeOpenError(
                PublishSubscribeOpenError::DoesNotSupportRequestedMinSubscriberBorrowedSamples,
            )) => demand_depth().open_or_create_with_attributes(verifier),
            other => other,
        };
        match result {
            Ok(service) => Ok(service),
            Err(PublishSubscribeOpenOrCreateError::PublishSubscribeOpenError(
                PublishSubscribeOpenError::DoesNotSupportRequestedMinBufferSize,
            )) => Err(self.sender_depth_refusal(node, name, route, verifier)),
            Err(error) => Err(map_open_or_create_error(&error)),
        }
    }

    /// The refusal for a send route deeper than the service it opened, naming the
    /// route, the declared depth and the service's. Setup only: it allocates.
    fn sender_depth_refusal(
        &self,
        node: &Node<S>,
        name: &ServiceName,
        route: &RouteDeclaration<'_>,
        verifier: &AttributeVerifier,
    ) -> BindingRefusal {
        let actual = self
            .service_builder(node, name, &route.qos)
            .open_with_attributes(verifier)
            .map(|service| service.static_config().subscriber_max_buffer_size());
        let held = match actual {
            Ok(depth) => format!("holds {depth} per receiver"),
            Err(_) => "holds a smaller depth per receiver".to_owned(),
        };
        let reason = format!(
            "depth: route {:?} {:?} {} declares depth {}, but the existing native service {held}; \
             peers must agree on a depth the service holds",
            route.direction, route.kind, route.source, route.qos.depth
        );
        BindingRefusal(Box::leak(reason.into_boxed_str()))
    }

    fn service_builder<'n>(
        &self,
        node: &'n Node<S>,
        name: &'n ServiceName,
        qos: &Qos,
    ) -> iceoryx2::service::builder::publish_subscribe::Builder<[u8], UProtocolHeader, S> {
        let defaults = &self.native.defaults.publish_subscribe;
        node.service_builder(name)
            .publish_subscribe::<[u8]>()
            .user_header::<UProtocolHeader>()
            .history_size(usize::from(qos.history))
            .enable_safe_overflow(matches!(qos.overflow, Overflow::DropOldest))
            .max_subscribers(defaults.max_subscribers)
            .max_publishers(defaults.max_publishers)
            .max_nodes(defaults.max_nodes)
    }

    fn prepare_send_route(
        &mut self,
        route: &RouteDeclaration<'_>,
        name: &ServiceName,
        node: &Node<S>,
        limits: FrameLimits,
    ) -> Result<PreparedRoute, BindingRefusal> {
        let verifier = Self::request_verifier(route)?;
        let qos = route.qos;
        // Up-front check on the frame itself so an unusable route is named here.
        let sample_len =
            frame_capacity(limits).ok_or(BindingRefusal("frame capacity overflows"))?;
        let factory = self.open_send_service(node, name, route, &verifier)?;
        let max_loans = route.slots.max(1);
        // Peer-created compatible services may have larger capacities or
        // alignment. Check the actual immutable service before create() reaches
        // native unchecked sample layout/count/segment arithmetic (R1).
        sizing::publisher(factory.static_config(), sample_len, max_loans)?;
        let mut builder = factory
            .publisher_builder()
            .initial_max_slice_len(sample_len)
            .allocation_strategy(AllocationStrategy::Static)
            .max_loaned_samples(max_loans);
        let wait = match qos.overflow {
            Overflow::DropOldest | Overflow::DropNewest => {
                builder = builder.backpressure_strategy(BackpressureStrategy::DiscardData);
                None
            }
            Overflow::Wait { timeout } => {
                // Retained handler bounds retries for an isolated send. R2:
                // concurrent publishes can overwrite this route-wide epoch;
                // a per-send deadline and bounded mutex contention remain open.
                let clock: WaitClock = monotonic_nanos;
                check_wait_clock(clock, ClockType::all_supported_clocks())?;
                let started = Arc::new(AtomicU64::new(NO_READING));
                let handler_started = Arc::clone(&started);
                let stats = self.stats;
                builder = builder
                    .backpressure_strategy(BackpressureStrategy::RetryUntilDelivered)
                    .set_backpressure_handler(move |_: &BackpressureInfo| {
                        wait_action(&handler_started, clock, timeout, stats)
                    });
                self.any_wait_route = true;
                Some(WaitBudget { started, clock })
            }
        };
        let publisher = builder.create().map_err(|error| match error {
            PublisherCreateError::ExceedsMaxSupportedPublishers => BindingRefusal(
                "max_publishers: the native service already has as many publishers as it \
                 allows; raise defaults.publish_subscribe.max_publishers in the iceoryx2 \
                 configuration (max_publishers in Iceoryx2Setup::from_config)",
            ),
            _ => BindingRefusal("native publisher could not be reserved for this route"),
        })?;
        // Exercise the initial native loan during setup. This is not evidence
        // that all backing pages are resident or cannot fault during runtime.
        let initial = publisher
            .loan_slice_uninit(sample_len)
            .map_err(|_| BindingRefusal("native static segment could not be materialized"))?;
        drop(initial);
        let (notifier, event) = self.open_notifier(node, name)?;
        let index = u32::try_from(self.routes.len()).unwrap_or(u32::MAX);
        self.routes.push(RouteEntry::Tx(TxEntry {
            factory,
            event,
            publisher: Arc::new(publisher),
            notifier,
            limits,
            wait,
            loans: AtomicUsize::new(0),
            max_loans,
        }));
        Ok(PreparedRoute::new(
            BindingRoute(index),
            RuntimeHeap::Middleware(&ICEORYX2_PEER_CHANGE_PERMIT),
        ))
    }

    fn open_notifier(
        &self,
        node: &Node<S>,
        name: &ServiceName,
    ) -> Result<(Arc<NativeNotifier>, NativeEventFactory), BindingRefusal> {
        let event = node
            .service_builder(name)
            .event()
            .open_or_create()
            .map_err(|_| BindingRefusal("native event service could not be opened"))?;
        let notifier = event
            .notifier_builder()
            .create()
            .map_err(|error| match error {
                NotifierCreateError::ExceedsMaxSupportedNotifiers => BindingRefusal(
                    "max_notifiers: the native event service already has as many notifiers as \
                     it allows; raise defaults.event.max_notifiers in the iceoryx2 \
                     configuration (max_notifiers in Iceoryx2Setup::from_config)",
                ),
                _ => BindingRefusal("native event notifier could not be reserved"),
            })?;
        Ok((Arc::new(notifier), event))
    }

    fn prepare_receive_route(
        &mut self,
        route: &RouteDeclaration<'_>,
        name: &ServiceName,
        node: &Node<S>,
    ) -> Result<PreparedRoute, BindingRefusal> {
        let inbound = route
            .inbound
            .ok_or(BindingRefusal("a receive route needs an inbound index"))?;
        let verifier = Self::request_verifier(route)?;
        let budget = route.slots.max(1);
        let expired_buffer = self
            .native
            .defaults
            .publish_subscribe
            .subscriber_expired_connection_buffer;
        // Twice the budget: one departed connection can take two entries (see
        // `RetainedBudget`).
        if budget
            .checked_mul(2)
            .is_none_or(|needed| needed > expired_buffer)
        {
            return Err(BindingRefusal(
                "this route's retained-sample budget needs twice its size in the \
                 expired-connection buffer; raise subscriber_expired_connection_buffer in the \
                 native configuration to at least twice the budget",
            ));
        }
        let borrowed = self.limits.max_borrowed_samples.max(budget);
        let factory = self.open_receive_service(node, name, route, &verifier, borrowed)?;
        let event = node
            .service_builder(name)
            .event()
            .open_or_create()
            .map_err(|_| BindingRefusal("native event service could not be opened"))?;
        // Before any port exists, so a refused route leaves no peer to see.
        check_peer_capacity(route, &factory, &event)?;
        let subscriber = factory
            .subscriber_builder()
            .buffer_size(usize::from(route.qos.depth))
            .create()
            .map_err(|_| {
                BindingRefusal("native subscriber could not be reserved for this route")
            })?;
        subscriber
            .update_connections()
            .map_err(|_| BindingRefusal("native subscriber connection failed"))?;
        let listener = event
            .listener_builder()
            .create()
            .map_err(|_| BindingRefusal("native event listener could not be reserved"))?;
        // SAFETY: iceoryx2-bb-posix 0.9.2 native_handle requires that the raw
        // value not be stored and that this code not close it. The temporary is
        // consumed solely by this comparison while listener owns the descriptor.
        // No ownership transfer or descriptor operation is performed.
        if unsafe { listener.file_descriptor().native_handle() } < 0
            // SAFETY: the same live listener still owns the descriptor; the
            // value is consumed by a capacity comparison, never stored/closed.
            || unsafe { listener.file_descriptor().native_handle() } as usize
                >= FileDescriptorSet::capacity()
        {
            return Err(BindingRefusal(
                "the event listener's descriptor does not fit a descriptor set",
            ));
        }
        let outstanding: &'static AtomicUsize = Box::leak(Box::new(AtomicUsize::new(0)));
        let dead_event = self
            .native
            .defaults
            .event
            .notifier_dead_event
            .map(EventId::new);
        // A request route has many clients, so its dead events name nobody.
        let lost_peer = (route.kind != Some(UMessageType::Request)).then(|| PeerRef {
            peer: uri_peer_id(route.source),
            incarnation: 0,
        });
        let index = u32::try_from(self.routes.len()).unwrap_or(u32::MAX);
        self.routes.push(RouteEntry::Rx(RxEntry {
            factory,
            subscriber: Arc::new(subscriber),
            listener: Arc::new(listener),
            inbound,
            faulted: AtomicBool::new(false),
            credit: RetainedBudget {
                outstanding,
                limit: budget,
            },
            dead_event,
            lost_peer,
        }));
        Ok(PreparedRoute::new(
            BindingRoute(index),
            RuntimeHeap::Middleware(&ICEORYX2_PEER_CHANGE_PERMIT),
        ))
    }
}

/// Refuses a receive route whose SDK peer table needs more peers than its native
/// services can connect (`RouteDeclaration::max_peers`, R29).
///
/// Only a request route hears from several peers on this binding: a peer is the
/// frame's source URI (see `peer_identity`), and every other receive route names
/// one exact source. A request route's clients each send through their own
/// publisher and notifier on the method's shared services, so those services'
/// `max_publishers` and `max_notifiers` bound how many clients can connect. They
/// are read from the services as they exist, which another participant may have
/// created. Setup only: a refusal is formatted once and leaked.
fn check_peer_capacity(
    route: &RouteDeclaration<'_>,
    data: &NativePubSubFactory,
    event: &NativeEventFactory,
) -> Result<(), BindingRefusal> {
    if route.kind != Some(UMessageType::Request) {
        return Ok(());
    }
    let publishers = data.static_config().max_publishers();
    let notifiers = event.static_config().max_notifiers();
    let (setting, limit, ports, path) = if route.max_peers > publishers {
        (
            "max_publishers",
            publishers,
            "publishers",
            "defaults.publish_subscribe.max_publishers",
        )
    } else if route.max_peers > notifiers {
        (
            "max_notifiers",
            notifiers,
            "notifiers",
            "defaults.event.max_notifiers",
        )
    } else {
        return Ok(());
    };
    let method = route.sink.unwrap_or(route.source);
    let reason = format!(
        "{setting}: the request route for {method} keeps {} peers apart, but its native \
         service allows {limit} {ports}, one per client; raise {path} in the iceoryx2 \
         configuration ({setting} in Iceoryx2Setup::from_config), or declare fewer peers",
        route.max_peers
    );
    Err(BindingRefusal(Box::leak(reason.into_boxed_str())))
}

fn frame_capacity(limits: FrameLimits) -> Option<usize> {
    sizing::frame_len(
        limits.max_metadata_len,
        limits.max_payload_len,
        limits.alignment,
    )
    .ok()
}

fn map_open_or_create_error(error: &PublishSubscribeOpenOrCreateError) -> BindingRefusal {
    use PublishSubscribeOpenError as Open;
    use PublishSubscribeOpenOrCreateError as Either;
    match error {
        Either::PublishSubscribeOpenError(Open::DoesNotSupportRequestedMinBufferSize) => {
            BindingRefusal(
                "depth: the existing native service has a smaller subscriber buffer than this \
                 route declares; peers must declare compatible depths",
            )
        }
        Either::PublishSubscribeOpenError(
            Open::DoesNotSupportRequestedMinSubscriberBorrowedSamples,
        ) => BindingRefusal(
            "max_held: the existing native service allows fewer borrowed samples than this \
             route needs; raise max_borrowed_samples or lower max_held",
        ),
        Either::PublishSubscribeOpenError(Open::DoesNotSupportRequestedMinHistorySize) => {
            BindingRefusal("history: the existing native service keeps less history than declared")
        }
        Either::PublishSubscribeOpenError(Open::IncompatibleOverflowBehavior) => BindingRefusal(
            "overflow: the existing native service has a different overflow policy than declared",
        ),
        Either::PublishSubscribeOpenError(Open::IncompatibleTypes) => {
            BindingRefusal("the existing native service has a different payload or header type")
        }
        Either::PublishSubscribeOpenError(Open::IncompatibleAttributes) => {
            BindingRefusal("the existing native service belongs to a different source")
        }
        Either::PublishSubscribeOpenError(Open::ExceedsMaxNumberOfNodes) => {
            BindingRefusal("the native service already has its maximum number of nodes")
        }
        _ => BindingRefusal("the native service could not be opened or created for this route"),
    }
}

impl BindingSetup for Iceoryx2Setup {
    type Running = Iceoryx2Running;

    fn name(&self) -> &'static str {
        "iceoryx2"
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities::new(Envelope::UFrameFields, ENVELOPES, MAX_FRAME_LEN)
            .with_version(env!("CARGO_PKG_VERSION"))
            .with_per_message_encoding(true)
            .with_truncate_payload(true)
            .with_declared_clients(true)
            .with_chunked_receive(false)
            .with_native_loans(true)
    }

    fn qos_support(&self, _kind: RouteKind) -> QosSupport {
        QosSupport::new()
            .with_reliable(true)
            .with_drop_oldest(true)
            .with_drop_newest(true)
            .with_wait(true)
            .with_max_depth(self.limits.max_depth)
            .with_max_history(self.limits.max_history)
            .with_priority_levels(0)
    }

    fn recover_predecessor(&mut self) -> Result<(), BindingRefusal> {
        if self.quiet_log {
            // A warning on a data path would format onto the heap. Fatal only.
            set_log_level(LogLevel::Fatal);
        }
        self.config
            .validate()
            .map_err(|_| BindingRefusal("the iceoryx2 configuration is invalid"))?;
        // Clean up after a crashed predecessor before any route opens a service.
        let cleaned = Node::<S>::try_cleanup_dead_nodes(&self.native);
        self.stats
            .dead_nodes_cleaned
            .fetch_add(cleaned.cleanups, Ordering::Relaxed);
        let node = NodeBuilder::new()
            .config(&self.native)
            .create::<S>()
            .map_err(|_| BindingRefusal("the iceoryx2 node could not be created"))?;
        self.node = Some(Arc::new(node));
        Ok(())
    }

    fn prepare_route(
        &mut self,
        route: &RouteDeclaration<'_>,
    ) -> Result<PreparedRoute, BindingRefusal> {
        if u32::try_from(self.routes.len()).is_err() {
            return Err(BindingRefusal("binding route index capacity exhausted"));
        }
        // Check public safe sizing before node lookup or any native builder.
        let limits = FrameLimits {
            max_metadata_len: route.max_metadata_len,
            max_payload_len: route.max_payload_len,
            alignment: route.payload_alignment,
        };
        frame_capacity(limits).ok_or(BindingRefusal("invalid or excessive frame capacity"))?;
        self.config.validate()?;
        let native = &self.native.defaults.publish_subscribe;
        sizing::Capacities {
            subscribers: native.max_subscribers,
            buffer: usize::from(route.qos.depth.max(Qos::DEFAULT.depth)),
            borrowed: self.limits.max_borrowed_samples.max(route.slots.max(1)),
            history: usize::from(route.qos.history),
        }
        .samples(route.slots.max(1))?;
        let node = self.node()?;
        let Some(kind) = route.kind else {
            return Err(BindingRefusal(
                "a carrier route here needs a message kind, because a lending leg names one \
                 iceoryx2 service: declare one route per kind, at an exact address",
            ));
        };
        if route.envelope != Envelope::UFrameFields {
            return Err(BindingRefusal("this binding carries uframe-fields only"));
        }
        let name = service_name(kind, route)?;
        if route.max_metadata_len == 0 || !route.payload_alignment.is_power_of_two() {
            return Err(BindingRefusal("invalid frame layout for this route"));
        }
        if let Overflow::Wait { timeout } = route.qos.overflow {
            if timeout > self.limits.max_wait {
                return Err(BindingRefusal(
                    "this route's Wait timeout exceeds the binding's limit; \
                     lower it or raise Iceoryx2Limits::max_wait",
                ));
            }
            verify_wait_prerequisites()?;
        }
        if kind == UMessageType::Response
            && route.direction == RouteDirection::Send
            && let Some(client) = route.sink
        {
            self.note_declared_client(client)?;
        }
        match route.direction {
            RouteDirection::Send => self.prepare_send_route(route, &name, &node, limits),
            RouteDirection::Receive => self.prepare_receive_route(route, &name, &node),
        }
    }

    fn into_running(self, port: RuntimePort) -> Result<Iceoryx2Running, BindingRefusal> {
        let Iceoryx2Setup {
            peers,
            limits,
            stats,
            node,
            routes,
            any_wait_route,
            ..
        } = self;
        let node = node.ok_or(BindingRefusal("the iceoryx2 node was not created"))?;
        if any_wait_route {
            // Repeat the startup probe after peer setup; it does not certify
            // future native clock calls or no-panic behavior.
            verify_wait_prerequisites()?;
        }
        let entries = routes.into_boxed_slice();
        force_connections(&entries, peers)?;
        let entries = Box::leak(entries);
        let node = Box::leak(Box::new(node));
        let running = Iceoryx2Running {
            routes: entries,
            stats,
            _node: node,
        };
        // Spawning a thread allocates, which is fine: this is initialization. It
        // reaches its idle loop before `into_running` returns.
        spawn_wait_thread(&running, port, limits.wait_period)?;
        Ok(running)
    }
}

const ENVELOPES: &[Envelope] = &[Envelope::UFrameFields];

fn service_name(
    kind: UMessageType,
    route: &RouteDeclaration<'_>,
) -> Result<ServiceName, BindingRefusal> {
    let refusal = |_| BindingRefusal("this route has no valid iceoryx2 service name");
    match kind {
        UMessageType::Request => {
            let method = route
                .sink
                .ok_or(BindingRefusal("a request route needs its method as sink"))?;
            exact(method)?;
            request_service_name(method).map_err(refusal)
        }
        UMessageType::Publish => {
            exact(route.source)?;
            compute_service_name(route.source, None, MessagingPattern::PublishSubscribe)
                .map_err(refusal)
        }
        UMessageType::Response | UMessageType::Notification => {
            exact(route.source)?;
            let sink = route
                .sink
                .ok_or(BindingRefusal("this route needs an exact sink"))?;
            exact(sink)?;
            compute_service_name(route.source, Some(sink), MessagingPattern::PublishSubscribe)
                .map_err(refusal)
        }
    }
}

fn exact(uri: &UUri) -> Result<(), BindingRefusal> {
    uri.check_no_wildcards().map_err(|_| {
        BindingRefusal(
            "a wildcard address names no iceoryx2 service; this binding needs exact addresses",
        )
    })
}

/// Probes the native clock and adaptive-wait constructor during setup.
/// A failure here is refused early. Success does not prove future native clock
/// calls or unwraps cannot fail; runtime no-panic/progress remains unqualified.
fn verify_wait_prerequisites() -> Result<(), BindingRefusal> {
    Time::now().map_err(|_| BindingRefusal("the system clock cannot be read"))?;
    iceoryx2_bb_posix::adaptive_wait::AdaptiveWaitBuilder::new()
        .create()
        .map_err(|_| BindingRefusal("the system clock cannot be read"))?;
    Ok(())
}

/// The ports the wait thread keeps connected.
enum WaitPort {
    Tx {
        publisher: Arc<NativePublisher>,
        notifier: Arc<NativeNotifier>,
    },
    Rx {
        subscriber: Arc<NativeSubscriber>,
    },
}

/// The connection-update view of every route. Built once, during initialization.
fn wait_ports(routes: &[RouteEntry]) -> Vec<WaitPort> {
    routes
        .iter()
        .map(|entry| match entry {
            RouteEntry::Tx(tx) => WaitPort::Tx {
                publisher: Arc::clone(&tx.publisher),
                notifier: Arc::clone(&tx.notifier),
            },
            RouteEntry::Rx(rx) => WaitPort::Rx {
                subscriber: Arc::clone(&rx.subscriber),
            },
        })
        .collect()
}

/// Waits, bounded, for the expected peers, then forces every native connection,
/// so the first frame travels on connections already established (R3).
fn force_connections(routes: &[RouteEntry], peers: Iceoryx2Peers) -> Result<(), BindingRefusal> {
    let clock = BindingRefusal("the system clock cannot be read");
    let deadline = monotonic_now()
        .ok_or(clock)?
        .checked_add(peers.timeout)
        .ok_or(BindingRefusal("peer readiness deadline overflows"))?;
    loop {
        let mut ready = true;
        for entry in routes {
            ready &= match entry {
                RouteEntry::Tx(tx) => {
                    // A subscribing peer also creates a listener. Wait for that
                    // too, or the first notify would connect to it in runtime.
                    tx.factory.dynamic_config().number_of_subscribers() >= peers.subscribers
                        && tx.event.dynamic_config().number_of_listeners() >= peers.subscribers
                }
                RouteEntry::Rx(rx) => {
                    rx.factory.dynamic_config().number_of_publishers() >= peers.publishers
                }
            };
        }
        if ready {
            break;
        }
        if monotonic_now().ok_or(clock)? >= deadline {
            return Err(BindingRefusal(
                "expected peers did not appear before the readiness deadline",
            ));
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    // A peer's port is counted before its registration is published, so the
    // first round can miss one. The second round, after a settle, picks it up.
    let ports = wait_ports(routes);
    update_all_connections(&ports, None)?;
    std::thread::sleep(peers.settle);
    update_all_connections(&ports, None)
}

fn update_all_connections(
    ports: &[WaitPort],
    stats: Option<&Iceoryx2Stats>,
) -> Result<(), BindingRefusal> {
    let mut result = Ok(());
    for port in ports {
        let outcome = match port {
            WaitPort::Tx {
                publisher,
                notifier,
            } => publisher
                .update_connections()
                .and_then(|()| notifier.update_connections()),
            WaitPort::Rx { subscriber } => subscriber.update_connections(),
        };
        if outcome.is_err() {
            if let Some(stats) = stats {
                stats
                    .connection_update_failed
                    .fetch_add(1, Ordering::Relaxed);
            }
            result = Err(BindingRefusal(
                "a native connection could not be established",
            ));
        }
    }
    result
}

/// The wait thread: drains listeners, wakes the SDK, updates connections.
///
/// Uses a descriptor set instead of the retained Vec-based WaitSet path. A
/// startup pass runs before ready is reported. Peer changes still perform native
/// allocation/free; lazy error/cleanup paths and timing are not qualified here.
fn wait_thread(
    wait_entries: Vec<WaitEntry>,
    ports: Vec<WaitPort>,
    port: RuntimePort,
    period: Duration,
    ready: &'static AtomicBool,
    stats: &'static Iceoryx2Stats,
) {
    let waitset = FileDescriptorSet::new();
    let mut guards = Vec::with_capacity(wait_entries.len());
    for entry in &wait_entries {
        match waitset.add(entry.listener.as_ref()) {
            Ok(guard) => guards.push(guard),
            Err(_) => {
                // Prepared listeners were checked against the descriptor limit
                // during setup, so this can only be a broken descriptor.
                stats
                    .connection_update_failed
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    run_pass(&wait_entries, &ports, port, stats);
    ready.store(true, Ordering::Release);
    loop {
        // The configured sleep bounds the requested idle wait only. Native
        // mutex acquisition and connection cleanup can extend a pass; this is
        // not a guaranteed upper bound on arrival notification latency.
        let _ = waitset.timed_wait(period, FileEvent::Read, |_| {});
        run_pass(&wait_entries, &ports, port, stats);
    }
}

struct WaitEntry {
    listener: Arc<NativeListener>,
    subscriber: Arc<NativeSubscriber>,
    inbound: InboundRoute,
    dead_event: Option<EventId>,
    lost_peer: Option<PeerRef>,
}

fn run_pass(
    wait_entries: &[WaitEntry],
    ports: &[WaitPort],
    port: RuntimePort,
    stats: &'static Iceoryx2Stats,
) {
    for entry in wait_entries {
        let mut woken = false;
        let mut lost = false;
        // Empty the wake hints. Each means data was published, except the
        // deployment's dead-notifier event, which cleanup posts for a dead peer.
        let _ = entry.listener.try_wait_all(|event| {
            if Some(event) == entry.dead_event {
                lost = true;
            } else {
                woken = true;
            }
        });
        if lost && let Some(peer) = entry.lost_peer {
            port.peer_lost(entry.inbound, peer);
            stats.peers_reported_lost.fetch_add(1, Ordering::Relaxed);
        }
        // `has_samples` also runs this port's connection update, and catches a
        // sample whose notification was lost.
        if entry.subscriber.has_samples().unwrap_or(false) || woken {
            port.readable(entry.inbound);
        }
    }
    let _ = update_all_connections(ports, Some(stats));
}

fn spawn_wait_thread(
    running: &Iceoryx2Running,
    port: RuntimePort,
    period: Duration,
) -> Result<(), BindingRefusal> {
    let mut wait_entries = Vec::new();
    for entry in running.routes.iter() {
        if let RouteEntry::Rx(rx) = entry {
            wait_entries.push(WaitEntry {
                listener: Arc::clone(&rx.listener),
                subscriber: Arc::clone(&rx.subscriber),
                inbound: rx.inbound,
                dead_event: rx.dead_event,
                lost_peer: rx.lost_peer,
            });
        }
    }
    let ports = wait_ports(&running.routes);
    let ready: &'static AtomicBool = Box::leak(Box::new(AtomicBool::new(false)));
    let stats = running.stats;
    std::thread::Builder::new()
        .name("iceoryx2-wait".into())
        .spawn(move || wait_thread(wait_entries, ports, port, period, ready, stats))
        .map_err(|_| BindingRefusal("the wait thread could not be started"))?;
    let clock = BindingRefusal("the system clock cannot be read");
    let deadline = monotonic_now()
        .ok_or(clock)?
        .saturating_add(Duration::from_secs(10));
    while !ready.load(Ordering::Acquire) {
        if monotonic_now().ok_or(clock)? >= deadline {
            return Err(BindingRefusal("the wait thread did not start"));
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    Ok(())
}

/// The runtime handle. Native resources remain reserved for the process lifetime.
/// Dropping this handle does not unregister a subscriber behind a retained sample.
pub struct Iceoryx2Running {
    // Safety invariant: these permanently reserved entries retain every native
    // subscriber/publisher/service while any loan or received sample can exist.
    routes: &'static [RouteEntry],
    stats: &'static Iceoryx2Stats,
    // Keeps the node, and through it the services and their shared memory, for
    // the whole process. No release path in runtime mode.
    _node: &'static Arc<Node<S>>,
}

impl std::fmt::Debug for Iceoryx2Running {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Iceoryx2Running")
            .field("routes", &self.routes.len())
            .finish_non_exhaustive()
    }
}

impl Iceoryx2Running {
    fn tx_entry(&self, route: BindingRoute) -> Option<&TxEntry> {
        match self.routes.get(route.0 as usize)? {
            RouteEntry::Tx(tx) => Some(tx),
            RouteEntry::Rx(_) => None,
        }
    }

    fn rx_entry(&self, route: BindingRoute) -> Option<&RxEntry> {
        match self.routes.get(route.0 as usize)? {
            RouteEntry::Rx(rx) => Some(rx),
            RouteEntry::Tx(_) => None,
        }
    }
}

/// An exclusive native uninitialized loan with immutable, disjoint SDK extents.
pub struct Iceoryx2Tx<'a> {
    running: &'a Iceoryx2Running,
    route: u32,
    // Safety invariant: Some owns the sole unpublished sample; no native send
    // or replacement occurs while either SDK view can be borrowed. None is
    // reachable only inside consuming publish, after the certificate is taken.
    sample: Option<NativeTx>,
    // Safety invariant: metadata is 0..metadata_len and payload is the checked
    // disjoint range in this sample. All other sample bytes are initialized u8.
    // Shrinking initializes the removed suffix and preserves allocation/offsets.
    layout: Iceoryx2PayloadLayout,
    /// Whether the SDK will fill a payload; a present empty payload differs from
    /// an absent one, which only the metadata records.
    present: bool,
    written: Option<WrittenHeader>,
}

impl std::fmt::Debug for Iceoryx2Tx<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Iceoryx2Tx").finish_non_exhaustive()
    }
}

// SAFETY: PreparedTxStorage requires exclusive stable storage, disjoint views,
// preserved initialized bytes, and a recorded header for the same metadata.
// The private sample/layout invariants establish these ranges. iceoryx2 0.9.2
// SampleMutUninit owns an unpublished loan and its documented payload_mut lends
// that allocation; neither accessor resizes/replaces it. The Rust &mut self
// borrow prevents overlapping safe accesses to either returned mutable slice
// (Reference: types/pointer.html#mutable-references). Only checked subslices of
// that native slice are exposed, including zero lengths; no raw pointers are
// reconstructed. record_header validates len/token and changes bookkeeping only.
// truncate_payload keeps the allocation and prefix in place and writes valid
// u8 zeroes into the relinquished suffix before narrowing its logical extent.
// The private fields and absence of Clone prevent safe callers forging owners.
// Moving/forgetting the owner cannot invalidate memory through a published
// pointer: only consuming publish hands it to middleware. The native loan owns
// its backing resources; permanently reserved routes also retain its publisher.
unsafe impl PreparedTxStorage for Iceoryx2Tx<'_> {
    fn metadata_uninit_mut(&mut self) -> &mut [MaybeUninit<u8>] {
        // A consumed loan has no extent; the SDK's length check then refuses it.
        let extent = self.layout.metadata_len();
        match self.sample.as_mut() {
            Some(sample) => sample.payload_mut().get_mut(..extent).unwrap_or_default(),
            None => &mut [],
        }
    }

    fn payload_uninit_mut(&mut self) -> Option<&mut [MaybeUninit<u8>]> {
        if !self.present {
            return None;
        }
        let range = self.layout.payload_range();
        self.sample
            .as_mut()
            .and_then(|sample| sample.payload_mut().get_mut(range))
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

    fn truncate_payload(&mut self, used: usize) -> Result<(), SendError> {
        let sample = self.sample.as_mut().ok_or(SendError::PayloadStorage)?;
        if used > self.layout.payload_len() {
            return Err(SendError::PayloadStorage);
        }
        // iceoryx2 cannot shrink a loan, and a `[u8]` service needs every byte of
        // the sample initialized. So `used..loaned` is zeroed here, which is the
        // only cost of a variable-size payload; an exact wire never pays it.
        let range = self.layout.payload_range();
        let tail_start = range
            .start
            .checked_add(used)
            .ok_or(SendError::PayloadStorage)?;
        if let Some(tail) = sample.payload_mut().get_mut(tail_start..range.end) {
            for byte in tail.iter_mut() {
                byte.write(0);
            }
            self.running
                .stats
                .zeroed_tail_bytes
                .fetch_add(tail.len() as u64, Ordering::Relaxed);
        }
        let shortened = Iceoryx2PayloadLayout::from_validated_parts(
            self.layout.metadata_len(),
            range.start,
            used,
            self.layout.payload_alignment(),
            sample.payload().len(),
        )
        .map_err(|_| SendError::PayloadStorage)?;
        sample
            .user_header_mut()
            .write_payload_layout(shortened)
            .map_err(|_| SendError::PayloadStorage)?;
        self.layout = shortened;
        Ok(())
    }
}

impl Drop for Iceoryx2Tx<'_> {
    fn drop(&mut self) {
        // Every loan ends here, published or not.
        if let Some(entry) = self.running.tx_entry(BindingRoute(self.route)) {
            entry.loans.fetch_sub(1, Ordering::AcqRel);
        }
        // Only unpublished storage is ours to wipe. Published native slots have
        // no last-reader callback and cannot be wiped here.
        if let Some(sample) = &mut self.sample
            && let Some(header) = self.written
            && let Some((offset, len)) = header.token
            && let Some(token) = offset
                .checked_add(len)
                .and_then(|end| sample.payload_mut().get_mut(offset..end))
        {
            for byte in token {
                byte.write(0);
            }
        }
    }
}

/// A received native sample, retained until its lease drops.
pub struct Iceoryx2Rx {
    sample: NativeSample,
    layout: Iceoryx2PayloadLayout,
    payload_present: bool,
    peer: PeerRef,
    _credit: RetainedCredit,
}

impl std::fmt::Debug for Iceoryx2Rx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Iceoryx2Rx").finish_non_exhaustive()
    }
}

impl Frame for Iceoryx2Rx {
    fn encoded_metadata(&self) -> &[u8] {
        // An extent outside the sample reads as empty, which admission rejects.
        self.sample
            .payload()
            .get(..self.layout.metadata_len())
            .unwrap_or_default()
    }

    fn payload(&self) -> Option<LoanedPayload<'_>> {
        if !self.payload_present {
            return None;
        }
        Some(LoanedPayload::new(
            self.sample.payload().get(self.layout.payload_range())?,
            PayloadLoanProvenance::OpaqueTransportLoan,
        ))
    }

    fn peer(&self) -> PeerRef {
        self.peer
    }
}

impl LendingRuntime for Iceoryx2Running {
    type Tx<'a> = Iceoryx2Tx<'a>;
    type Rx = Iceoryx2Rx;

    fn receive_fault(&self, route: BindingRoute) -> Option<ReceiveError> {
        let entry = self.rx_entry(route)?;
        entry
            .faulted
            .load(Ordering::Acquire)
            .then_some(ReceiveError::Unsupported(
                "native receive failed; route closed",
            ))
    }

    /// Whether a loan is free on `route`: all this binding can see.
    ///
    /// `false` is a promise that the next loan would be refused. `true` is not a
    /// promise of delivery: iceoryx2 tells a publisher nothing about a receiver's
    /// queue, so a full receiver shows only at `send`, under the route's overflow
    /// policy, and the loan count cannot see a data segment that has run out.
    fn has_capacity(&self, route: BindingRoute) -> bool {
        self.tx_entry(route)
            .is_some_and(|entry| entry.loans.load(Ordering::Acquire) < entry.max_loans)
    }

    fn loan(
        &self,
        route: BindingRoute,
        layout: TxFrameLayout,
    ) -> Result<Iceoryx2Tx<'_>, LoanError> {
        let entry = self.tx_entry(route).ok_or(LoanError::InvalidRoute)?;
        let limits = entry.limits;
        let payload_len = layout.payload_len.unwrap_or(0);
        if layout.metadata_len > limits.max_metadata_len
            || payload_len > limits.max_payload_len
            || !layout.payload_alignment.is_power_of_two()
            || layout.payload_alignment > limits.alignment
        {
            return Err(LoanError::SlotTooSmall);
        }
        // Metadata, up to `alignment - 1` bytes of alignment gap, and the payload.
        // Loaning exactly this keeps the tail to the alignment slack, which is
        // zeroed below; an exact-size wire then costs no zeroing beyond that.
        let sample_len = layout
            .metadata_len
            .checked_add(layout.payload_alignment - 1)
            .and_then(|len| len.checked_add(payload_len))
            .ok_or(LoanError::SlotTooSmall)?;
        let mut sample =
            entry
                .publisher
                .loan_slice_uninit(sample_len)
                .map_err(|error| match error {
                    NativeLoanError::ExceedsMaxLoanSize => LoanError::SlotTooSmall,
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
            payload_len,
            layout.payload_alignment,
            sample_len,
        )
        .map_err(|_| LoanError::SlotTooSmall)?;
        sample
            .user_header_mut()
            .write_payload_layout(physical)
            .map_err(|_| LoanError::SlotTooSmall)?;
        // Initialize what the SDK will not: the alignment gap and any tail.
        if let Some(gap) = sample.payload_mut().get_mut(layout.metadata_len..offset) {
            for byte in gap.iter_mut() {
                byte.write(0);
            }
        }
        if let Some(tail) = sample.payload_mut().get_mut(physical.payload_range().end..) {
            for byte in tail.iter_mut() {
                byte.write(0);
            }
        }
        if let Some(payload) = sample.payload_mut().get(physical.payload_range()) {
            self.stats
                .last_loan_payload
                .store(payload.as_ptr() as usize, Ordering::Relaxed);
        }
        entry.loans.fetch_add(1, Ordering::AcqRel);
        Ok(Iceoryx2Tx {
            running: self,
            route: route.0,
            sample: Some(sample),
            layout: physical,
            present: layout.payload_len.is_some(),
            written: None,
        })
    }

    fn publish<'a>(
        &'a self,
        route: BindingRoute,
        frame: InitializedTx<Iceoryx2Tx<'a>>,
    ) -> Result<SendReport, SendError> {
        let mut tx = frame.into_storage();
        if tx.route != route.0 || !std::ptr::eq(tx.running, self) || tx.written.is_none() {
            return Err(SendError::PayloadStorage);
        }
        let entry = self.tx_entry(route).ok_or(SendError::PayloadStorage)?;
        let sample = tx.sample.take().ok_or(SendError::PayloadStorage)?;
        if let Some(wait) = &entry.wait {
            wait.begin_send();
        }
        // SAFETY: iceoryx2 0.9.2 SampleMutUninit::assume_init requires every
        // payload element to be a valid initialized u8. The consumed
        // InitializedTx contract certifies the current metadata/payload views;
        // route/runtime identity and a recorded exact metadata length were
        // checked above. The private layout invariant supplies initialized
        // alignment gap and tail, including every suffix removed by truncation.
        // Together these disjoint ranges cover the full native slice, including
        // absent/present-empty payloads. Its header is a fully initialized
        // UProtocolHeader value; no interpretation of metadata bits is used for
        // memory access here. sample was taken from the sole owner and cannot
        // have a remaining safe mutable borrow. No intervening call receives
        // its storage. The conversion consumes the uninitialized owner, yields
        // one initialized owner of the same allocation, then send consumes it.
        let sample = unsafe { sample.assume_init() };
        let recipients = match sample.send() {
            Ok(recipients) => recipients,
            Err(NativeSendError::UnableToDeliver) => {
                self.stats.publish_failed.fetch_add(1, Ordering::Relaxed);
                return Err(SendError::TimedOut);
            }
            Err(_) => {
                self.stats.publish_failed.fetch_add(1, Ordering::Relaxed);
                return Err(SendError::Delivery);
            }
        };
        self.stats.published.fetch_add(1, Ordering::Relaxed);
        let connected = entry.factory.dynamic_config().number_of_subscribers();
        let dropped = connected.saturating_sub(recipients);
        if dropped > 0 {
            self.stats.undelivered.fetch_add(1, Ordering::Relaxed);
        }
        // Publication already succeeded. A lost notification cannot roll it back:
        // the wait thread still finds the data on its next pass.
        match entry.notifier.notify() {
            Ok(_) => {}
            // The event service's deadline passed. iceoryx2 delivered it anyway.
            Err(NotifierNotifyError::MissedDeadline) => {
                self.stats.notification_late.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                self.stats
                    .notification_failed
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
        Ok(native_send_report(recipients))
    }

    fn receive(&self, route: BindingRoute) -> Option<Iceoryx2Rx> {
        self.receive_impl(route, None)
    }

    fn receive_bounded(&self, route: BindingRoute, budget: &ReceiveBudget) -> Option<Iceoryx2Rx> {
        self.receive_impl(route, Some(budget))
    }
}

impl Iceoryx2Running {
    fn receive_impl(
        &self,
        route: BindingRoute,
        budget: Option<&ReceiveBudget>,
    ) -> Option<Iceoryx2Rx> {
        let entry = self.rx_entry(route)?;
        if entry.faulted.load(Ordering::Acquire) {
            return None;
        }
        // Legacy direct pulls retain their local bound. SDK pulls share the
        // caller's allowance across this filter and every later admission layer.
        // The observation bounds the loop; try_take below debits the SAME budget.
        let attempts = budget.map_or(RECEIVE_DISCARD_BOUND, ReceiveBudget::remaining);
        for _ in 0..attempts {
            // Reserve a credit before borrowing, so this receiver's total borrows
            // can never exceed the guarded budget.
            let Some(credit) = entry.credit.take() else {
                self.stats.borrowed_capacity.fetch_add(1, Ordering::Relaxed);
                return None;
            };
            // Reserve retained-sample capacity first. A failed credit does not
            // attempt a native receive. Exhaustion returns that unused credit.
            if budget.is_some_and(|budget| !budget.try_take()) {
                return None;
            }
            let sample = match entry.subscriber.receive() {
                Ok(Some(sample)) => sample,
                Ok(None) => return None,
                Err(NativeReceiveError::ExceedsMaxBorrows) => {
                    self.stats.borrowed_capacity.fetch_add(1, Ordering::Relaxed);
                    return None;
                }
                Err(_) => {
                    entry.faulted.store(true, Ordering::Release);
                    self.stats.receive_failed.fetch_add(1, Ordering::Relaxed);
                    return None;
                }
            };
            self.stats.received.fetch_add(1, Ordering::Relaxed);
            let Ok(layout) = sample.user_header().payload_layout(sample.payload().len()) else {
                self.stats.rejected_layout.fetch_add(1, Ordering::Relaxed);
                continue;
            };
            let payload = sample.payload();
            let address = (payload.as_ptr() as usize).checked_add(layout.payload_range().start);
            if address.is_none_or(|address| address & (layout.payload_alignment() - 1) != 0) {
                self.stats.rejected_layout.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            let metadata = payload.get(..layout.metadata_len()).unwrap_or_default();
            let Some(facts) = frame_facts(metadata) else {
                self.stats
                    .rejected_malformed
                    .fetch_add(1, Ordering::Relaxed);
                continue;
            };
            if let Some(body) = payload.get(layout.payload_range()) {
                self.stats
                    .last_received_payload
                    .store(body.as_ptr() as usize, Ordering::Relaxed);
            }
            let peer = peer_ref(facts.source_id(), sample.origin().value());
            return Some(Iceoryx2Rx {
                sample,
                layout,
                payload_present: facts.payload_present,
                peer,
                _credit: credit,
            });
        }
        None
    }
}

// Stock send returns recipient acceptances, but neither DropOldest replacements
// nor a coherent per-send rejected-receiver count. A later connection snapshot
// is diagnostic only and cannot establish even a lower bound on those discards.
fn native_send_report(recipients: usize) -> SendReport {
    let mut report = SendReport::unknown();
    report.delivered = u64::try_from(recipients).unwrap_or(u64::MAX);
    report
}

#[cfg(test)]
mod send_report_tests {
    use super::native_send_report;

    #[test]
    fn acceptance_count_is_not_truncated_to_u16() {
        let recipients = usize::from(u16::MAX) + 1;
        let report = native_send_report(recipients);
        assert_eq!(report.delivered, 65_536);
        assert_eq!(report.dropped_full, 0);
        assert!(!report.counts_exact);
    }

    #[test]
    fn zero_and_maximum_acceptance_keep_drop_totals_unknown() {
        for recipients in [0, usize::MAX] {
            let report = native_send_report(recipients);
            assert_eq!(
                report.delivered,
                u64::try_from(recipients).unwrap_or(u64::MAX)
            );
            assert_eq!(report.dropped_full, 0);
            assert!(!report.counts_exact);
        }
    }
}

/// Malformed frames discarded per pull before giving control back.
const RECEIVE_DISCARD_BOUND: usize = 32;

/// Whether `uri` would share a peer id with a different, already declared client.
fn declared_id_collides(known: &[(u64, UUri)], id: u64, uri: &UUri) -> bool {
    known
        .iter()
        .any(|(known_id, known_uri)| *known_id == id && known_uri != uri)
}

#[cfg(test)]
mod declared_client_tests {
    use super::*;

    fn client(type_id: u16) -> UUri {
        UUri::try_from_wire_parts("clients", (1 << 16) | u32::from(type_id), 1, 0).expect("uri")
    }

    #[test]
    fn a_different_uri_with_the_same_id_is_a_collision() {
        let known = vec![(7, client(1))];
        assert!(declared_id_collides(&known, 7, &client(2)));
    }

    #[test]
    fn the_same_uri_is_not_a_collision() {
        let known = vec![(7, client(1))];
        assert!(!declared_id_collides(&known, 7, &client(1)));
    }

    #[test]
    fn a_different_id_is_not_a_collision() {
        let known = vec![(7, client(1))];
        assert!(!declared_id_collides(&known, 8, &client(2)));
    }
}

#[cfg(test)]
mod retained_budget_tests {
    use super::*;

    #[test]
    fn a_retained_budget_is_a_limit_and_credits_come_back() {
        static OUTSTANDING: AtomicUsize = AtomicUsize::new(0);
        let budget = RetainedBudget {
            outstanding: &OUTSTANDING,
            limit: 2,
        };
        let first = budget.take().expect("within the budget");
        let second = budget.take().expect("within the budget");
        assert!(budget.take().is_none(), "the budget is a limit");
        assert_eq!(
            budget.outstanding.load(Ordering::Relaxed),
            2,
            "a refused take gives its count back"
        );
        drop(first);
        let third = budget.take().expect("a released credit is available again");
        drop(second);
        drop(third);
        assert_eq!(budget.outstanding.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn a_retained_budget_never_exceeds_its_limit_under_contention() {
        static OUTSTANDING: AtomicUsize = AtomicUsize::new(0);
        let budget = RetainedBudget {
            outstanding: &OUTSTANDING,
            limit: 3,
        };
        std::thread::scope(|scope| {
            for _ in 0..4 {
                scope.spawn(|| {
                    for _ in 0..200 {
                        if let Some(credit) = budget.take() {
                            assert!(budget.outstanding.load(Ordering::Acquire) <= 4);
                            drop(credit);
                        }
                    }
                });
            }
        });
        assert_eq!(budget.outstanding.load(Ordering::Relaxed), 0);
    }
}

/// The `Wait` path's clock (R67 B1): each of its three reads fails in one test,
/// and the clock goes back in another. Every failure ends the wait as expired
/// and is counted; none is a panic or a longer wait.
#[cfg(test)]
mod wait_clock_tests {
    use std::cell::RefCell;
    use std::collections::VecDeque;

    use super::*;

    thread_local! {
        /// What the scripted clock reads on this test's thread, in order. `None`
        /// is a failed read, and so is every read after the last.
        static READINGS: RefCell<VecDeque<Option<u64>>> = const { RefCell::new(VecDeque::new()) };
    }

    fn scripted() -> Option<u64> {
        READINGS.with(|readings| readings.borrow_mut().pop_front().flatten())
    }

    fn script(readings: &[Option<u64>]) {
        READINGS.with(|script| *script.borrow_mut() = readings.iter().copied().collect());
    }

    const TIMEOUT: Duration = Duration::from_micros(10);
    const SUPPORTED: &[ClockType] = &[ClockType::Monotonic, ClockType::Realtime];

    /// A `Wait` route's budget, as setup builds it, on the scripted clock.
    fn budget() -> WaitBudget {
        WaitBudget {
            started: Arc::new(AtomicU64::new(NO_READING)),
            clock: scripted,
        }
    }

    /// One call of the route's backpressure handler.
    fn decide(budget: &WaitBudget, stats: &Iceoryx2Stats) -> BackpressureAction {
        wait_action(&budget.started, budget.clock, TIMEOUT, stats)
    }

    fn clock_failures(stats: &Iceoryx2Stats) -> u64 {
        stats.clock_failed.load(Ordering::Relaxed)
    }

    #[test]
    fn a_wait_retries_within_its_timeout_and_fails_at_it() {
        script(&[Some(1_000), Some(10_999), Some(11_000)]);
        let stats = Iceoryx2Stats::default();
        let budget = budget();
        budget.begin_send();
        assert_eq!(decide(&budget, &stats), BackpressureAction::Retry);
        assert_eq!(
            decide(&budget, &stats),
            BackpressureAction::DiscardDataAndFail
        );
        assert_eq!(
            clock_failures(&stats),
            0,
            "a timeout is not a clock failure"
        );
    }

    /// At `f2909c0` a step back saturated to no time waited, so the send
    /// retried, and a clock that kept stepping back held it past its bound.
    #[test]
    fn a_clock_that_goes_back_ends_the_wait_and_is_counted() {
        script(&[Some(50_000), Some(49_999)]);
        let stats = Iceoryx2Stats::default();
        let budget = budget();
        budget.begin_send();
        assert_eq!(
            decide(&budget, &stats),
            BackpressureAction::DiscardDataAndFail
        );
        assert_eq!(clock_failures(&stats), 1);
    }

    #[test]
    fn a_failed_read_when_the_send_begins_ends_the_wait_and_is_counted() {
        script(&[None, Some(1_000)]);
        let stats = Iceoryx2Stats::default();
        let budget = budget();
        budget.begin_send();
        assert_eq!(
            decide(&budget, &stats),
            BackpressureAction::DiscardDataAndFail
        );
        assert_eq!(clock_failures(&stats), 1);
    }

    #[test]
    fn a_failed_read_while_waiting_ends_the_wait_and_is_counted() {
        script(&[Some(1_000), Some(1_001), None]);
        let stats = Iceoryx2Stats::default();
        let budget = budget();
        budget.begin_send();
        assert_eq!(decide(&budget, &stats), BackpressureAction::Retry);
        assert_eq!(
            decide(&budget, &stats),
            BackpressureAction::DiscardDataAndFail
        );
        assert_eq!(clock_failures(&stats), 1);
    }

    #[test]
    fn a_send_whose_start_was_never_read_ends_the_wait_and_is_counted() {
        script(&[Some(1_000)]);
        let stats = Iceoryx2Stats::default();
        assert_eq!(
            decide(&budget(), &stats),
            BackpressureAction::DiscardDataAndFail
        );
        assert_eq!(clock_failures(&stats), 1);
    }

    #[test]
    fn a_failed_read_at_setup_refuses_the_route() {
        script(&[None]);
        assert_eq!(
            check_wait_clock(scripted, SUPPORTED),
            Err(BindingRefusal(
                "an Overflow::Wait route needs the monotonic clock, which cannot be read"
            ))
        );
        script(&[Some(1_000)]);
        assert_eq!(check_wait_clock(scripted, SUPPORTED), Ok(()));
    }

    #[test]
    fn a_platform_without_a_monotonic_clock_refuses_the_route() {
        script(&[Some(1_000)]);
        assert_eq!(
            check_wait_clock(scripted, &[ClockType::Realtime]),
            Err(BindingRefusal(
                "an Overflow::Wait route needs a monotonic clock, and this platform has none"
            ))
        );
    }

    /// Bracketed by two reads of `ClockType::Monotonic`, so the route reads that
    /// clock. On Linux the default `Realtime` clock reads decades higher.
    #[test]
    fn the_wait_clock_is_the_monotonic_clock_asked_for_by_name() {
        let nanos = |time: Time| time.seconds() * NANOS_PER_SECOND + u64::from(time.nanoseconds());
        let before = nanos(Time::now_with_clock(ClockType::Monotonic).expect("monotonic"));
        let reading = monotonic_nanos().expect("a reading");
        let after = nanos(Time::now_with_clock(ClockType::Monotonic).expect("monotonic"));
        assert!(
            (before..=after).contains(&reading),
            "{before} <= {reading} <= {after}"
        );
    }
}

#[cfg(test)]
mod sizing_entry_tests {
    use super::*;
    use up_rust_core::transport::LifetimePolicy;

    #[test]
    fn invalid_safe_route_is_refused_before_node_or_native_builder() {
        let source = UUri::try_from_wire_parts("sizing", 0x0001_0001, 1, 0x8001).unwrap();
        let route = RouteDeclaration {
            lifetime_policy: LifetimePolicy::DEFAULT,
            direction: RouteDirection::Send,
            kind: Some(UMessageType::Publish),
            source: &source,
            sink: None,
            inbound: None,
            envelope: Envelope::UFrameFields,
            encodings: &[],
            families: &[],
            max_metadata_len: isize::MAX as usize,
            max_payload_len: 0,
            payload_alignment: 1,
            slots: 1,
            qos: Qos::DEFAULT,
            native: false,
            max_peers: 1,
        };
        let mut setup = Iceoryx2Setup::new(Iceoryx2Config::default(), Iceoryx2Peers::default());
        assert!(setup.node.is_none());
        let error = setup.prepare_route(&route).err().unwrap();
        assert_eq!(error.0, "invalid or excessive frame capacity");
        assert!(setup.node.is_none());
        assert!(setup.routes.is_empty());
    }
}
