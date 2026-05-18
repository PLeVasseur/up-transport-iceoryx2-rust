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
use iceoryx2::prelude::{AllocationStrategy, CallbackProgression, MessagingPattern, Service};
use iceoryx2::sample::Sample;
use iceoryx2::sample_mut::SampleMut;
use iceoryx2::{
    node::{Node, NodeBuilder},
    port::{publisher::Publisher, subscriber::Subscriber},
    prelude::ServiceName,
    service::ipc_threadsafe,
};
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;
use tokio::sync::RwLock;
use up_rust::{
    UCode, UFrameMetadata, UOwnedFrame, UOwnedTransport, UStatus, UUri,
    transport::verify_filter_criteria,
    validate_frame_metadata_for_payload, validate_owned_frame_for_transport,
    zero_copy::{
        UContiguousZeroCopyRxFrame, UTxBuffer, UZeroCopyListener, UZeroCopyRxFrame,
        UZeroCopyTransport,
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
/// It also implements [`UOwnedTransport`] by copying owned payload bytes into a
/// reserved loan on send and copying receive leases into owned frames for owned
/// listeners.
///
/// Variable native-frame metadata is stored in a hidden implementation metadata
/// prefix before the application payload. The payload views exposed through
/// [`Iceoryx2TxLoan`] and [`Iceoryx2RxLease`] exclude that prefix and any
/// alignment padding.
///
/// [`UZeroCopyTransport`]: up_rust::zero_copy::UZeroCopyTransport
/// [`UOwnedTransport`]: up_rust::UOwnedTransport
pub struct Iceoryx2PubSub {
    node: Node<ipc_threadsafe::Service>,
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
    zero_copy_listeners: ZeroCopyListenerMap,
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
        let node = NodeBuilder::new()
            .create::<ipc_threadsafe::Service>()
            .expect("Failed to create Iceoryx2 Node");
        let transport = Arc::new(Self {
            node,
            publishers: RwLock::new(HashMap::new()),
            subscribers: RwLock::new(HashMap::new()),
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
        let publisher = self.get_publisher(service_name.clone()).await;
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

        let subscriber = self.create_subscriber(service_name.clone(), source)?;
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
            .allocation_strategy(AllocationStrategy::PowerOfTwo)
            .create()
            .map_err(|e| {
                UStatus::fail_with_code(UCode::INTERNAL, format!("Failed to create publisher: {e}"))
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

    async fn refresh_listener_subscriptions(&self) -> Result<(), UStatus> {
        let mut services = Vec::new();
        ipc_threadsafe::Service::list(self.node.config(), |service| {
            services.push((
                service.static_details.name().clone(),
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
                let subscriber = self.create_subscriber(service_name.clone(), None)?;
                registration
                    .subscribers
                    .insert(service_name.clone(), Arc::new(subscriber));
            }
        }
        Ok(())
    }
}

/// iceoryx2 transmit loan for one native uProtocol frame.
///
/// Values are returned by [`UZeroCopyTransport::reserve`] for
/// [`Iceoryx2PubSub`]. The exposed payload range is aligned for the selected
/// serializer and excludes the hidden metadata prefix. After
/// [`UZeroCopyTransport::send_zero_copy`] consumes the loan, callers must treat
/// the underlying storage as no longer accessible.
///
/// [`UZeroCopyTransport::reserve`]: up_rust::zero_copy::UZeroCopyTransport::reserve
/// [`UZeroCopyTransport::send_zero_copy`]: up_rust::zero_copy::UZeroCopyTransport::send_zero_copy
pub struct Iceoryx2TxLoan {
    metadata: UFrameMetadata,
    sample: IpcSampleMut,
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

/// iceoryx2 receive lease for one native uProtocol frame.
///
/// Dropping the lease releases the underlying iceoryx2 sample. The payload is
/// guaranteed contiguous in the current mapping, so this type implements both
/// [`UZeroCopyRxFrame`] and [`UContiguousZeroCopyRxFrame`]. Borrowed decoded
/// values must not outlive the lease.
///
/// [`UZeroCopyRxFrame`]: up_rust::zero_copy::UZeroCopyRxFrame
/// [`UContiguousZeroCopyRxFrame`]: up_rust::zero_copy::UContiguousZeroCopyRxFrame
pub struct Iceoryx2RxLease {
    metadata: UFrameMetadata,
    sample: IpcSample,
    payload_offset: usize,
    payload_len: usize,
}

impl UZeroCopyRxFrame for Iceoryx2RxLease {
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

impl UContiguousZeroCopyRxFrame for Iceoryx2RxLease {
    fn contiguous_payload(&self) -> &[u8] {
        self.try_contiguous_payload()
            .expect("iceoryx2 receive payload layout should be valid")
    }
}

#[async_trait]
impl UZeroCopyTransport for Iceoryx2PubSub {
    type Tx = Iceoryx2TxLoan;
    type Rx = Iceoryx2RxLease;

    async fn reserve(
        &self,
        header: UFrameMetadata,
        payload_len: usize,
        alignment: usize,
    ) -> Result<Self::Tx, UStatus> {
        validate_alignment(alignment)?;
        if header.encoding().is_none() && payload_len != 0 {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "message payload is present but payload encoding is absent",
            ));
        }
        validate_frame_metadata_for_payload(&header, header.encoding().is_some())?;
        let source = header.attributes().source();
        let service_name = compute_service_name(
            source,
            header.attributes().sink(),
            MessagingPattern::PublishSubscribe,
        )?;
        let metadata = encode_frame_metadata(&header)?;
        let metadata_len = metadata.len();
        let publisher = self.get_or_create_publisher(service_name, source).await?;
        let mut sample_len = metadata_len.checked_add(payload_len).ok_or_else(|| {
            UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "sample length overflow")
        })?;
        let mut aligned_loan = None;
        for _ in 0..8 {
            let sample = publisher.loan_slice(sample_len).map_err(|e| {
                UStatus::fail_with_code(UCode::INTERNAL, format!("Failed to loan sample: {e}"))
            })?;
            let payload_offset = aligned_payload_offset(
                sample.payload().as_ptr() as usize,
                metadata_len,
                alignment,
            )?;
            let aligned_sample_len = payload_offset.checked_add(payload_len).ok_or_else(|| {
                UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "sample length overflow")
            })?;
            if aligned_sample_len == sample.payload().len() {
                aligned_loan = Some((sample, payload_offset));
                break;
            }
            sample_len = aligned_sample_len;
        }
        let (mut sample, payload_offset) = aligned_loan.ok_or_else(|| {
            UStatus::fail_with_code(UCode::INTERNAL, "failed to reserve aligned payload loan")
        })?;
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

    async fn send_zero_copy(&self, buffer: Self::Tx) -> Result<(), UStatus> {
        buffer.sample.send().map_err(|e| {
            UStatus::fail_with_code(UCode::INTERNAL, format!("Failed to send: {e}"))
        })?;
        Ok(())
    }

    async fn receive_zero_copy(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
    ) -> Result<Self::Rx, UStatus> {
        verify_filter_criteria(source_filter, sink_filter)?;
        let service_name = compute_service_name(
            source_filter,
            sink_filter,
            MessagingPattern::PublishSubscribe,
        )?;
        let subscriber = self
            .get_or_create_subscriber(service_name, Some(source_filter))
            .await?;
        loop {
            let sample = subscriber
                .receive()
                .map_err(|e| UStatus::fail_with_code(UCode::INTERNAL, e.to_string()))?
                .ok_or_else(|| UStatus::fail_with_code(UCode::NOT_FOUND, "no sample available"))?;
            let lease = lease_from_sample(sample)?;
            if !sink_matches(lease.metadata().attributes().sink(), sink_filter) {
                continue;
            }
            return Ok(lease);
        }
    }

    async fn register_zero_copy_listener(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UZeroCopyListener<Self::Rx>>,
    ) -> Result<(), UStatus> {
        verify_filter_criteria(source_filter, sink_filter)?;
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
            let subscriber = self.create_subscriber(service_name.clone(), Some(source_filter))?;
            registration
                .subscribers
                .insert(service_name, Arc::new(subscriber));
        }
        listeners.push(registration);
        Ok(())
    }

    async fn unregister_zero_copy_listener(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UZeroCopyListener<Self::Rx>>,
    ) -> Result<(), UStatus> {
        verify_filter_criteria(source_filter, sink_filter)?;
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

fn lease_from_sample(sample: IpcSample) -> Result<Iceoryx2RxLease, UStatus> {
    let (payload_offset, payload_len) = sample
        .user_header()
        .payload_layout(sample.payload().len())?;
    let metadata = sample.user_header().frame_metadata(sample.payload())?;
    validate_frame_metadata_for_payload(&metadata, metadata.encoding().is_some())?;
    validate_payload_alignment(sample.user_header(), sample.payload(), payload_offset)?;
    Ok(Iceoryx2RxLease {
        metadata,
        sample,
        payload_offset,
        payload_len,
    })
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

#[async_trait]
impl UOwnedTransport for Iceoryx2PubSub {
    async fn send_owned(&self, frame: UOwnedFrame) -> Result<(), UStatus> {
        validate_owned_frame_for_transport(&frame)?;
        let payload = frame.payload().cloned();
        let payload_len = payload.as_ref().map_or(0, |payload| payload.len());
        let mut loan = self
            .reserve(frame.metadata().clone(), payload_len, 1)
            .await?;
        if let Some(payload) = payload {
            loan.payload_mut().copy_from_slice(&payload);
        }
        self.send_zero_copy(loan).await
    }
}
