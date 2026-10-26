// ################################################################################
// Copyright (c) 2026 Contributors to the Eclipse Foundation
//
// SPDX-License-Identifier: Apache-2.0
// ################################################################################

//! Feature-gated owned-frame support for iceoryx2 benchmark measurements.

use std::{collections::VecDeque, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::Mutex;
use up_rust::UWire;
use up_rust::{
    EncodedOwnedFrame, PreparedOwnedFrame, UEncodedOwnedListener, UEncodedRxFrame,
    UOwnedTransportCore,
};
use up_rust::{
    FrameSink, OverflowPolicy, Registration, RegistrationConfig, RegistrationHandler, UCode,
    UStatus, UTxBuffer, UUri,
};
use up_rust::{UNativePrefixWireTransport, UWithNativePrefixWire};

use crate::{Iceoryx2PubSub, Iceoryx2RxLease};

/// In-memory owned-frame core for owned adapter tests and diagnostics.
#[derive(Clone, Default)]
pub struct Iceoryx2OwnedCore {
    state: Arc<Mutex<Iceoryx2OwnedState>>,
}

/// Benchmark-only owned loopback core backed by real iceoryx2 mechanics.
#[derive(Clone)]
pub struct BenchmarkOwnedIceoryx2Core {
    inner: Iceoryx2PubSub,
    listeners: Arc<Mutex<Vec<OwnedLoopbackListenerRegistration>>>,
}

impl BenchmarkOwnedIceoryx2Core {
    /// Creates a benchmark-only owned loopback core around a real iceoryx2 core.
    #[must_use]
    pub fn new(inner: Iceoryx2PubSub) -> Self {
        Self {
            inner,
            listeners: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Wraps this core in the generic selected-wire adapter.
    #[must_use]
    pub fn with_selected_wire<W>(self, wire: W) -> UNativePrefixWireTransport<Self, W>
    where
        W: UWire,
    {
        self.into_native_prefix_wire_transport(wire)
    }

    /// Returns the wrapped real iceoryx2 core.
    #[must_use]
    pub fn inner(&self) -> &Iceoryx2PubSub {
        &self.inner
    }
}

struct OwnedLoopbackListenerRegistration {
    source_filter: UUri,
    sink_filter: Option<UUri>,
    owned_listener: Arc<dyn UEncodedOwnedListener>,
    inbox: Arc<dyn FrameSink<Iceoryx2RxLease>>,
    worker: Registration<Iceoryx2RxLease>,
}

struct OwnedLoopbackListener {
    listener: Arc<dyn UEncodedOwnedListener>,
}

impl RegistrationHandler<Iceoryx2RxLease> for OwnedLoopbackListener {
    async fn on_receive(&mut self, frame: Iceoryx2RxLease) {
        if let Ok(frame) = lease_to_encoded_owned(&frame) {
            self.listener.on_receive_encoded_owned(frame).await;
        }
    }
}

#[async_trait]
impl UOwnedTransportCore for BenchmarkOwnedIceoryx2Core {
    async fn send_prepared_owned(&self, frame: PreparedOwnedFrame) -> Result<(), UStatus> {
        let payload_len = frame.payload().map_or(0, Bytes::len);
        let mut loan = self
            .inner
            .loan_encoded(
                frame.metadata().clone(),
                frame.encoded_metadata(),
                payload_len,
                1,
            )
            .await?;
        if let Some(payload) = frame.payload() {
            loan.payload_mut().copy_from_slice(payload);
        }
        self.inner.publish_encoded(loan).await
    }

    async fn receive_encoded_owned(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
    ) -> Result<EncodedOwnedFrame, UStatus> {
        let frame = self
            .inner
            .receive_encoded(source_filter, sink_filter)
            .await?;
        lease_to_encoded_owned(&frame)
    }

    async fn register_encoded_owned_listener(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UEncodedOwnedListener>,
    ) -> Result<(), UStatus> {
        let worker = Registration::new(
            RegistrationConfig {
                capacity: 1,
                overflow: OverflowPolicy::Block {
                    deadline: std::time::Duration::from_secs(5),
                },
            },
            OwnedLoopbackListener {
                listener: listener.clone(),
            },
            || {},
        )
        .map_err(|error| UStatus::fail_with_code(UCode::ResourceExhausted, error.to_string()))?;
        let inbox: Arc<dyn FrameSink<Iceoryx2RxLease>> = worker.inbox();
        self.inner
            .attach_encoded(source_filter, sink_filter, inbox.clone())
            .await?;
        self.listeners
            .lock()
            .await
            .push(OwnedLoopbackListenerRegistration {
                source_filter: source_filter.clone(),
                sink_filter: sink_filter.cloned(),
                owned_listener: listener,
                inbox,
                worker,
            });
        Ok(())
    }

    async fn unregister_encoded_owned_listener(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UEncodedOwnedListener>,
    ) -> Result<(), UStatus> {
        let registration = {
            let mut listeners = self.listeners.lock().await;
            let Some(index) = listeners.iter().position(|registration| {
                registration.source_filter == *source_filter
                    && registration.sink_filter.as_ref() == sink_filter
                    && Arc::ptr_eq(&registration.owned_listener, &listener)
            }) else {
                return Err(UStatus::fail_with_code(
                    UCode::NotFound,
                    "owned loopback listener not registered",
                ));
            };
            listeners.remove(index)
        };
        self.inner
            .detach_encoded(source_filter, sink_filter, registration.inbox)
            .await?;
        registration
            .worker
            .stop()
            .await
            .map_err(|error| UStatus::fail_with_code(UCode::Internal, error.to_string()))
    }
}

fn lease_to_encoded_owned(frame: &Iceoryx2RxLease) -> Result<EncodedOwnedFrame, UStatus> {
    let payload = frame.try_contiguous_payload().map(Bytes::copy_from_slice);
    Ok(EncodedOwnedFrame::new(
        frame.encoded_metadata().to_vec(),
        payload,
    ))
}

#[derive(Default)]
struct Iceoryx2OwnedState {
    sent: Vec<Iceoryx2EncodedOwnedFrameLog>,
    received: VecDeque<EncodedOwnedFrame>,
    listeners: Vec<Arc<dyn UEncodedOwnedListener>>,
}

/// Captured prepared owned frame parts used by tests and diagnostics.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Iceoryx2EncodedOwnedFrameLog {
    encoded_metadata: Vec<u8>,
    payload: Option<Vec<u8>>,
}

impl Iceoryx2EncodedOwnedFrameLog {
    /// Returns selected-wire encoded metadata bytes.
    #[must_use]
    pub fn encoded_metadata(&self) -> &[u8] {
        &self.encoded_metadata
    }

    /// Returns owned payload bytes, if present.
    #[must_use]
    pub fn payload(&self) -> Option<&[u8]> {
        self.payload.as_deref()
    }

    fn from_frame(frame: &PreparedOwnedFrame) -> Self {
        Self {
            encoded_metadata: frame.encoded_metadata().to_vec(),
            payload: frame.payload().map(|payload| payload.to_vec()),
        }
    }
}

impl Iceoryx2OwnedCore {
    /// Creates an empty owned-frame core.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Wraps this core in the generic selected-wire adapter.
    #[must_use]
    pub fn with_selected_wire<W>(self, wire: W) -> UNativePrefixWireTransport<Self, W>
    where
        W: UWire,
    {
        self.into_native_prefix_wire_transport(wire)
    }

    /// Returns the last prepared owned frame observed by the core.
    pub async fn last_sent(&self) -> Option<Iceoryx2EncodedOwnedFrameLog> {
        self.state.lock().await.sent.last().cloned()
    }

    /// Injects one encoded owned frame for pull receive tests.
    pub async fn push_encoded_owned(&self, frame: EncodedOwnedFrame) {
        self.state.lock().await.received.push_back(frame);
    }

    /// Delivers one encoded owned frame to registered raw listeners.
    pub async fn deliver_encoded_owned(&self, frame: EncodedOwnedFrame) {
        let listeners = self.state.lock().await.listeners.clone();
        for listener in listeners {
            listener.on_receive_encoded_owned(frame.clone()).await;
        }
    }
}

#[async_trait]
impl UOwnedTransportCore for Iceoryx2OwnedCore {
    async fn send_prepared_owned(&self, frame: PreparedOwnedFrame) -> Result<(), UStatus> {
        self.state
            .lock()
            .await
            .sent
            .push(Iceoryx2EncodedOwnedFrameLog::from_frame(&frame));
        Ok(())
    }

    async fn receive_encoded_owned(
        &self,
        _source_filter: &UUri,
        _sink_filter: Option<&UUri>,
    ) -> Result<EncodedOwnedFrame, UStatus> {
        self.state
            .lock()
            .await
            .received
            .pop_front()
            .ok_or_else(|| UStatus::fail_with_code(UCode::NotFound, "no frame available"))
    }

    async fn register_encoded_owned_listener(
        &self,
        _source_filter: &UUri,
        _sink_filter: Option<&UUri>,
        listener: Arc<dyn UEncodedOwnedListener>,
    ) -> Result<(), UStatus> {
        self.state.lock().await.listeners.push(listener);
        Ok(())
    }

    async fn unregister_encoded_owned_listener(
        &self,
        _source_filter: &UUri,
        _sink_filter: Option<&UUri>,
        listener: Arc<dyn UEncodedOwnedListener>,
    ) -> Result<(), UStatus> {
        let mut state = self.state.lock().await;
        let Some(index) = state
            .listeners
            .iter()
            .position(|registered| Arc::ptr_eq(registered, &listener))
        else {
            return Err(UStatus::fail_with_code(
                UCode::NotFound,
                "listener not registered",
            ));
        };
        state.listeners.remove(index);
        Ok(())
    }
}
