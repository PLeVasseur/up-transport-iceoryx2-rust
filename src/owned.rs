// ################################################################################
// Copyright (c) 2026 Contributors to the Eclipse Foundation
//
// SPDX-License-Identifier: Apache-2.0
// ################################################################################

//! Feature-gated owned-frame support for iceoryx2 benchmark measurements.
//!
//! This module is available only behind `benchmark-owned`. It is not part of the
//! default product-real zero-copy selected-wire evidence for [`crate::Iceoryx2PubSub`].

use std::{collections::VecDeque, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::Mutex;
use up_rust::{
    EncodedOwnedFrame, PreparedOwnedFrame, PreparedTxLoanSpec, UCode, UEncodedOwnedListener,
    UEncodedRxFrame, UEncodedZeroCopyListener, UNativePrefixWireTransport, UOwnedTransportCore,
    UStatus, UTxBuffer, UUri, UWire, UWithNativePrefixWire, UZeroCopyTransportCore,
};

use crate::{Iceoryx2PubSub, Iceoryx2RxLease};

/// iceoryx2 owned-frame core for feature-gated owned benchmark/support paths.
#[derive(Clone, Default)]
pub struct Iceoryx2OwnedCore {
    state: Arc<Mutex<Iceoryx2OwnedState>>,
}

/// Benchmark-only owned loopback core backed by real [`Iceoryx2PubSub`] mechanics.
///
/// This core is feature-gated and exists only to produce apples-to-apples owned
/// benchmark rows. It copies owned payload bytes into real iceoryx2 loans and
/// converts real receive leases back into selected-wire encoded owned frames.
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
    zero_copy_listener: Arc<OwnedLoopbackListener>,
}

struct OwnedLoopbackListener {
    listener: Arc<dyn UEncodedOwnedListener>,
}

#[async_trait]
impl UEncodedZeroCopyListener<Iceoryx2RxLease> for OwnedLoopbackListener {
    async fn on_receive_encoded_zero_copy(&self, frame: Iceoryx2RxLease) {
        match lease_to_encoded_owned(&frame) {
            Ok(frame) => self.listener.on_receive_encoded_owned(frame).await,
            Err(_error) => {}
        }
    }
}

#[async_trait]
impl UOwnedTransportCore for BenchmarkOwnedIceoryx2Core {
    async fn send_prepared_owned(&self, frame: PreparedOwnedFrame) -> Result<(), UStatus> {
        let payload_len = frame.payload().map_or(0, Bytes::len);
        let spec = PreparedTxLoanSpec::from_encoded_parts(
            frame.metadata().clone(),
            frame.encoded_metadata().to_vec(),
            payload_len,
            1,
        )?;
        let mut loan = self.inner.loan_prepared_tx(spec).await?;
        if let Some(payload) = frame.payload() {
            loan.payload_mut().copy_from_slice(payload);
        }
        self.inner.send_prepared_zero_copy(loan).await
    }

    async fn receive_encoded_owned(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
    ) -> Result<EncodedOwnedFrame, UStatus> {
        let frame = self
            .inner
            .receive_encoded_zero_copy(source_filter, sink_filter)
            .await?;
        lease_to_encoded_owned(&frame)
    }

    async fn register_encoded_owned_listener(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UEncodedOwnedListener>,
    ) -> Result<(), UStatus> {
        let zero_copy_listener = Arc::new(OwnedLoopbackListener {
            listener: listener.clone(),
        });
        self.inner
            .register_encoded_zero_copy_listener(
                source_filter,
                sink_filter,
                zero_copy_listener.clone(),
            )
            .await?;
        self.listeners
            .lock()
            .await
            .push(OwnedLoopbackListenerRegistration {
                source_filter: source_filter.clone(),
                sink_filter: sink_filter.cloned(),
                owned_listener: listener,
                zero_copy_listener,
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
            .unregister_encoded_zero_copy_listener(
                source_filter,
                sink_filter,
                registration.zero_copy_listener,
            )
            .await
    }
}

fn lease_to_encoded_owned(frame: &Iceoryx2RxLease) -> Result<EncodedOwnedFrame, UStatus> {
    let payload = frame
        .try_contiguous_payload()
        .map(|payload| Bytes::copy_from_slice(payload));
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
        _source_filter: &up_rust::UUri,
        _sink_filter: Option<&up_rust::UUri>,
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
        _source_filter: &up_rust::UUri,
        _sink_filter: Option<&up_rust::UUri>,
        listener: Arc<dyn UEncodedOwnedListener>,
    ) -> Result<(), UStatus> {
        self.state.lock().await.listeners.push(listener);
        Ok(())
    }

    async fn unregister_encoded_owned_listener(
        &self,
        _source_filter: &up_rust::UUri,
        _sink_filter: Option<&up_rust::UUri>,
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
