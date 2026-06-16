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
use tokio::sync::Mutex;
use up_rust::{
    EncodedOwnedFrame, PreparedOwnedFrame, UCode, UEncodedOwnedListener, UOwnedTransportCore,
    UStatus, UWire, UWireTransport, UWithWire,
};

/// iceoryx2 owned-frame core for feature-gated owned benchmark/support paths.
#[derive(Clone, Default)]
pub struct Iceoryx2OwnedCore {
    state: Arc<Mutex<Iceoryx2OwnedState>>,
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
    pub fn with_selected_wire<W>(self, wire: W) -> UWireTransport<Self, W>
    where
        W: UWire,
    {
        self.with_wire(wire)
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
