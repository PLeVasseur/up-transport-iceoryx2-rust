// ################################################################################
// Copyright (c) 2026 Contributors to the Eclipse Foundation
//
// SPDX-License-Identifier: Apache-2.0
// ################################################################################

//! Benchmark-only owned iceoryx2 transport wrapper.
//!
//! This module exists only behind `benchmark-owned`. It is not the generic
//! copying adapter and is not a product-facing owned transport API.

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;
use up_rust::{
    UCode, UOwnedFrame, UOwnedListener, UStatus, UUri,
    transport::{UOwnedTransportImpl, ValidatedOwnedFrame},
    zero_copy::{UFrameView, UTxBuffer, UTxLoanSpec, UZeroCopyListener, UZeroCopyTransport},
};

use crate::{Iceoryx2PubSub, Iceoryx2RxLease};

/// Benchmark-only owned wrapper around [`Iceoryx2PubSub`].
pub struct BenchmarkOwnedIceoryx2PubSub {
    inner: Arc<Iceoryx2PubSub>,
    listeners: Mutex<Vec<OwnedListenerRegistration>>,
}

impl BenchmarkOwnedIceoryx2PubSub {
    /// Creates a benchmark-only owned wrapper.
    #[must_use]
    pub fn new(inner: Arc<Iceoryx2PubSub>) -> Self {
        Self {
            inner,
            listeners: Mutex::new(Vec::new()),
        }
    }

    /// Returns the wrapped zero-copy transport.
    #[must_use]
    pub fn inner(&self) -> &Arc<Iceoryx2PubSub> {
        &self.inner
    }
}

#[async_trait]
impl UOwnedTransportImpl for BenchmarkOwnedIceoryx2PubSub {
    async fn send_validated_owned(&self, frame: ValidatedOwnedFrame) -> Result<(), UStatus> {
        let frame = frame.into_inner();
        let mut loan = self
            .inner
            .loan_tx(tx_loan_spec(
                frame.metadata().clone(),
                frame.has_payload(),
                frame.payload_bytes().len(),
            )?)
            .await?;
        if frame.has_payload() {
            loan.payload_mut().copy_from_slice(frame.payload_bytes());
        }
        self.inner.send_zero_copy(loan).await
    }

    async fn receive_validated_owned(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
    ) -> Result<UOwnedFrame, UStatus> {
        let frame = self
            .inner
            .receive_zero_copy(source_filter, sink_filter)
            .await?;
        lease_to_owned_frame(&frame)
    }

    async fn register_validated_owned_listener(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UOwnedListener>,
    ) -> Result<(), UStatus> {
        let zero_copy_listener = Arc::new(OwnedBenchmarkListener {
            listener: listener.clone(),
        });
        self.inner
            .register_zero_copy_listener(source_filter, sink_filter, zero_copy_listener.clone())
            .await?;
        self.listeners.lock().await.push(OwnedListenerRegistration {
            source_filter: source_filter.clone(),
            sink_filter: sink_filter.cloned(),
            owned_listener: listener,
            zero_copy_listener,
        });
        Ok(())
    }

    async fn unregister_validated_owned_listener(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UOwnedListener>,
    ) -> Result<(), UStatus> {
        let registration = {
            let mut listeners = self.listeners.lock().await;
            let Some(index) = listeners.iter().position(|registration| {
                registration.source_filter == *source_filter
                    && registration.sink_filter.as_ref() == sink_filter
                    && Arc::ptr_eq(&registration.owned_listener, &listener)
            }) else {
                return Err(UStatus::fail_with_code(
                    UCode::NOT_FOUND,
                    "no such benchmark-owned iceoryx2 listener registered for filters",
                ));
            };
            listeners.remove(index)
        };
        self.inner
            .unregister_zero_copy_listener(
                source_filter,
                sink_filter,
                registration.zero_copy_listener,
            )
            .await
    }
}

struct OwnedListenerRegistration {
    source_filter: UUri,
    sink_filter: Option<UUri>,
    owned_listener: Arc<dyn UOwnedListener>,
    zero_copy_listener: Arc<OwnedBenchmarkListener>,
}

struct OwnedBenchmarkListener {
    listener: Arc<dyn UOwnedListener>,
}

#[async_trait]
impl UZeroCopyListener<Iceoryx2RxLease> for OwnedBenchmarkListener {
    async fn on_receive_zero_copy(&self, frame: Iceoryx2RxLease) {
        let frame = lease_to_owned_frame(&frame)
            .expect("benchmark-only iceoryx2 owned listener should receive valid frames");
        self.listener.on_receive_owned(frame).await;
    }
}

fn tx_loan_spec(
    metadata: up_rust::UFrameMetadata,
    has_payload: bool,
    payload_len: usize,
) -> Result<UTxLoanSpec, UStatus> {
    if !has_payload {
        return UTxLoanSpec::no_payload(metadata);
    }
    if payload_len == 0 {
        return UTxLoanSpec::present_empty_payload(metadata);
    }
    let layout = up_rust::payload::PayloadLayout::new(payload_len, 1).map_err(UStatus::from)?;
    UTxLoanSpec::payload(metadata, layout)
}

fn lease_to_owned_frame(frame: &Iceoryx2RxLease) -> Result<UOwnedFrame, UStatus> {
    if !frame.has_payload() {
        return UOwnedFrame::try_without_payload(frame.metadata().clone()).map_err(|error| {
            UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                format!("invalid benchmark-owned iceoryx2 no-payload frame: {error}"),
            )
        });
    }
    let mut payload = Vec::with_capacity(frame.payload_len());
    for slice in frame.payload_slices() {
        payload.extend_from_slice(slice);
    }
    UOwnedFrame::try_with_payload(frame.metadata().clone(), payload).map_err(|error| {
        UStatus::fail_with_code(
            UCode::INVALID_ARGUMENT,
            format!("invalid benchmark-owned iceoryx2 payload frame: {error}"),
        )
    })
}
