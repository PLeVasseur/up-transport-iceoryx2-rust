// Copyright (c) 2026 Contributors to the Eclipse Foundation
// SPDX-License-Identifier: Apache-2.0
//! Allocating native controls for the inherited discovery/lifetime test corpus.
//! Prepared-path proofs use the real prepared core in their separate binaries.
#![allow(dead_code)] // Each independent test binary uses a different control subset.

use std::{
    marker::PhantomData,
    sync::{Arc, Mutex},
};
use up_rust::{
    FrameSink, NativeProfileAgreement, UCode, UFrameFieldsMetadataCodec, UFrameView, UStatus,
    UTxLoanSpec, UUri, UWire, UWireMetadataCodec, UWireMetadataContext, UWireRx,
};
use up_transport_iceoryx2_rust::{
    Iceoryx2PubSub, Iceoryx2PubSubConfig, Iceoryx2RxLease, Iceoryx2TxLoan, Iceoryx2UninitTxLoan,
};

pub type Received<W> = UWireRx<Iceoryx2RxLease, W, UFrameFieldsMetadataCodec>;

#[derive(Clone, Debug)]
pub struct Core(Iceoryx2PubSub);
impl Core {
    pub fn new() -> Self {
        Self(Iceoryx2PubSub::new())
    }
    pub fn with_config(config: Iceoryx2PubSubConfig) -> Self {
        Self(Iceoryx2PubSub::with_config(config))
    }
    pub fn try_with_config(config: Iceoryx2PubSubConfig) -> Result<Self, UStatus> {
        Iceoryx2PubSub::try_with_config(config).map(Self)
    }
    pub fn with_selected_wire<W: UWire + Send + Sync + 'static>(self, _wire: W) -> Transport<W> {
        Transport {
            core: self.0,
            agreement: None,
            registrations: Arc::new(Mutex::new(Vec::new())),
            wire: PhantomData,
        }
    }
    pub fn with_stable_wire(
        self,
        agreement: NativeProfileAgreement,
    ) -> Transport<up_rust::StableContainerWire> {
        let mut transport = self.with_selected_wire(up_rust::StableContainerWire);
        transport.agreement = Some(agreement);
        transport
    }
    pub fn publish_subscribe_service_name(
        source: &UUri,
        sink: Option<&UUri>,
    ) -> Result<String, UStatus> {
        Iceoryx2PubSub::publish_subscribe_service_name(source, sink)
    }
    pub fn exact_source_publish_subscribe_service_name(
        source: &up_rust::ExactUUri,
    ) -> Result<String, UStatus> {
        Iceoryx2PubSub::exact_source_publish_subscribe_service_name(source)
    }
}
impl std::ops::Deref for Core {
    type Target = Iceoryx2PubSub;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[async_trait::async_trait]
pub trait Listener<W: UWire + Send + Sync + 'static>: Send + Sync {
    async fn on_frame(&self, frame: Received<W>);
}

struct Entry<W: UWire + Send + Sync + 'static> {
    source: UUri,
    sink: Option<UUri>,
    listener: Arc<dyn Listener<W>>,
    raw: Arc<dyn FrameSink<Iceoryx2RxLease>>,
}
pub struct Transport<W: UWire + Send + Sync + 'static> {
    core: Iceoryx2PubSub,
    agreement: Option<NativeProfileAgreement>,
    registrations: Arc<Mutex<Vec<Entry<W>>>>,
    wire: PhantomData<W>,
}
impl<W: UWire + Send + Sync + 'static> Clone for Transport<W> {
    fn clone(&self) -> Self {
        Self {
            core: self.core.clone(),
            agreement: self.agreement.clone(),
            registrations: Arc::clone(&self.registrations),
            wire: PhantomData,
        }
    }
}

fn decode<W: UWire>(
    raw: Iceoryx2RxLease,
    agreement: Option<&NativeProfileAgreement>,
) -> Result<Received<W>, UStatus> {
    if let Some(agreement) = agreement {
        Received::try_from_encoded_with_native_agreement(
            raw,
            &UFrameFieldsMetadataCodec,
            agreement.clone(),
        )
    } else {
        Received::try_from_encoded(raw, &UFrameFieldsMetadataCodec)
    }
}
fn matches<W: UWire>(frame: &Received<W>, source: &UUri, sink: Option<&UUri>) -> bool {
    source.matches(frame.metadata().source())
        && match (sink, frame.metadata().sink()) {
            (None, None) => true,
            (Some(filter), Some(actual)) => filter.matches(actual),
            _ => false,
        }
}

struct Sink<W: UWire + Send + Sync + 'static> {
    listener: Arc<dyn Listener<W>>,
    source: UUri,
    sink: Option<UUri>,
    agreement: Option<NativeProfileAgreement>,
}
impl<W: UWire + Send + Sync + 'static> FrameSink<Iceoryx2RxLease> for Sink<W> {
    fn push(&self, raw: Iceoryx2RxLease) {
        if let Ok(frame) = decode::<W>(raw, self.agreement.as_ref())
            && matches(&frame, &self.source, self.sink.as_ref())
        {
            // This allocating control deliberately preserves the inherited
            // sequential native callback/snapshot topology. It does not
            // implement or stand in for the prepared generic handler path.
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(self.listener.on_frame(frame))
            });
        }
    }
    fn close(&self) {}
}

impl<W: UWire + Send + Sync + 'static> Transport<W> {
    pub fn core(&self) -> &Iceoryx2PubSub {
        &self.core
    }
    pub async fn loan_validated_tx(&self, spec: UTxLoanSpec) -> Result<Iceoryx2TxLoan, UStatus> {
        let bytes = UFrameFieldsMetadataCodec.encode_frame_metadata(
            UWireMetadataContext::from_wire::<W, UFrameFieldsMetadataCodec>(),
            spec.metadata(),
        )?;
        self.core
            .loan_encoded(
                spec.metadata().clone(),
                &bytes,
                spec.payload_len(),
                spec.payload_alignment_proof().as_usize(),
            )
            .await
    }
    pub async fn loan_validated_uninit_tx(
        &self,
        spec: UTxLoanSpec,
    ) -> Result<Iceoryx2UninitTxLoan, UStatus> {
        let bytes = UFrameFieldsMetadataCodec.encode_frame_metadata(
            UWireMetadataContext::from_wire::<W, UFrameFieldsMetadataCodec>(),
            spec.metadata(),
        )?;
        self.core
            .loan_encoded_uninit(
                spec.metadata().clone(),
                &bytes,
                spec.payload_len(),
                spec.payload_alignment_proof().as_usize(),
            )
            .await
    }
    pub async fn send_validated_zero_copy(&self, loan: Iceoryx2TxLoan) -> Result<(), UStatus> {
        self.core.publish_encoded(loan).await
    }
    pub async fn receive_validated_zero_copy(
        &self,
        source: &UUri,
        sink: Option<&UUri>,
    ) -> Result<Received<W>, UStatus> {
        let raw = self.core.receive_encoded(source, sink).await?;
        let frame = decode(raw, self.agreement.as_ref())?;
        if !matches(&frame, source, sink) {
            return Err(UStatus::fail_with_code(
                UCode::NotFound,
                "decoded control filters do not match",
            ));
        }
        Ok(frame)
    }
    pub async fn register_validated_zero_copy_listener(
        &self,
        source: &UUri,
        sink: Option<&UUri>,
        listener: Arc<dyn Listener<W>>,
    ) -> Result<(), UStatus> {
        let raw: Arc<dyn FrameSink<Iceoryx2RxLease>> = Arc::new(Sink {
            listener: Arc::clone(&listener),
            source: source.clone(),
            sink: sink.cloned(),
            agreement: self.agreement.clone(),
        });
        self.core
            .attach_encoded(source, sink, Arc::clone(&raw))
            .await?;
        self.registrations.lock().unwrap().push(Entry {
            source: source.clone(),
            sink: sink.cloned(),
            listener,
            raw,
        });
        Ok(())
    }
    pub async fn unregister_validated_zero_copy_listener(
        &self,
        source: &UUri,
        sink: Option<&UUri>,
        listener: Arc<dyn Listener<W>>,
    ) -> Result<(), UStatus> {
        let entry = {
            let mut entries = self.registrations.lock().unwrap();
            let index = entries
                .iter()
                .position(|entry| {
                    &entry.source == source
                        && entry.sink.as_ref() == sink
                        && Arc::ptr_eq(&entry.listener, &listener)
                })
                .ok_or_else(|| {
                    UStatus::fail_with_code(UCode::NotFound, "control registration not found")
                })?;
            entries.remove(index)
        };
        self.core.detach_encoded(source, sink, entry.raw).await
    }
}
