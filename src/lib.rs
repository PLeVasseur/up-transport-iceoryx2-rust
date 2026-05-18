// ################################################################################
// Copyright (c) 2025 Contributors to the Eclipse Foundation
//
// See the NOTICE file(s) distributed with this work for additional
// information regarding copyright ownership.
//
// This program and the accompanying materials are made available under the
// terms of the Apache License Version 2.0 which is available at
// https: //www.apache.org/licenses/LICENSE-2.0
//
// SPDX-License-Identifier: Apache-2.0
// ################################################################################

//! iceoryx2 transport for native uProtocol frames.
//!
//! The primary transport type is [`Iceoryx2PubSub`], constructed through
//! [`transport::UTransportIceoryx2::build`] with
//! [`MessagingPattern::PublishSubscribe`]. It implements
//! [`up_rust::zero_copy::UZeroCopyTransport`] using iceoryx2 transmit loans and
//! receive leases. The exposed payload view contains only the application bytes
//! produced by the selected [`up_rust::payload::PayloadFormat`]; transport metadata and
//! alignment padding are hidden from callers.
//!
//! `Iceoryx2PubSub` also implements [`up_rust::UOwnedTransport`] as a copying
//! convenience adapter. Owned sends reserve an iceoryx2 loan and copy the owned
//! payload into it; use the zero-copy extension helpers when callers can
//! serialize directly into the loan.
//!
//! Wildcard listener registrations discover concrete iceoryx2 services through
//! service attributes. Independent listener registrations use independent
//! subscribers so one consumed receive sample does not starve another matching
//! uProtocol listener.

#![warn(rustdoc::bare_urls, rustdoc::broken_intra_doc_links)]

/// uProtocol major version encoded in the iceoryx2 user header.
pub const UPROTOCOL_MAJOR_VERSION: u8 = 0;

use iceoryx2::{
    port::{publisher::Publisher, subscriber::Subscriber},
    prelude::{ServiceName, ZeroCopySend},
};
use std::{collections::HashMap, fmt::Debug, sync::Arc};
use tokio::sync::RwLock;

use crate::uprotocolheader::UProtocolHeader;

pub(crate) mod service_attributes;
pub(crate) mod service_name_mapping;
pub mod transport;
pub(crate) mod uprotocolheader;
pub(crate) mod utransport_pubsub;
pub(crate) mod workers;

pub use iceoryx2::prelude::MessagingPattern;
pub use utransport_pubsub::{Iceoryx2PubSub, Iceoryx2RxLease, Iceoryx2TxLoan};

/// Marker trait for user-header types that are safe for iceoryx2 zero-copy use.
pub trait BaseUserHeader: Debug + ZeroCopySend {}

/// Marker trait for payload types that are safe for iceoryx2 zero-copy use.
pub trait BasePayload: Debug + ZeroCopySend {}

pub(crate) type PublisherSet<Service> =
    RwLock<HashMap<ServiceName, Arc<Publisher<Service, [u8], UProtocolHeader>>>>;
pub(crate) type SubscriberSet<Service> =
    RwLock<HashMap<ServiceName, Arc<Subscriber<Service, [u8], UProtocolHeader>>>>;
