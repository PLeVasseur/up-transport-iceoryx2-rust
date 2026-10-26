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

//! uProtocol classic and selected-wire transport bindings over iceoryx2.
//!
//! Selected-wire cores retain native sample ownership; their placement headers
//! describe byte regions while SDK adapters validate metadata and payload identity.

/// Major version stamped and checked by this binding's physical sample header.
pub const UPROTOCOL_MAJOR_VERSION: u8 = 0;

use iceoryx2::{
    port::{publisher::Publisher, subscriber::Subscriber},
    prelude::{ServiceName, ZeroCopySend},
};
use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    sync::Arc,
};
use tokio::sync::RwLock;
use up_rust::ComparableListener;

mod classic;
pub(crate) mod service_attributes;
pub(crate) mod service_name_mapping;
/// Construction of the classic message transport for supported messaging patterns.
pub mod transport;
pub(crate) mod uprotocolheader;
pub(crate) mod utransport_pubsub;
pub(crate) mod workers;

pub use iceoryx2::prelude::MessagingPattern;
pub use uprotocolheader::{Iceoryx2PayloadLayout, UProtocolHeader};
pub use utransport_pubsub::{
    Iceoryx2PubSub, Iceoryx2PubSubConfig, Iceoryx2PullMismatchQueueFullPolicy, Iceoryx2RxLease,
    Iceoryx2TxLoan, Iceoryx2UninitTxLoan, PullMismatchQueueDiagnostics,
};

/// Marker for debug-printable user headers satisfying iceoryx2's send contract.
pub trait BaseUserHeader: Debug + ZeroCopySend {}
/// Marker for debug-printable payloads satisfying iceoryx2's send contract.
pub trait BasePayload: Debug + ZeroCopySend {}

pub(crate) type PublisherSet<Service> =
    RwLock<HashMap<ServiceName, Arc<Publisher<Service, [u8], uprotocolheader::UProtocolHeader>>>>;
pub(crate) type SubscriberSet<Service> =
    RwLock<HashMap<ServiceName, Arc<Subscriber<Service, [u8], uprotocolheader::UProtocolHeader>>>>;
pub(crate) type ListenerMap = RwLock<HashMap<ServiceName, HashSet<ComparableListener>>>;
