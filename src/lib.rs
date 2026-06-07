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

use crate::uprotocolheader::UProtocolHeader;

#[cfg(feature = "benchmark-owned")]
mod owned_benchmark;
pub(crate) mod service_attributes;
pub(crate) mod service_name_mapping;
pub mod transport;
pub(crate) mod uprotocolheader;
pub(crate) mod utransport_pubsub;
pub(crate) mod workers;

pub use iceoryx2::prelude::MessagingPattern;
#[cfg(feature = "benchmark-owned")]
pub use owned_benchmark::BenchmarkOwnedIceoryx2PubSub;
pub use utransport_pubsub::{
    Iceoryx2PubSub, Iceoryx2PubSubConfig, Iceoryx2PullMismatchQueueFullPolicy, Iceoryx2RxLease,
    Iceoryx2TxLoan, Iceoryx2UninitTxLoan, PullMismatchQueueDiagnostics,
};

pub trait BaseUserHeader: Debug + ZeroCopySend {}
pub trait BasePayload: Debug + ZeroCopySend {}

pub(crate) type PublisherSet<Service> =
    RwLock<HashMap<ServiceName, Arc<Publisher<Service, [u8], UProtocolHeader>>>>;
pub(crate) type SubscriberSet<Service> =
    RwLock<HashMap<ServiceName, Arc<Subscriber<Service, [u8], UProtocolHeader>>>>;
pub(crate) type ListenerMap = RwLock<HashMap<ServiceName, HashSet<ComparableListener>>>;
