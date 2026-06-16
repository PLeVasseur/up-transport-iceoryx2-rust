// ################################################################################
// Copyright (c) 2026 Contributors to the Eclipse Foundation
//
// SPDX-License-Identifier: Apache-2.0
// ################################################################################

//! iceoryx2 selected-wire transport core.
//!
//! The product core is [`Iceoryx2PubSub`]. It stores selected-wire metadata bytes
//! prepared by `up-rust` in the iceoryx2 sample prefix and keeps the iceoryx2
//! user header limited to physical placement information.

pub const UPROTOCOL_MAJOR_VERSION: u8 = 0;

#[cfg(feature = "benchmark-owned")]
mod owned;
pub(crate) mod service_attributes;
pub(crate) mod service_name_mapping;
pub(crate) mod uprotocolheader;
pub(crate) mod utransport_pubsub;
pub(crate) mod workers;

pub use iceoryx2::prelude::MessagingPattern;
#[cfg(feature = "benchmark-owned")]
pub use owned::{Iceoryx2EncodedOwnedFrameLog, Iceoryx2OwnedCore};
pub use uprotocolheader::{Iceoryx2PayloadLayout, UProtocolHeader};
pub use utransport_pubsub::{
    Iceoryx2PubSub, Iceoryx2PubSubConfig, Iceoryx2PullMismatchQueueFullPolicy, Iceoryx2RxLease,
    Iceoryx2TxLoan, Iceoryx2UninitTxLoan, PullMismatchQueueDiagnostics,
};
