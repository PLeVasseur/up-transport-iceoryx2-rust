// Copyright (c) 2026 Contributors to the Eclipse Foundation
// SPDX-License-Identifier: Apache-2.0
//! Experimental synchronous iceoryx2 adapter for `up-rust-core`.
//!
//! Setup creates native resources; the runtime uses lending loans and retained
//! samples. Host compilation does not qualify middleware allocation, panic,
//! queue-depth, arrival notification or fault-containment behavior.

mod config;
mod peer_identity;
mod service_attributes;
mod service_name_mapping;
mod sizing;
/// Setup and runtime implementation of the core lending transport contracts.
pub mod two_mode;
mod uprotocolheader;

pub use config::Iceoryx2Config;
pub use two_mode::{
    Iceoryx2Diagnostics, Iceoryx2Limits, Iceoryx2Peers, Iceoryx2Running, Iceoryx2Rx, Iceoryx2Setup,
    Iceoryx2Stats, Iceoryx2Tx,
};
pub(crate) use uprotocolheader::{Iceoryx2PayloadLayout, UProtocolHeader};
pub(crate) const UPROTOCOL_MAJOR_VERSION: u8 = 0;
