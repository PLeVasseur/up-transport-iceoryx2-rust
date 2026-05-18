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

use std::sync::Arc;

use crate::utransport_pubsub::Iceoryx2PubSub;
use iceoryx2::prelude::MessagingPattern;
use up_rust::{UCode, UStatus};

/// Factory for iceoryx2 uProtocol transports.
///
/// iceoryx2 support in this crate is currently implemented for
/// [`MessagingPattern::PublishSubscribe`]. The returned [`Iceoryx2PubSub`]
/// implements both the true zero-copy transport capability and an owned-frame
/// copying adapter.
pub struct UTransportIceoryx2 {}

impl UTransportIceoryx2 {
    /// Builds an iceoryx2 transport for the requested messaging pattern.
    ///
    /// # Errors
    ///
    /// Returns [`UCode::UNIMPLEMENTED`] for messaging patterns other than
    /// [`MessagingPattern::PublishSubscribe`].
    pub fn build(messaging_pattern: MessagingPattern) -> Result<Arc<Iceoryx2PubSub>, UStatus> {
        match messaging_pattern {
            MessagingPattern::PublishSubscribe => Ok(UTransportIceoryx2::build_publish_subscribe()),
            _ => Err(UStatus::fail_with_code(
                UCode::UNIMPLEMENTED,
                "Unimplemented messaging pattern",
            )),
        }
    }

    fn build_publish_subscribe() -> Arc<Iceoryx2PubSub> {
        Iceoryx2PubSub::new()
    }
}
