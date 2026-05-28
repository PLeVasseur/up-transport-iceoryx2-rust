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

use crate::utransport_pubsub::{Iceoryx2PubSub, Iceoryx2PubSubConfig};
use iceoryx2::prelude::MessagingPattern;
use up_rust::{UCode, UStatus};

/// Factory for iceoryx2 uProtocol transports.
///
/// iceoryx2 support in this crate is currently implemented for
/// [`MessagingPattern::PublishSubscribe`]. The returned [`Iceoryx2PubSub`]
/// implements the direct true zero-copy transport capability. Use
/// `up_rust::transport::UOwnedFrameEndpoint::from_zero_copy_copying_adapter`
/// when an owned-frame copy boundary is intentional; that adapter is not a
/// direct zero-copy path.
pub struct UTransportIceoryx2 {}

impl UTransportIceoryx2 {
    /// Builds an iceoryx2 transport for the requested messaging pattern.
    ///
    /// # Errors
    ///
    /// Returns [`UCode::UNIMPLEMENTED`] for messaging patterns other than
    /// [`MessagingPattern::PublishSubscribe`].
    pub fn build(messaging_pattern: MessagingPattern) -> Result<Arc<Iceoryx2PubSub>, UStatus> {
        Self::build_with_config(messaging_pattern, Iceoryx2PubSubConfig::default())
    }

    /// Builds an iceoryx2 transport with explicit publish-subscribe allocation settings.
    ///
    /// # Errors
    ///
    /// Returns [`UCode::UNIMPLEMENTED`] for messaging patterns other than
    /// [`MessagingPattern::PublishSubscribe`].
    pub fn build_with_config(
        messaging_pattern: MessagingPattern,
        config: Iceoryx2PubSubConfig,
    ) -> Result<Arc<Iceoryx2PubSub>, UStatus> {
        match messaging_pattern {
            MessagingPattern::PublishSubscribe => {
                Ok(UTransportIceoryx2::build_publish_subscribe(config))
            }
            _ => Err(UStatus::fail_with_code(
                UCode::UNIMPLEMENTED,
                "Unimplemented messaging pattern",
            )),
        }
    }

    fn build_publish_subscribe(config: Iceoryx2PubSubConfig) -> Arc<Iceoryx2PubSub> {
        Iceoryx2PubSub::with_config(config)
    }
}
