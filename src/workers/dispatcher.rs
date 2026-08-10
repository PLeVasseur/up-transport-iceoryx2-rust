// ################################################################################
// Copyright (c) 2026 Contributors to the Eclipse Foundation
//
// SPDX-License-Identifier: Apache-2.0
// ################################################################################

use std::{sync::Weak, time::Duration};

use crate::utransport_pubsub::Iceoryx2PubSubInner;

pub(crate) struct Iceoryx2WorkerDispatcher;

impl Iceoryx2WorkerDispatcher {
    pub(crate) fn start_listener_worker(transport: Weak<Iceoryx2PubSubInner>) {
        tokio::spawn(async move {
            while let Some(transport) = transport.upgrade() {
                let _ = transport.relay_zero_copy_listeners().await;
                drop(transport);
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });
    }
}
