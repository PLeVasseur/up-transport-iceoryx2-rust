// ################################################################################
// Copyright (c) 2026 Contributors to the Eclipse Foundation
//
// SPDX-License-Identifier: Apache-2.0
// ################################################################################

use std::sync::{Arc, atomic::AtomicBool};

use crate::utransport_pubsub::Iceoryx2PubSubInner;

pub(crate) struct Iceoryx2Worker {
    pub(crate) keep_alive: Arc<AtomicBool>,
    pub(crate) transport: Arc<Iceoryx2PubSubInner>,
}

impl Iceoryx2Worker {
    pub(crate) fn new(transport: Arc<Iceoryx2PubSubInner>) -> Self {
        Self {
            keep_alive: Arc::new(AtomicBool::new(true)),
            transport,
        }
    }
}
