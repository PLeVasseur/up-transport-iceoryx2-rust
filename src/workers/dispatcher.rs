// ################################################################################
// Copyright (c) 2026 Contributors to the Eclipse Foundation
//
// SPDX-License-Identifier: Apache-2.0
// ################################################################################

use std::{sync::Arc, sync::atomic::Ordering, time::Duration};

use crate::{utransport_pubsub::Iceoryx2PubSubInner, workers::worker::Iceoryx2Worker};

pub(crate) struct Iceoryx2WorkerDispatcher;

impl Iceoryx2WorkerDispatcher {
    pub(crate) fn start_listener_worker(transport: Arc<Iceoryx2PubSubInner>) {
        let worker = Iceoryx2Worker::new(transport);
        tokio::spawn(async move {
            while worker.keep_alive.load(Ordering::Relaxed) {
                let _ = worker.transport.relay_zero_copy_listeners().await;
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });
    }
}
