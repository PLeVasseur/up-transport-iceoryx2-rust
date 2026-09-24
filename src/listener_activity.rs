// Copyright (c) 2026 Contributors to the Eclipse Foundation
// SPDX-License-Identifier: Apache-2.0

use std::{
    future::{Future, poll_fn},
    sync::atomic::{AtomicBool, Ordering},
    task::Poll,
};
use tokio::sync::Mutex;

pub(crate) struct ListenerActivity {
    active: AtomicBool,
    entry: Mutex<()>,
}

impl ListenerActivity {
    pub(crate) fn new() -> Self {
        Self {
            active: AtomicBool::new(true),
            entry: Mutex::new(()),
        }
    }

    pub(crate) async fn stop(&self) {
        let _entry = self.entry.lock().await;
        self.active.store(false, Ordering::Release);
    }

    pub(crate) async fn dispatch<F: Future<Output = ()>>(&self, callback: impl FnOnce() -> F) {
        let entry = self.entry.lock().await;
        if !self.active.load(Ordering::Acquire) {
            return;
        }
        let mut callback = std::pin::pin!(callback());
        // Poll once under the admission gate: creating an async future is not
        // entering its callback body. The gate is released at the first yield,
        // so an entered callback can finish and can unregister itself.
        let pending = poll_fn(|cx| Poll::Ready(callback.as_mut().poll(cx).is_pending())).await;
        drop(entry);
        if pending {
            callback.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{sync::Arc, time::Duration};

    #[tokio::test]
    async fn stopped_snapshot_never_enters_callback() {
        let activity = ListenerActivity::new();
        activity.stop().await;
        activity
            .dispatch(|| async { panic!("callback entered after unregister") })
            .await;
    }

    #[tokio::test]
    async fn callback_can_unregister_itself() {
        let activity = ListenerActivity::new();
        tokio::time::timeout(
            Duration::from_secs(5),
            activity.dispatch(|| async {
                activity.stop().await;
            }),
        )
        .await
        .expect("self-unregister must not hold the entry gate across a yield");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unregister_is_serialized_with_the_callbacks_first_poll() {
        let activity = Arc::new(ListenerActivity::new());
        let entered = Arc::new(tokio::sync::Notify::new());
        let (release, wait) = std::sync::mpsc::channel();
        let delivery = tokio::spawn({
            let activity = Arc::clone(&activity);
            let entered = Arc::clone(&entered);
            async move {
                activity
                    .dispatch(|| async move {
                        entered.notify_one();
                        wait.recv_timeout(Duration::from_secs(5))
                            .expect("release first poll");
                        std::future::pending::<()>().await;
                    })
                    .await;
            }
        });
        tokio::time::timeout(Duration::from_secs(5), entered.notified())
            .await
            .unwrap();
        let mut stop = std::pin::pin!(activity.stop());
        assert!(
            poll_fn(|cx| Poll::Ready(stop.as_mut().poll(cx)))
                .await
                .is_pending()
        );
        release.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), stop)
            .await
            .unwrap();
        delivery.abort();
        assert!(delivery.await.unwrap_err().is_cancelled());
    }
}
