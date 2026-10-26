// SPDX-License-Identifier: Apache-2.0
//! Real IPC admission, self-unregister and retained-sample timeout checks.
use async_trait::async_trait;
use std::{
    future::{Future, poll_fn},
    sync::{Arc, Mutex, Weak},
    task::Poll,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::Notify;
use up_rust::{
    PayloadEncoding, ProtobufWire, UCode, UFrameMetadata, UFrameView, UNativePrefixWireTransport,
    UStatus, UTxBuffer, UTxLoanSpec, UUri, UZeroCopyListener, UZeroCopyTransportImpl,
};
use up_transport_iceoryx2_rust::{Iceoryx2PubSub, Iceoryx2PubSubConfig};

type Transport = UNativePrefixWireTransport<Iceoryx2PubSub, ProtobufWire>;
type Frame = <Transport as UZeroCopyTransportImpl>::Rx;

fn pair(timeout: Duration) -> (Transport, Arc<Transport>, UUri) {
    let root = std::env::var("UP_ICEORYX2_TEST_ROOT").unwrap_or_else(|_| {
        std::env::temp_dir()
            .join("up-iox-quiesce")
            .to_string_lossy()
            .into_owned()
    });
    std::fs::create_dir_all(&root).unwrap();
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let config = Iceoryx2PubSubConfig::static_allocation(64 * 1024)
        .with_namespace(&root, &format!("q{}_{}", std::process::id(), nonce))
        .unwrap()
        .with_publisher_readiness(1, Duration::from_secs(5));
    let publisher = Iceoryx2PubSub::with_config(config.clone()).with_selected_wire(ProtobufWire);
    let mut config = config;
    config.listener_quiesce_timeout = timeout;
    let subscriber = Arc::new(Iceoryx2PubSub::with_config(config).with_selected_wire(ProtobufWire));
    let source = UUri::try_from_parts("quiescence", 0x4210, 1, 0x9000).unwrap();
    (publisher, subscriber, source)
}

async fn send(publisher: &Transport, source: &UUri) {
    let metadata = UFrameMetadata::publish(source.clone())
        .with_payload_encoding(PayloadEncoding::PROTOBUF)
        .build()
        .unwrap();
    let mut tx = publisher
        .loan_validated_tx(UTxLoanSpec::payload(metadata, 4, 1).unwrap())
        .await
        .unwrap();
    tx.payload_mut().copy_from_slice(b"live");
    publisher.send_validated_zero_copy(tx).await.unwrap();
}

#[derive(Default)]
struct Blocking {
    entered: Notify,
    release: Notify,
    done: Notify,
    retained: Mutex<Option<Frame>>,
}

#[async_trait]
impl UZeroCopyListener<Frame> for Blocking {
    async fn on_receive_zero_copy(&self, frame: Frame) {
        *self.retained.lock().unwrap() = Some(frame);
        self.entered.notify_one();
        self.release.notified().await;
        assert_eq!(
            self.retained
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .try_contiguous_payload(),
            Some(b"live".as_slice())
        );
        self.done.notify_one();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn unregister_waits_for_an_entered_native_callback() {
    let (publisher, subscriber, source) = pair(Duration::from_secs(5));
    let listener = Arc::new(Blocking::default());
    let registration: Arc<dyn UZeroCopyListener<Frame>> = listener.clone();
    subscriber
        .register_validated_zero_copy_listener(&source, None, registration.clone())
        .await
        .unwrap();
    send(&publisher, &source).await;
    tokio::time::timeout(Duration::from_secs(5), listener.entered.notified())
        .await
        .unwrap();
    let mut unregister = std::pin::pin!(subscriber.unregister_validated_zero_copy_listener(
        &source,
        None,
        registration
    ));
    assert!(
        poll_fn(|cx| Poll::Ready(unregister.as_mut().poll(cx)))
            .await
            .is_pending()
    );
    listener.release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), unregister)
        .await
        .unwrap()
        .unwrap();
    listener.done.notified().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn timeout_does_not_revoke_the_native_sample() {
    let (publisher, subscriber, source) = pair(Duration::from_millis(20));
    let listener = Arc::new(Blocking::default());
    let registration: Arc<dyn UZeroCopyListener<Frame>> = listener.clone();
    subscriber
        .register_validated_zero_copy_listener(&source, None, registration.clone())
        .await
        .unwrap();
    send(&publisher, &source).await;
    tokio::time::timeout(Duration::from_secs(5), listener.entered.notified())
        .await
        .unwrap();
    let error = subscriber
        .unregister_validated_zero_copy_listener(&source, None, registration)
        .await
        .unwrap_err();
    assert_eq!(error.code(), UCode::DeadlineExceeded);
    assert_eq!(
        listener
            .retained
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .try_contiguous_payload(),
        Some(b"live".as_slice())
    );
    listener.release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), listener.done.notified())
        .await
        .unwrap();
    let retained = listener.retained.lock().unwrap().take().unwrap();
    drop(subscriber);
    drop(publisher);
    assert_eq!(retained.try_contiguous_payload(), Some(b"live".as_slice()));
}

struct SelfRemoving {
    own: Weak<Self>,
    transport: Weak<Transport>,
    source: UUri,
    result: Mutex<Option<tokio::sync::oneshot::Sender<Result<(), UStatus>>>>,
}

#[async_trait]
impl UZeroCopyListener<Frame> for SelfRemoving {
    async fn on_receive_zero_copy(&self, frame: Frame) {
        let listener: Arc<dyn UZeroCopyListener<Frame>> = self.own.upgrade().unwrap();
        let result = self
            .transport
            .upgrade()
            .unwrap()
            .unregister_validated_zero_copy_listener(&self.source, None, listener)
            .await;
        assert_eq!(frame.try_contiguous_payload(), Some(b"live".as_slice()));
        self.result
            .lock()
            .unwrap()
            .take()
            .unwrap()
            .send(result)
            .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn native_callback_unregisters_itself_without_waiting_for_itself() {
    let (publisher, subscriber, source) = pair(Duration::from_secs(5));
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let listener = Arc::new_cyclic(|own| SelfRemoving {
        own: own.clone(),
        transport: Arc::downgrade(&subscriber),
        source: source.clone(),
        result: Mutex::new(Some(sender)),
    });
    subscriber
        .register_validated_zero_copy_listener(&source, None, listener)
        .await
        .unwrap();
    send(&publisher, &source).await;
    tokio::time::timeout(Duration::from_secs(2), receiver)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}
