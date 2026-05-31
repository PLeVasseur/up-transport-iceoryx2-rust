// ################################################################################
// Copyright (c) 2026 Contributors to the Eclipse Foundation
//
// SPDX-License-Identifier: Apache-2.0
// ################################################################################

#![allow(
    clippy::cast_possible_truncation,
    clippy::missing_panics_doc,
    clippy::too_many_lines
)]

#[cfg(feature = "payload-contract-benchmarks")]
use std::mem;
use std::{
    cmp,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

use criterion::{
    BenchmarkGroup, BenchmarkId, Criterion, black_box, criterion_group, criterion_main,
};
use tokio::runtime::Runtime;
#[cfg(feature = "payload-contract-benchmarks")]
use up_rust::{
    ProtobufPayload,
    payload::{StablePayload, USerializer},
    zero_copy::ULoanedContiguousZeroCopyRxFrame,
};
use up_rust::{
    UFrameBuilder, UFrameMetadata, UMessageType, UOwnedFrame, UOwnedTransport, UStatus, UUID, UUri,
    payload::{PayloadLayout, RawBytes, UWireError},
    zero_copy::{
        LoanedUninitByteWriter, UFrameView, UTxBuffer, UTxLoanSpec, UUninitTxBuffer,
        UZeroCopyTransport, UZeroCopyUninitTransport, UZeroCopyUninitTransportExt,
    },
};
use up_transport_iceoryx2_rust::{
    BenchmarkOwnedIceoryx2PubSub, Iceoryx2PubSub, Iceoryx2PubSubConfig, MessagingPattern,
    transport::UTransportIceoryx2,
};

#[cfg(feature = "payload-contract-benchmarks")]
#[allow(
    unknown_lints,
    clippy::all,
    unused_attributes,
    dead_code,
    missing_docs,
    non_camel_case_types,
    non_snake_case,
    non_upper_case_globals,
    trivial_casts,
    unused_mut,
    unused_results
)]
mod bench_payload_proto {
    include!(concat!(env!("OUT_DIR"), "/bench_payload.rs"));
}

#[cfg(feature = "payload-contract-benchmarks")]
use bench_payload_proto::BenchPayload;

const CORE_PAYLOAD_CASES: &[(&str, usize)] = &[
    ("empty_present", 0),
    ("can_classic_max", 8),
    ("can_fd_max", 64),
    ("someip_single_mtu", 1_456),
    ("streamer_4k", 4 * 1_024),
    ("radar_ars548_detection_list", 35_336),
    ("streamer_64k", 64 * 1_024),
];
const LARGE_SENSOR_PAYLOAD_CASES: &[(&str, usize)] =
    &[("camera_8mp_3840x2160_raw12_packed", 12_441_600)];
const BENCH_TIMEOUT: Duration = Duration::from_secs(5);
const LARGE_SENSOR_BENCH_TIMEOUT: Duration = Duration::from_secs(30);
const CORE_STATIC_ALLOCATION: usize = 128 * 1_024;
const CAMERA_STATIC_ALLOCATION: usize = 16 * 1_024 * 1_024;
const DIRECT_WRITE_CHUNK: usize = 8 * 1_024;
const UUID_LSB_BASE: u64 = 0x8000_0000_0000_0000;
#[cfg(feature = "payload-contract-benchmarks")]
const PAYLOAD_CONTRACT_SEQUENCE: u32 = 1;
#[cfg(feature = "payload-contract-benchmarks")]
const PAYLOAD_CONTRACT_FILL_BYTE: u8 = 0x5a;
#[cfg(feature = "payload-contract-benchmarks")]
const PAYLOAD_CONTRACT_CORE_CASES: &[PayloadContractCase] = &[
    PayloadContractCase::new(1, "can_classic_max", 8),
    PayloadContractCase::new(2, "can_fd_max", 64),
    PayloadContractCase::new(3, "someip_single_mtu", 1_456),
    PayloadContractCase::new(4, "streamer_4k", 4 * 1_024),
    PayloadContractCase::new(5, "radar_ars548_detection_list", 35_336),
    PayloadContractCase::new(6, "streamer_64k", 64 * 1_024),
];
#[cfg(feature = "payload-contract-benchmarks")]
const PAYLOAD_CONTRACT_LARGE_SENSOR_CASES: &[PayloadContractCase] = &[PayloadContractCase::new(
    7,
    "camera_8mp_3840x2160_raw12_packed",
    12_441_600,
)];

#[cfg(feature = "payload-contract-benchmarks")]
#[repr(C)]
#[derive(up_rust::StablePayload, up_rust::ByteBackedStablePayload, up_rust::StablePayloadInit)]
#[stable_payload(type_name = "org.eclipse.uprotocol.bench.StableBenchHeader")]
struct StableBenchHeader {
    case_id: u32,
    sequence: u32,
    logical_payload_len: u32,
}

#[cfg(feature = "payload-contract-benchmarks")]
trait StableBenchPayloadView: StablePayload {
    fn header(&self) -> &StableBenchHeader;
    fn checksum(&self) -> u32;
    fn payload(&self) -> &[u8];
}

#[cfg(feature = "payload-contract-benchmarks")]
macro_rules! define_stable_bench_payload {
    ($name:ident, $type_name:literal, $payload_len:expr) => {
        #[repr(C)]
        #[derive(
            up_rust::StablePayload, up_rust::ByteBackedStablePayload, up_rust::StablePayloadInit,
        )]
        #[stable_payload(type_name = $type_name)]
        struct $name {
            header: StableBenchHeader,
            checksum: u32,
            payload: [u8; $payload_len],
        }

        impl StableBenchPayloadView for $name {
            fn header(&self) -> &StableBenchHeader {
                &self.header
            }

            fn checksum(&self) -> u32 {
                self.checksum
            }

            fn payload(&self) -> &[u8] {
                &self.payload
            }
        }
    };
}

#[cfg(feature = "payload-contract-benchmarks")]
define_stable_bench_payload!(
    StableBenchPayload8,
    "org.eclipse.uprotocol.bench.StableBenchPayload8",
    8
);
#[cfg(feature = "payload-contract-benchmarks")]
define_stable_bench_payload!(
    StableBenchPayload64,
    "org.eclipse.uprotocol.bench.StableBenchPayload64",
    64
);
#[cfg(feature = "payload-contract-benchmarks")]
define_stable_bench_payload!(
    StableBenchPayload1456,
    "org.eclipse.uprotocol.bench.StableBenchPayload1456",
    1_456
);
#[cfg(feature = "payload-contract-benchmarks")]
define_stable_bench_payload!(
    StableBenchPayload4096,
    "org.eclipse.uprotocol.bench.StableBenchPayload4096",
    4 * 1_024
);
#[cfg(feature = "payload-contract-benchmarks")]
define_stable_bench_payload!(
    StableBenchPayload35336,
    "org.eclipse.uprotocol.bench.StableBenchPayload35336",
    35_336
);
#[cfg(feature = "payload-contract-benchmarks")]
define_stable_bench_payload!(
    StableBenchPayload65536,
    "org.eclipse.uprotocol.bench.StableBenchPayload65536",
    64 * 1_024
);
#[cfg(feature = "payload-contract-benchmarks")]
define_stable_bench_payload!(
    StableBenchPayload12441600,
    "org.eclipse.uprotocol.bench.StableBenchPayload12441600",
    12_441_600
);

#[derive(Clone, Copy)]
enum BenchSuite {
    Raw,
    PayloadContract,
    All,
}

impl BenchSuite {
    fn from_env() -> Self {
        match std::env::var("TRANSPORT_BENCH_SUITE")
            .unwrap_or_else(|_| "raw".to_string())
            .as_str()
        {
            "raw" => Self::Raw,
            "payload-contract" => Self::PayloadContract,
            "all" => Self::All,
            other => panic!(
                "TRANSPORT_BENCH_SUITE must be one of raw, payload-contract, all; got {other}"
            ),
        }
    }

    fn includes_raw(self) -> bool {
        matches!(self, Self::Raw | Self::All)
    }

    fn includes_payload_contract(self) -> bool {
        matches!(self, Self::PayloadContract | Self::All)
    }
}

#[derive(Clone, Copy)]
enum BenchProfile {
    Core,
    Camera,
    All,
}

impl BenchProfile {
    fn from_env() -> Self {
        match std::env::var("TRANSPORT_BENCH_PROFILE")
            .unwrap_or_else(|_| "all".to_string())
            .as_str()
        {
            "core" => Self::Core,
            "camera" => Self::Camera,
            "all" => Self::All,
            other => {
                panic!("TRANSPORT_BENCH_PROFILE must be one of core, camera, all; got {other}")
            }
        }
    }

    fn includes_core(self) -> bool {
        matches!(self, Self::Core | Self::All)
    }

    fn includes_camera(self) -> bool {
        matches!(self, Self::Camera | Self::All)
    }
}

#[derive(Clone, Copy)]
enum BenchPath {
    Owned,
    ZeroCopyLoanCopy,
    ZeroCopyUninitDirect,
}

impl BenchPath {
    fn label(self) -> &'static str {
        match self {
            Self::Owned => "owned",
            Self::ZeroCopyLoanCopy => "zero_copy_loan_copy",
            Self::ZeroCopyUninitDirect => "zero_copy_uninit_direct",
        }
    }
}

#[derive(Clone, Copy)]
enum BenchMessageType {
    Publish,
    Notification,
    Request,
    Response,
}

impl BenchMessageType {
    fn label(self) -> &'static str {
        match self {
            Self::Publish => "publish",
            Self::Notification => "notification",
            Self::Request => "request",
            Self::Response => "response",
        }
    }

    fn message_type(self) -> UMessageType {
        match self {
            Self::Publish => UMessageType::Publish,
            Self::Notification => UMessageType::Notification,
            Self::Request => UMessageType::Request,
            Self::Response => UMessageType::Response,
        }
    }
}

#[derive(Clone)]
struct BenchCase {
    message_type: BenchMessageType,
    payload_case_id: &'static str,
    payload_len: usize,
    source: UUri,
    sink: Option<UUri>,
    request_id: Option<UUID>,
}

impl BenchCase {
    fn new(
        message_type: BenchMessageType,
        payload_case_id: &'static str,
        payload_len: usize,
    ) -> Self {
        let sequence = next_sequence();
        let authority = format!("iox-bench-{}-{sequence}", std::process::id());
        let source_resource = resource_id(0x9000, sequence);
        let method_resource = resource_id(0x1000, sequence);
        match message_type {
            BenchMessageType::Publish => Self {
                message_type,
                payload_case_id,
                payload_len,
                source: uri(&authority, 0x4210, source_resource),
                sink: None,
                request_id: None,
            },
            BenchMessageType::Notification => Self {
                message_type,
                payload_case_id,
                payload_len,
                source: uri(&authority, 0x4211, source_resource),
                sink: Some(uri(&authority, 0x4220, 0)),
                request_id: None,
            },
            BenchMessageType::Request => Self {
                message_type,
                payload_case_id,
                payload_len,
                source: uri(&authority, 0x4300, 0),
                sink: Some(uri(&authority, 0x4310, method_resource)),
                request_id: None,
            },
            BenchMessageType::Response => Self {
                message_type,
                payload_case_id,
                payload_len,
                source: uri(&authority, 0x4310, method_resource),
                sink: Some(uri(&authority, 0x4300, 0)),
                request_id: Some(uuid_for(sequence.saturating_add(10_000))),
            },
        }
    }

    fn benchmark_id(&self, path: BenchPath) -> BenchmarkId {
        BenchmarkId::new(
            path.label(),
            format!(
                "{}/{}/{}",
                self.message_type.label(),
                self.payload_case_id,
                self.payload_len
            ),
        )
    }

    fn no_payload_benchmark_id(&self, path: BenchPath) -> BenchmarkId {
        BenchmarkId::new(path.label(), self.message_type.label())
    }

    fn builder(&self, id: UUID) -> UFrameBuilder {
        match self.message_type {
            BenchMessageType::Publish => UFrameBuilder::publish(self.source.clone()),
            BenchMessageType::Notification => UFrameBuilder::notification(
                self.source.clone(),
                self.sink.clone().expect("notification sink"),
            ),
            BenchMessageType::Request => UFrameBuilder::request(
                self.sink.clone().expect("request method"),
                self.source.clone(),
                5_000,
            ),
            BenchMessageType::Response => UFrameBuilder::response(
                self.sink.clone().expect("response reply-to"),
                self.request_id.clone().expect("response request id"),
                self.source.clone(),
            ),
        }
        .with_message_id(id)
    }

    fn metadata(&self, id: UUID, payload_present: bool) -> UFrameMetadata {
        let builder = self.builder(id);
        if payload_present {
            builder.with_encoding(RawBytes::encoding()).build_metadata()
        } else {
            builder.build_metadata()
        }
        .expect("valid benchmark metadata")
    }

    fn owned_frame(
        &self,
        id: UUID,
        payload: &PreparedPayload,
        payload_present: bool,
    ) -> UOwnedFrame {
        let builder = self.builder(id);
        if payload_present {
            builder
                .build_with_raw_payload(
                    payload.bytes().expect("precomputed payload bytes").to_vec(),
                )
                .expect("valid owned benchmark frame")
        } else {
            builder.build().expect("valid no-payload benchmark frame")
        }
    }
}

struct PreparedPayload {
    bytes: Option<Vec<u8>>,
    len: usize,
    checksum: u64,
}

impl PreparedPayload {
    fn precomputed(len: usize) -> Self {
        let mut payload = vec![0_u8; len];
        fill_pattern(&mut payload, 0);
        Self {
            checksum: checksum_bytes(0, &payload),
            bytes: Some(payload),
            len,
        }
    }

    fn direct(len: usize) -> Self {
        Self {
            bytes: None,
            len,
            checksum: checksum_for_len(len),
        }
    }

    fn for_path(path: BenchPath, len: usize) -> Self {
        match path {
            BenchPath::ZeroCopyUninitDirect => Self::direct(len),
            BenchPath::Owned | BenchPath::ZeroCopyLoanCopy => Self::precomputed(len),
        }
    }

    fn no_payload_for(path: BenchPath) -> Self {
        match path {
            BenchPath::ZeroCopyUninitDirect => Self::direct(0),
            BenchPath::Owned | BenchPath::ZeroCopyLoanCopy => Self::precomputed(0),
        }
    }

    fn bytes(&self) -> Option<&[u8]> {
        self.bytes.as_deref()
    }
}

struct ReceivedAck {
    id: UUID,
    message_type: UMessageType,
    has_payload: bool,
    payload_len: usize,
    checksum: u64,
}

#[cfg(feature = "payload-contract-benchmarks")]
#[derive(Clone, Copy)]
struct PayloadContractCase {
    case_id: u32,
    payload_case_id: &'static str,
    logical_payload_len: usize,
}

#[cfg(feature = "payload-contract-benchmarks")]
impl PayloadContractCase {
    const fn new(case_id: u32, payload_case_id: &'static str, logical_payload_len: usize) -> Self {
        Self {
            case_id,
            payload_case_id,
            logical_payload_len,
        }
    }
}

#[cfg(feature = "payload-contract-benchmarks")]
#[derive(Clone, Copy)]
enum PayloadContractPath {
    ProtobufOwnedFull,
    StableZcNoZeroFull,
}

#[cfg(feature = "payload-contract-benchmarks")]
impl PayloadContractPath {
    fn label(self) -> &'static str {
        match self {
            Self::ProtobufOwnedFull => "protobuf_owned_full",
            Self::StableZcNoZeroFull => "stable_zc_nozero_full",
        }
    }
}

#[cfg(feature = "payload-contract-benchmarks")]
struct PayloadContractAck {
    id: UUID,
    message_type: UMessageType,
    case_id: u32,
    sequence: u32,
    logical_payload_len: usize,
    transported_payload_len: usize,
    checksum: u32,
    first_payload_byte: u8,
    last_payload_byte: u8,
}

struct BenchTransports {
    zero_copy: Arc<Iceoryx2PubSub>,
    owned: Arc<BenchmarkOwnedIceoryx2PubSub>,
}

impl BenchTransports {
    fn build(max_slice_len: usize) -> Self {
        let config = Iceoryx2PubSubConfig::static_allocation(max_slice_len)
            .with_pull_mismatch_queue_capacity(4_096);
        let zero_copy =
            UTransportIceoryx2::build_with_config(MessagingPattern::PublishSubscribe, config)
                .expect("iceoryx2 benchmark transport should build");
        let owned = Arc::new(BenchmarkOwnedIceoryx2PubSub::new(zero_copy.clone()));
        Self { zero_copy, owned }
    }
}

fn bench_payload_matrix(
    c: &mut Criterion,
    runtime: &Runtime,
    transports: &BenchTransports,
    group_name: &'static str,
    payload_cases: &[(&'static str, usize)],
    timeout: Duration,
    send_receive: bool,
) {
    let mut group = c.benchmark_group(group_name);
    for (payload_case_id, payload_len) in payload_cases {
        for path in [
            BenchPath::Owned,
            BenchPath::ZeroCopyLoanCopy,
            BenchPath::ZeroCopyUninitDirect,
        ] {
            for message_type in [
                BenchMessageType::Publish,
                BenchMessageType::Notification,
                BenchMessageType::Request,
                BenchMessageType::Response,
            ] {
                let case = BenchCase::new(message_type, payload_case_id, *payload_len);
                runtime.block_on(prime_subscriber(transports, &case));
                if send_receive {
                    bench_send_receive_case(runtime, transports, &mut group, path, case, timeout);
                } else {
                    bench_tx_only_case(runtime, transports, &mut group, path, case);
                }
            }
        }
    }
    group.finish();
}

fn bench_send_receive_case(
    runtime: &Runtime,
    transports: &BenchTransports,
    group: &mut BenchmarkGroup<'_, criterion::measurement::WallTime>,
    path: BenchPath,
    case: BenchCase,
    timeout: Duration,
) {
    let payload = PreparedPayload::for_path(path, case.payload_len);
    group.bench_function(case.benchmark_id(path), |b| {
        b.iter(|| {
            runtime.block_on(async {
                let id = next_uuid();
                send_path(transports, path, &case, id.clone(), &payload, true).await;
                let ack =
                    receive_matching_ack(transports, path, &case, &id, true, &payload, timeout)
                        .await;
                black_box(ack.payload_len);
                black_box(ack.checksum);
                black_box(case.message_type.label());
            });
        });
    });
}

fn bench_tx_only_case(
    runtime: &Runtime,
    transports: &BenchTransports,
    group: &mut BenchmarkGroup<'_, criterion::measurement::WallTime>,
    path: BenchPath,
    case: BenchCase,
) {
    let payload = PreparedPayload::for_path(path, case.payload_len);
    group.bench_function(case.benchmark_id(path), |b| {
        b.iter_custom(|iterations| {
            runtime.block_on(async {
                let mut elapsed = Duration::ZERO;
                for _ in 0..iterations {
                    let id = next_uuid();
                    let started = Instant::now();
                    send_path(transports, path, &case, id.clone(), &payload, true).await;
                    elapsed = elapsed.saturating_add(started.elapsed());
                    let ack = receive_matching_ack(
                        transports,
                        path,
                        &case,
                        &id,
                        true,
                        &payload,
                        BENCH_TIMEOUT,
                    )
                    .await;
                    black_box(ack.checksum);
                }
                elapsed
            })
        });
    });
}

fn bench_no_payload_smoke(c: &mut Criterion, runtime: &Runtime, transports: &BenchTransports) {
    let mut group = c.benchmark_group("transport_no_payload_smoke");
    for path in [
        BenchPath::Owned,
        BenchPath::ZeroCopyLoanCopy,
        BenchPath::ZeroCopyUninitDirect,
    ] {
        for message_type in [
            BenchMessageType::Publish,
            BenchMessageType::Notification,
            BenchMessageType::Request,
            BenchMessageType::Response,
        ] {
            let case = BenchCase::new(message_type, "no_payload", 0);
            runtime.block_on(prime_subscriber(transports, &case));
            let payload = PreparedPayload::no_payload_for(path);
            group.bench_function(case.no_payload_benchmark_id(path), |b| {
                b.iter(|| {
                    runtime.block_on(async {
                        let id = next_uuid();
                        send_path(transports, path, &case, id.clone(), &payload, false).await;
                        let ack = receive_matching_ack(
                            transports,
                            path,
                            &case,
                            &id,
                            false,
                            &payload,
                            BENCH_TIMEOUT,
                        )
                        .await;
                        black_box(ack.message_type);
                    });
                });
            });
        }
    }
    group.finish();
}

async fn prime_subscriber(transports: &BenchTransports, case: &BenchCase) {
    match transports
        .zero_copy
        .receive_zero_copy(&case.source, case.sink.as_ref())
        .await
    {
        Ok(frame) => drop(frame),
        Err(status) if status.get_code() == up_rust::UCode::NOT_FOUND => {}
        Err(status) => panic!("failed to prime iceoryx2 pull subscriber: {status:?}"),
    }
}

async fn send_path(
    transports: &BenchTransports,
    path: BenchPath,
    case: &BenchCase,
    id: UUID,
    payload: &PreparedPayload,
    payload_present: bool,
) {
    match path {
        BenchPath::Owned => {
            let frame = case.owned_frame(id, payload, payload_present);
            transports
                .owned
                .send_owned(frame)
                .await
                .expect("iceoryx2 benchmark owned send should succeed");
        }
        BenchPath::ZeroCopyLoanCopy => {
            let metadata = case.metadata(id, payload_present);
            let mut loan = transports
                .zero_copy
                .loan_tx(
                    loan_spec(metadata, payload.len, payload_present).expect("valid loan spec"),
                )
                .await
                .expect("iceoryx2 zero-copy loan-copy loan should succeed");
            if payload_present {
                loan.payload_mut()
                    .copy_from_slice(payload.bytes().expect("precomputed payload bytes"));
            }
            transports
                .zero_copy
                .send_zero_copy(loan)
                .await
                .expect("iceoryx2 zero-copy loan-copy send should succeed");
        }
        BenchPath::ZeroCopyUninitDirect => {
            let metadata = case.metadata(id, payload_present);
            if payload_present {
                transports
                    .zero_copy
                    .send_uninit_loaned_bytes_as::<RawBytes>(metadata, payload.len, 1, |writer| {
                        write_pattern_to_uninit_writer(writer)
                    })
                    .await
                    .expect("iceoryx2 zero-copy uninit-direct send should succeed");
            } else {
                let loan = transports
                    .zero_copy
                    .loan_uninit_tx(
                        UTxLoanSpec::no_payload(metadata).expect("valid no-payload spec"),
                    )
                    .await
                    .expect("iceoryx2 no-payload uninit loan should succeed");
                // SAFETY: a no-payload loan has an empty visible payload range.
                let loan = unsafe { loan.assume_payload_init() };
                transports
                    .zero_copy
                    .send_zero_copy(loan)
                    .await
                    .expect("iceoryx2 no-payload uninit send should succeed");
            }
        }
    }
}

async fn receive_matching_ack(
    transports: &BenchTransports,
    path: BenchPath,
    case: &BenchCase,
    expected_id: &UUID,
    payload_present: bool,
    payload: &PreparedPayload,
    timeout: Duration,
) -> ReceivedAck {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "timed out waiting for matching iceoryx2 benchmark frame"
        );
        let result = match path {
            BenchPath::Owned => tokio::time::timeout(
                remaining,
                transports
                    .owned
                    .receive_owned(&case.source, case.sink.as_ref()),
            )
            .await
            .expect("timed out waiting for iceoryx2 owned receive")
            .map(owned_ack),
            BenchPath::ZeroCopyLoanCopy | BenchPath::ZeroCopyUninitDirect => tokio::time::timeout(
                remaining,
                transports
                    .zero_copy
                    .receive_zero_copy(&case.source, case.sink.as_ref()),
            )
            .await
            .expect("timed out waiting for iceoryx2 zero-copy receive")
            .map(|frame| lease_ack(&frame)),
        };
        match result {
            Ok(ack) if &ack.id == expected_id => {
                assert_eq!(ack.message_type, case.message_type.message_type());
                assert_eq!(ack.has_payload, payload_present);
                assert_eq!(
                    ack.payload_len,
                    if payload_present { payload.len } else { 0 }
                );
                assert_eq!(
                    ack.checksum,
                    if payload_present { payload.checksum } else { 0 }
                );
                return ack;
            }
            Ok(_) => continue,
            Err(status) if status.get_code() == up_rust::UCode::NOT_FOUND => {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            Err(status) => panic!("unexpected iceoryx2 receive error: {status:?}"),
        }
    }
}

fn owned_ack(frame: UOwnedFrame) -> ReceivedAck {
    ReceivedAck {
        id: frame.metadata().attributes().id().clone(),
        message_type: frame.metadata().attributes().message_type(),
        has_payload: frame.has_payload(),
        payload_len: frame.payload_bytes().len(),
        checksum: checksum_bytes(0, frame.payload_bytes()),
    }
}

fn lease_ack(frame: &impl UFrameView) -> ReceivedAck {
    ReceivedAck {
        id: frame.metadata().attributes().id().clone(),
        message_type: frame.metadata().attributes().message_type(),
        has_payload: frame.has_payload(),
        payload_len: frame.payload_len(),
        checksum: frame.payload_slices().fold(0_u64, checksum_bytes),
    }
}

fn loan_spec(
    metadata: UFrameMetadata,
    payload_len: usize,
    payload_present: bool,
) -> Result<UTxLoanSpec, UStatus> {
    if !payload_present {
        return UTxLoanSpec::no_payload(metadata);
    }
    if payload_len == 0 {
        return UTxLoanSpec::present_empty_payload(metadata);
    }
    let layout = PayloadLayout::new(payload_len, 1).map_err(UStatus::from)?;
    UTxLoanSpec::payload(metadata, layout)
}

fn write_pattern_to_uninit_writer<'a>(
    mut writer: LoanedUninitByteWriter<'a>,
) -> Result<LoanedUninitByteWriter<'a>, UWireError> {
    let mut offset = 0;
    let mut chunk = [0_u8; DIRECT_WRITE_CHUNK];
    while offset < writer.len() {
        let take = cmp::min(chunk.len(), writer.len() - offset);
        fill_pattern(&mut chunk[..take], offset);
        writer.write_all(&chunk[..take])?;
        offset += take;
    }
    Ok(writer)
}

#[cfg(feature = "payload-contract-benchmarks")]
fn bench_payload_contract_matrix(
    c: &mut Criterion,
    runtime: &Runtime,
    transports: &BenchTransports,
    group_name: &'static str,
    payload_cases: &[PayloadContractCase],
    timeout: Duration,
) {
    let mut group = c.benchmark_group(group_name);
    for contract in payload_cases {
        for path in [
            PayloadContractPath::ProtobufOwnedFull,
            PayloadContractPath::StableZcNoZeroFull,
        ] {
            let case = BenchCase::new(
                BenchMessageType::Publish,
                contract.payload_case_id,
                contract.logical_payload_len,
            );
            runtime.block_on(prime_subscriber(transports, &case));
            let transported_payload_len = payload_contract_transported_len(path, contract);
            group.bench_function(
                BenchmarkId::new(
                    path.label(),
                    format!(
                        "publish/{}/{}/{}",
                        contract.payload_case_id,
                        contract.logical_payload_len,
                        transported_payload_len
                    ),
                ),
                |b| {
                    b.iter(|| {
                        runtime.block_on(async {
                            let id = next_uuid();
                            send_payload_contract_path(
                                transports,
                                path,
                                &case,
                                id.clone(),
                                contract,
                            )
                            .await;
                            let ack = receive_payload_contract_ack(
                                transports,
                                path,
                                &case,
                                &id,
                                contract,
                                transported_payload_len,
                                timeout,
                            )
                            .await;
                            black_box(ack.logical_payload_len);
                            black_box(ack.transported_payload_len);
                            black_box(ack.checksum);
                        });
                    });
                },
            );
        }
    }
    group.finish();
}

#[cfg(feature = "payload-contract-benchmarks")]
async fn send_payload_contract_path(
    transports: &BenchTransports,
    path: PayloadContractPath,
    case: &BenchCase,
    id: UUID,
    contract: &PayloadContractCase,
) {
    match path {
        PayloadContractPath::ProtobufOwnedFull => {
            let payload = build_bench_payload(contract);
            let metadata = case.builder(id).build_metadata().expect("valid metadata");
            let frame = UOwnedFrame::from_serializable::<ProtobufPayload, _>(metadata, &payload)
                .expect("protobuf benchmark payload should serialize");
            transports
                .owned
                .send_owned(frame)
                .await
                .expect("iceoryx2 payload-contract protobuf send should succeed");
        }
        PayloadContractPath::StableZcNoZeroFull => {
            let metadata = case.builder(id).build_metadata().expect("valid metadata");
            send_stable_payload_contract(transports, metadata, contract).await;
        }
    }
}

#[cfg(feature = "payload-contract-benchmarks")]
async fn send_stable_payload_contract(
    transports: &BenchTransports,
    metadata: UFrameMetadata,
    contract: &PayloadContractCase,
) {
    macro_rules! send_stable {
        ($payload_ty:ty) => {
            transports
                .zero_copy
                .send_uninit_stable_payload_as::<$payload_ty>(metadata, |payload| {
                    payload
                        .header(|header| {
                            header
                                .case_id(contract.case_id)
                                .sequence(PAYLOAD_CONTRACT_SEQUENCE)
                                .logical_payload_len(logical_payload_len_u32(contract))
                                .finish()
                        })?
                        .checksum(payload_contract_checksum(contract))
                        .payload_fill(PAYLOAD_CONTRACT_FILL_BYTE)
                        .finish()
                })
                .await
                .expect("iceoryx2 payload-contract stable no-zero send should succeed")
        };
    }

    match contract.logical_payload_len {
        8 => send_stable!(StableBenchPayload8),
        64 => send_stable!(StableBenchPayload64),
        1_456 => send_stable!(StableBenchPayload1456),
        4_096 => send_stable!(StableBenchPayload4096),
        35_336 => send_stable!(StableBenchPayload35336),
        65_536 => send_stable!(StableBenchPayload65536),
        12_441_600 => send_stable!(StableBenchPayload12441600),
        other => panic!("unsupported stable payload-contract size {other}"),
    }
}

#[cfg(feature = "payload-contract-benchmarks")]
async fn receive_payload_contract_ack(
    transports: &BenchTransports,
    path: PayloadContractPath,
    case: &BenchCase,
    expected_id: &UUID,
    contract: &PayloadContractCase,
    expected_transported_payload_len: usize,
    timeout: Duration,
) -> PayloadContractAck {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "timed out waiting for matching iceoryx2 payload-contract frame"
        );
        let result = match path {
            PayloadContractPath::ProtobufOwnedFull => tokio::time::timeout(
                remaining,
                transports
                    .owned
                    .receive_owned(&case.source, case.sink.as_ref()),
            )
            .await
            .expect("timed out waiting for iceoryx2 payload-contract owned receive")
            .map(protobuf_payload_contract_ack),
            PayloadContractPath::StableZcNoZeroFull => tokio::time::timeout(
                remaining,
                transports
                    .zero_copy
                    .receive_zero_copy(&case.source, case.sink.as_ref()),
            )
            .await
            .expect("timed out waiting for iceoryx2 payload-contract zero-copy receive")
            .map(|frame| stable_payload_contract_ack_for_len(&frame, contract.logical_payload_len)),
        };
        match result {
            Ok(ack) if &ack.id == expected_id => {
                assert_eq!(ack.message_type, UMessageType::Publish);
                assert_eq!(ack.case_id, contract.case_id);
                assert_eq!(ack.sequence, PAYLOAD_CONTRACT_SEQUENCE);
                assert_eq!(ack.logical_payload_len, contract.logical_payload_len);
                assert_eq!(
                    ack.transported_payload_len,
                    expected_transported_payload_len
                );
                assert_eq!(ack.checksum, payload_contract_checksum(contract));
                assert_eq!(ack.first_payload_byte, PAYLOAD_CONTRACT_FILL_BYTE);
                assert_eq!(ack.last_payload_byte, PAYLOAD_CONTRACT_FILL_BYTE);
                return ack;
            }
            Ok(_) => continue,
            Err(status) if status.get_code() == up_rust::UCode::NOT_FOUND => {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            Err(status) => panic!("unexpected iceoryx2 payload-contract receive error: {status:?}"),
        }
    }
}

#[cfg(feature = "payload-contract-benchmarks")]
fn protobuf_payload_contract_ack(frame: UOwnedFrame) -> PayloadContractAck {
    let transported_payload_len = frame.payload_bytes().len();
    let id = frame.metadata().attributes().id().clone();
    let message_type = frame.metadata().attributes().message_type();
    let payload: BenchPayload = frame
        .deserialize::<ProtobufPayload, _>()
        .expect("protobuf payload-contract frame should deserialize");
    PayloadContractAck {
        id,
        message_type,
        case_id: payload.case_id,
        sequence: payload.sequence,
        logical_payload_len: usize::try_from(payload.logical_payload_len)
            .expect("payload len fits usize"),
        transported_payload_len,
        checksum: payload.checksum,
        first_payload_byte: *payload.payload.first().expect("payload is non-empty"),
        last_payload_byte: *payload.payload.last().expect("payload is non-empty"),
    }
}

#[cfg(feature = "payload-contract-benchmarks")]
fn stable_payload_contract_ack_for_len(
    frame: &impl ULoanedContiguousZeroCopyRxFrame,
    logical_payload_len: usize,
) -> PayloadContractAck {
    match logical_payload_len {
        8 => stable_payload_contract_ack::<StableBenchPayload8>(frame),
        64 => stable_payload_contract_ack::<StableBenchPayload64>(frame),
        1_456 => stable_payload_contract_ack::<StableBenchPayload1456>(frame),
        4_096 => stable_payload_contract_ack::<StableBenchPayload4096>(frame),
        35_336 => stable_payload_contract_ack::<StableBenchPayload35336>(frame),
        65_536 => stable_payload_contract_ack::<StableBenchPayload65536>(frame),
        12_441_600 => stable_payload_contract_ack::<StableBenchPayload12441600>(frame),
        other => panic!("unsupported stable payload-contract size {other}"),
    }
}

#[cfg(feature = "payload-contract-benchmarks")]
fn stable_payload_contract_ack<T>(
    frame: &impl ULoanedContiguousZeroCopyRxFrame,
) -> PayloadContractAck
where
    T: StableBenchPayloadView,
{
    black_box(
        frame
            .payload_loan_provenance()
            .expect("stable payload should be loan-backed"),
    );
    let payload = frame
        .borrow_stable_payload::<T>()
        .expect("stable payload-contract frame should borrow");
    PayloadContractAck {
        id: frame.metadata().attributes().id().clone(),
        message_type: frame.metadata().attributes().message_type(),
        case_id: payload.header().case_id,
        sequence: payload.header().sequence,
        logical_payload_len: usize::try_from(payload.header().logical_payload_len)
            .expect("payload len fits usize"),
        transported_payload_len: frame.payload_len(),
        checksum: payload.checksum(),
        first_payload_byte: *payload.payload().first().expect("payload is non-empty"),
        last_payload_byte: *payload.payload().last().expect("payload is non-empty"),
    }
}

#[cfg(feature = "payload-contract-benchmarks")]
fn payload_contract_transported_len(
    path: PayloadContractPath,
    contract: &PayloadContractCase,
) -> usize {
    match path {
        PayloadContractPath::ProtobufOwnedFull => {
            let payload = build_bench_payload(contract);
            <BenchPayload as USerializer<ProtobufPayload>>::encoded_len(&payload)
        }
        PayloadContractPath::StableZcNoZeroFull => match contract.logical_payload_len {
            8 => mem::size_of::<StableBenchPayload8>(),
            64 => mem::size_of::<StableBenchPayload64>(),
            1_456 => mem::size_of::<StableBenchPayload1456>(),
            4_096 => mem::size_of::<StableBenchPayload4096>(),
            35_336 => mem::size_of::<StableBenchPayload35336>(),
            65_536 => mem::size_of::<StableBenchPayload65536>(),
            12_441_600 => mem::size_of::<StableBenchPayload12441600>(),
            other => panic!("unsupported stable payload-contract size {other}"),
        },
    }
}

#[cfg(feature = "payload-contract-benchmarks")]
fn build_bench_payload(contract: &PayloadContractCase) -> BenchPayload {
    let mut payload = BenchPayload::new();
    payload.case_id = contract.case_id;
    payload.sequence = PAYLOAD_CONTRACT_SEQUENCE;
    payload.logical_payload_len = logical_payload_len_u32(contract);
    payload.checksum = payload_contract_checksum(contract);
    payload.payload = vec![PAYLOAD_CONTRACT_FILL_BYTE; contract.logical_payload_len];
    payload
}

#[cfg(feature = "payload-contract-benchmarks")]
fn logical_payload_len_u32(contract: &PayloadContractCase) -> u32 {
    u32::try_from(contract.logical_payload_len).expect("payload len fits u32")
}

#[cfg(feature = "payload-contract-benchmarks")]
fn payload_contract_checksum(contract: &PayloadContractCase) -> u32 {
    0xace0_0000
        ^ contract.case_id
        ^ PAYLOAD_CONTRACT_SEQUENCE
        ^ logical_payload_len_u32(contract)
        ^ u32::from(PAYLOAD_CONTRACT_FILL_BYTE)
}

fn preflight(
    runtime: &Runtime,
    transports: &BenchTransports,
    fit_payload_len: usize,
    timeout: Duration,
) {
    runtime.block_on(async {
        let warm = BenchCase::new(BenchMessageType::Publish, "preflight", 8);
        prime_subscriber(transports, &warm).await;
        let payload = PreparedPayload::precomputed(8);
        let id = next_uuid();
        send_path(
            transports,
            BenchPath::ZeroCopyLoanCopy,
            &warm,
            id.clone(),
            &payload,
            true,
        )
        .await;
        let ack = receive_matching_ack(
            transports,
            BenchPath::ZeroCopyLoanCopy,
            &warm,
            &id,
            true,
            &payload,
            BENCH_TIMEOUT,
        )
        .await;
        black_box(ack.checksum);

        let fit = BenchCase::new(BenchMessageType::Publish, "preflight_fit", fit_payload_len);
        prime_subscriber(transports, &fit).await;
        let payload = PreparedPayload::precomputed(fit_payload_len);
        let id = next_uuid();
        send_path(
            transports,
            BenchPath::ZeroCopyLoanCopy,
            &fit,
            id.clone(),
            &payload,
            true,
        )
        .await;
        let ack = receive_matching_ack(
            transports,
            BenchPath::ZeroCopyLoanCopy,
            &fit,
            &id,
            true,
            &payload,
            timeout,
        )
        .await;
        black_box(ack.checksum);
    });
}

fn bench_transport(c: &mut Criterion) {
    let suite = BenchSuite::from_env();
    let profile = BenchProfile::from_env();
    let runtime = Runtime::new().expect("tokio runtime");
    if suite.includes_raw() && profile.includes_core() {
        let transports = runtime.block_on(async { BenchTransports::build(CORE_STATIC_ALLOCATION) });
        preflight(&runtime, &transports, 64 * 1_024, BENCH_TIMEOUT);
        bench_payload_matrix(
            c,
            &runtime,
            &transports,
            "transport_send_receive",
            CORE_PAYLOAD_CASES,
            BENCH_TIMEOUT,
            true,
        );
        bench_payload_matrix(
            c,
            &runtime,
            &transports,
            "transport_tx_only",
            CORE_PAYLOAD_CASES,
            BENCH_TIMEOUT,
            false,
        );
        bench_no_payload_smoke(c, &runtime, &transports);
    }
    if suite.includes_raw() && profile.includes_camera() {
        let transports =
            runtime.block_on(async { BenchTransports::build(CAMERA_STATIC_ALLOCATION) });
        preflight(
            &runtime,
            &transports,
            12_441_600,
            LARGE_SENSOR_BENCH_TIMEOUT,
        );
        bench_payload_matrix(
            c,
            &runtime,
            &transports,
            "transport_large_sensor_send_receive",
            LARGE_SENSOR_PAYLOAD_CASES,
            LARGE_SENSOR_BENCH_TIMEOUT,
            true,
        );
        bench_payload_matrix(
            c,
            &runtime,
            &transports,
            "transport_large_sensor_tx_only",
            LARGE_SENSOR_PAYLOAD_CASES,
            LARGE_SENSOR_BENCH_TIMEOUT,
            false,
        );
    }
    if suite.includes_payload_contract() {
        bench_payload_contract(c, &runtime, profile);
    }
}

#[cfg(feature = "payload-contract-benchmarks")]
fn bench_payload_contract(c: &mut Criterion, runtime: &Runtime, profile: BenchProfile) {
    if profile.includes_core() {
        let transports = runtime.block_on(async { BenchTransports::build(CORE_STATIC_ALLOCATION) });
        preflight(&runtime, &transports, 64 * 1_024, BENCH_TIMEOUT);
        bench_payload_contract_matrix(
            c,
            runtime,
            &transports,
            "transport_payload_contract_core",
            PAYLOAD_CONTRACT_CORE_CASES,
            BENCH_TIMEOUT,
        );
    }
    if profile.includes_camera() {
        let transports =
            runtime.block_on(async { BenchTransports::build(CAMERA_STATIC_ALLOCATION) });
        preflight(runtime, &transports, 12_441_600, LARGE_SENSOR_BENCH_TIMEOUT);
        bench_payload_contract_matrix(
            c,
            runtime,
            &transports,
            "transport_payload_contract_large_sensor",
            PAYLOAD_CONTRACT_LARGE_SENSOR_CASES,
            LARGE_SENSOR_BENCH_TIMEOUT,
        );
    }
}

#[cfg(not(feature = "payload-contract-benchmarks"))]
fn bench_payload_contract(_c: &mut Criterion, _runtime: &Runtime, _profile: BenchProfile) {
    panic!("TRANSPORT_BENCH_SUITE=payload-contract requires feature payload-contract-benchmarks");
}

fn fill_pattern(dst: &mut [u8], start: usize) {
    for (offset, byte) in dst.iter_mut().enumerate() {
        *byte = u8::try_from((start + offset) % 251).expect("pattern byte fits");
    }
}

fn checksum_for_len(len: usize) -> u64 {
    let mut checksum = 0_u64;
    let mut offset = 0;
    let mut chunk = [0_u8; DIRECT_WRITE_CHUNK];
    while offset < len {
        let take = cmp::min(chunk.len(), len - offset);
        fill_pattern(&mut chunk[..take], offset);
        checksum = checksum_bytes(checksum, &chunk[..take]);
        offset += take;
    }
    checksum
}

fn checksum_bytes(checksum: u64, bytes: &[u8]) -> u64 {
    bytes.iter().fold(checksum, |checksum, byte| {
        checksum.wrapping_mul(16_777_619) ^ u64::from(*byte)
    })
}

fn next_sequence() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};

    static SEQUENCE: AtomicU64 = AtomicU64::new(1);
    SEQUENCE.fetch_add(1, Ordering::Relaxed)
}

fn next_uuid() -> UUID {
    uuid_for(next_sequence())
}

fn uuid_for(sequence: u64) -> UUID {
    let timestamp_millis = u64::try_from(
        SystemTime::UNIX_EPOCH
            .elapsed()
            .expect("system time should be after UNIX epoch")
            .as_millis(),
    )
    .expect("timestamp millis should fit in u64");
    let msb = (timestamp_millis << 16) | 0x7000 | (sequence & 0x0fff);
    let lsb = UUID_LSB_BASE | (sequence & 0x3fff_ffff_ffff_ffff);
    UUID::from_u64_pair(msb, lsb).expect("benchmark UUID should be valid UUIDv7")
}

fn resource_id(base: u16, sequence: u64) -> u16 {
    let offset = u16::try_from(sequence % 0x0fff).expect("resource offset fits in u16");
    base.checked_add(offset)
        .expect("benchmark resource id fits")
}

fn uri(authority: &str, entity_type: u32, resource: u16) -> UUri {
    UUri::try_from_parts(authority, entity_type, 1, resource).expect("valid benchmark URI")
}

criterion_group!(transport_criterion, bench_transport);
criterion_main!(transport_criterion);
