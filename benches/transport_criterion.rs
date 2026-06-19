// ################################################################################
// Copyright (c) 2026 Contributors to the Eclipse Foundation
//
// SPDX-License-Identifier: Apache-2.0
// ################################################################################

#![allow(clippy::missing_panics_doc, clippy::too_many_lines)]

use std::{sync::Arc, time::Duration, time::SystemTime};

#[cfg(feature = "benchmark-owned")]
use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use tokio::runtime::Runtime;
#[cfg(feature = "payload-contract-benchmarks")]
use up_rust::bench_fixtures::payload_contract::{self, *};
#[cfg(feature = "benchmark-owned")]
use up_rust::{EncodedOwnedFrame, ProtobufPayload, UOwnedFrame, UOwnedTransport, UWireMetadata};
use up_rust::{
    PayloadEncoding, StableContainerWireFormat, UCode, UFrameMetadata,
    ULoanedContiguousZeroCopyRxFrame, UMessageBuilder, UMessageType, UUID, UUri, UWithWire,
    UZeroCopyTransport, UZeroCopyUninitTransportExt,
};
#[cfg(feature = "benchmark-owned")]
use up_transport_iceoryx2_rust::{BenchmarkOwnedIceoryx2Core, Iceoryx2OwnedCore};
use up_transport_iceoryx2_rust::{Iceoryx2PubSub, Iceoryx2PubSubConfig};

const BENCH_TIMEOUT: Duration = Duration::from_secs(5);
const LARGE_SENSOR_BENCH_TIMEOUT: Duration = Duration::from_secs(30);
const CORE_STATIC_ALLOCATION: usize = 128 * 1_024;
const CAMERA_STATIC_ALLOCATION: usize = 16 * 1_024 * 1_024;
const UUID_LSB_BASE: u64 = 0x8000_0000_0000_0000;
#[cfg(feature = "payload-contract-benchmarks")]
const PAYLOAD_CONTRACT_SEQUENCE: u32 = 1;

#[derive(Clone, Copy)]
enum BenchSuite {
    PayloadContract,
}

impl BenchSuite {
    fn from_env() -> Self {
        match std::env::var("TRANSPORT_BENCH_SUITE")
            .unwrap_or_else(|_| "payload-contract".to_string())
            .as_str()
        {
            "payload-contract" => Self::PayloadContract,
            other => panic!("TRANSPORT_BENCH_SUITE must be payload-contract; got {other}"),
        }
    }
}

#[derive(Clone, Copy)]
enum BenchProfile {
    Core,
    Camera,
    All,
}

#[derive(Clone, Copy)]
enum BenchDiagnostic {
    Authority,
    PrebuiltPayload,
    MetadataOnly,
    TxOnly,
    RxOnly,
    CopyLedger,
}

impl BenchDiagnostic {
    fn from_env() -> Self {
        match std::env::var("TRANSPORT_BENCH_DIAGNOSTIC")
            .unwrap_or_else(|_| "authority".to_string())
            .as_str()
        {
            "authority" => Self::Authority,
            "prebuilt-payload" => Self::PrebuiltPayload,
            "metadata-only" => Self::MetadataOnly,
            "tx-only" => Self::TxOnly,
            "rx-only" => Self::RxOnly,
            "copy-ledger" => Self::CopyLedger,
            other => panic!(
                "TRANSPORT_BENCH_DIAGNOSTIC must be one of authority, prebuilt-payload, metadata-only, tx-only, rx-only, copy-ledger; got {other}"
            ),
        }
    }

    fn uses_real_transports(self) -> bool {
        matches!(self, Self::Authority | Self::PrebuiltPayload)
    }
}

#[derive(Clone, Copy)]
enum DiagnosticValidate {
    None,
    Sample,
    Full,
}

impl DiagnosticValidate {
    fn from_env() -> Self {
        match std::env::var("TRANSPORT_BENCH_VALIDATE")
            .unwrap_or_else(|_| "full".to_string())
            .as_str()
        {
            "none" => Self::None,
            "sample" => Self::Sample,
            "full" => Self::Full,
            other => {
                panic!("TRANSPORT_BENCH_VALIDATE must be one of none, sample, full; got {other}")
            }
        }
    }
}

fn copy_ledger_enabled() -> bool {
    match std::env::var("TRANSPORT_BENCH_COPY_LEDGER")
        .unwrap_or_else(|_| "0".to_string())
        .as_str()
    {
        "0" => false,
        "1" => true,
        other => panic!("TRANSPORT_BENCH_COPY_LEDGER must be 0 or 1; got {other}"),
    }
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

#[cfg(feature = "payload-contract-benchmarks")]
#[derive(Clone, Copy)]
enum PayloadContractPath {
    #[cfg(feature = "benchmark-owned")]
    ProtobufOwned,
    StableZcNoZero,
    #[cfg(feature = "benchmark-owned")]
    StableOwnedBytes,
}

#[cfg(feature = "payload-contract-benchmarks")]
impl PayloadContractPath {
    fn label(self) -> &'static str {
        match self {
            #[cfg(feature = "benchmark-owned")]
            Self::ProtobufOwned => "protobuf_owned_full",
            Self::StableZcNoZero => "stable_zc_nozero_full",
            #[cfg(feature = "benchmark-owned")]
            Self::StableOwnedBytes => "stable_owned_bytes_full",
        }
    }
}

#[cfg(feature = "payload-contract-benchmarks")]
#[derive(Clone, Copy)]
enum PayloadContractPathMode {
    All,
    Owned,
    ZeroCopy,
}

#[cfg(feature = "payload-contract-benchmarks")]
impl PayloadContractPathMode {
    fn from_env() -> Self {
        match std::env::var("TRANSPORT_BENCH_PATH")
            .unwrap_or_else(|_| "all".to_string())
            .as_str()
        {
            "all" => Self::All,
            "owned" => Self::Owned,
            "zero-copy" => Self::ZeroCopy,
            other => {
                panic!("TRANSPORT_BENCH_PATH must be one of all, owned, zero-copy; got {other}")
            }
        }
    }
}

#[cfg(all(feature = "payload-contract-benchmarks", feature = "benchmark-owned"))]
#[derive(Clone)]
struct PrebuiltOwnedPayload {
    encoding: PayloadEncoding,
    bytes: Vec<u8>,
}

#[cfg(all(feature = "payload-contract-benchmarks", feature = "benchmark-owned"))]
impl PrebuiltOwnedPayload {
    fn for_path(path: PayloadContractPath, contract: &PayloadContractCase) -> Option<Self> {
        match path {
            PayloadContractPath::ProtobufOwned => Some(Self {
                encoding: ProtobufPayload::encoding(),
                bytes: payload_contract::protobuf_encoded_bytes_for(
                    contract,
                    PAYLOAD_CONTRACT_SEQUENCE,
                )
                .expect("protobuf benchmark payload should serialize"),
            }),
            PayloadContractPath::StableOwnedBytes => {
                let fixture =
                    payload_contract::stable_owned_fixture_for(contract, PAYLOAD_CONTRACT_SEQUENCE)
                        .expect("stable owned fixture should initialize");
                Some(Self {
                    encoding: fixture.encoding,
                    bytes: fixture.bytes,
                })
            }
            PayloadContractPath::StableZcNoZero => None,
        }
    }
}

#[derive(Clone)]
struct BenchCase {
    source: UUri,
}

impl BenchCase {
    fn new(case_name: &str) -> Self {
        let authority = format!("iox-userializer-bench-{}-{case_name}", std::process::id());
        Self {
            source: UUri::try_from_parts(&authority, 0x4210, 1, resource_id(next_sequence()))
                .expect("valid benchmark source URI"),
        }
    }

    fn metadata(&self, id: UUID, encoding: Option<PayloadEncoding>) -> UFrameMetadata {
        let mut builder = UMessageBuilder::publish(self.source.clone());
        builder.with_message_id(id);
        let message = builder.build().expect("valid benchmark message");
        UFrameMetadata::new(message.attributes().clone(), encoding).expect("valid metadata")
    }
}

#[cfg(feature = "payload-contract-benchmarks")]
struct PayloadContractAck {
    id: UUID,
    message_type: UMessageType,
    case_id: u32,
    sequence: u32,
    semantic_reference_len: usize,
    transported_payload_len: usize,
}

struct BenchTransports {
    zero_copy: Arc<up_rust::UWireTransport<Iceoryx2PubSub, StableContainerWireFormat>>,
    #[cfg(feature = "benchmark-owned")]
    owned: Arc<up_rust::UWireTransport<BenchmarkOwnedIceoryx2Core, StableContainerWireFormat>>,
}

impl BenchTransports {
    fn build(max_slice_len: usize) -> Self {
        let config = Iceoryx2PubSubConfig::static_allocation(max_slice_len)
            .with_pull_mismatch_queue_capacity(4_096);
        let core = Iceoryx2PubSub::with_config(config);
        let zero_copy = Arc::new(core.clone().with_wire(StableContainerWireFormat));
        #[cfg(feature = "benchmark-owned")]
        let owned = Arc::new(
            BenchmarkOwnedIceoryx2Core::new(core).with_selected_wire(StableContainerWireFormat),
        );
        Self {
            zero_copy,
            #[cfg(feature = "benchmark-owned")]
            owned,
        }
    }
}

async fn prime_subscriber(transports: &BenchTransports, case: &BenchCase) {
    match transports
        .zero_copy
        .receive_zero_copy(&case.source, None)
        .await
    {
        Ok(frame) => drop(frame),
        Err(status) if status.get_code() == UCode::NotFound => {}
        Err(status) => panic!("failed to prime iceoryx2 pull subscriber: {status:?}"),
    }
}

#[cfg(feature = "payload-contract-benchmarks")]
fn payload_contract_paths(path_mode: PayloadContractPathMode) -> &'static [PayloadContractPath] {
    #[cfg(feature = "benchmark-owned")]
    {
        match path_mode {
            PayloadContractPathMode::All => &[
                PayloadContractPath::ProtobufOwned,
                PayloadContractPath::StableZcNoZero,
                PayloadContractPath::StableOwnedBytes,
            ],
            PayloadContractPathMode::Owned => &[
                PayloadContractPath::ProtobufOwned,
                PayloadContractPath::StableOwnedBytes,
            ],
            PayloadContractPathMode::ZeroCopy => &[PayloadContractPath::StableZcNoZero],
        }
    }
    #[cfg(not(feature = "benchmark-owned"))]
    {
        match path_mode {
            PayloadContractPathMode::All | PayloadContractPathMode::ZeroCopy => {
                &[PayloadContractPath::StableZcNoZero]
            }
            PayloadContractPathMode::Owned => {
                panic!("TRANSPORT_BENCH_PATH=owned requires feature benchmark-owned")
            }
        }
    }
}

#[cfg(feature = "payload-contract-benchmarks")]
fn bench_payload_contract_matrix(
    c: &mut Criterion,
    runtime: &Runtime,
    transports: &BenchTransports,
    group_name: &'static str,
    payload_cases: &[PayloadContractCase],
    timeout: Duration,
    path_mode: PayloadContractPathMode,
) {
    let mut group = c.benchmark_group(group_name);
    for contract in payload_cases {
        for &path in payload_contract_paths(path_mode) {
            let case = BenchCase::new(contract.name());
            runtime.block_on(prime_subscriber(transports, &case));
            let transported_payload_len = payload_contract_transported_len(path, contract);
            group.bench_function(
                BenchmarkId::new(
                    path.label(),
                    format!(
                        "publish/{}/{}/{}",
                        contract.name(),
                        contract.semantic_reference_len(),
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
                            black_box(ack.semantic_reference_len);
                            black_box(ack.transported_payload_len);
                            black_box(contract.name());
                        });
                    });
                },
            );
        }
    }
    group.finish();
}

#[cfg(all(feature = "payload-contract-benchmarks", feature = "benchmark-owned"))]
fn bench_payload_contract_prebuilt_payload_matrix(
    c: &mut Criterion,
    runtime: &Runtime,
    transports: &BenchTransports,
    group_name: &'static str,
    payload_cases: &[PayloadContractCase],
    timeout: Duration,
    path_mode: PayloadContractPathMode,
) {
    let _validate = DiagnosticValidate::from_env();
    let mut group = c.benchmark_group(group_name);
    for contract in payload_cases {
        for &path in payload_contract_paths(path_mode) {
            let Some(prebuilt) = PrebuiltOwnedPayload::for_path(path, contract) else {
                continue;
            };
            let case = BenchCase::new(contract.name());
            runtime.block_on(prime_subscriber(transports, &case));
            let transported_payload_len = prebuilt.bytes.len();
            group.bench_function(
                BenchmarkId::new(
                    format!("{}_diagnostic_prebuilt_payload", path.label()),
                    format!(
                        "publish/{}/{}/{}",
                        contract.name(),
                        contract.semantic_reference_len(),
                        transported_payload_len
                    ),
                ),
                |b| {
                    b.iter(|| {
                        runtime.block_on(async {
                            let id = next_uuid();
                            send_prebuilt_owned_payload_contract_path(
                                transports,
                                path,
                                &case,
                                id.clone(),
                                &prebuilt,
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
                            black_box(ack.semantic_reference_len);
                            black_box(ack.transported_payload_len);
                        });
                    });
                },
            );
        }
    }
    group.finish();
}

#[cfg(all(feature = "payload-contract-benchmarks", feature = "benchmark-owned"))]
fn bench_payload_contract_owned_adapter_diagnostic_matrix(
    c: &mut Criterion,
    runtime: &Runtime,
    diagnostic: BenchDiagnostic,
    group_name: &'static str,
    payload_cases: &[PayloadContractCase],
    path_mode: PayloadContractPathMode,
) {
    let _validate = DiagnosticValidate::from_env();
    let mut group = c.benchmark_group(group_name);
    for contract in payload_cases {
        for &path in payload_contract_paths(path_mode) {
            let Some(prebuilt) = PrebuiltOwnedPayload::for_path(path, contract) else {
                continue;
            };
            let case = BenchCase::new(contract.name());
            let core = Iceoryx2OwnedCore::new();
            let transport = core.clone().with_selected_wire(StableContainerWireFormat);
            let encoded_metadata = StableContainerWireFormat::encode_frame_metadata(
                &case.metadata(next_uuid(), Some(prebuilt.encoding.clone())),
            )
            .expect("diagnostic metadata should encode");
            let id_label = match diagnostic {
                BenchDiagnostic::TxOnly => "diagnostic_tx_only",
                BenchDiagnostic::RxOnly => "diagnostic_rx_only",
                BenchDiagnostic::CopyLedger => "diagnostic_copy_ledger",
                _ => unreachable!("unsupported owned adapter diagnostic"),
            };
            let estimated_copy_bytes = 2 * encoded_metadata.len() + 2 * prebuilt.bytes.len();
            let parameter = if matches!(diagnostic, BenchDiagnostic::CopyLedger) {
                format!(
                    "copy-ledger/{}/{}/{}/{}",
                    contract.name(),
                    contract.semantic_reference_len(),
                    prebuilt.bytes.len(),
                    estimated_copy_bytes
                )
            } else {
                format!(
                    "publish/{}/{}/{}",
                    contract.name(),
                    contract.semantic_reference_len(),
                    prebuilt.bytes.len()
                )
            };
            group.bench_function(
                BenchmarkId::new(format!("{}_{}", path.label(), id_label), parameter),
                |b| match diagnostic {
                    BenchDiagnostic::TxOnly => {
                        b.iter(|| {
                            runtime.block_on(async {
                                let metadata =
                                    case.metadata(next_uuid(), Some(prebuilt.encoding.clone()));
                                let frame =
                                    UOwnedFrame::with_payload(metadata, prebuilt.bytes.clone())
                                        .expect("valid diagnostic owned TX frame");
                                transport
                                    .send_owned(frame)
                                    .await
                                    .expect("diagnostic owned TX should succeed");
                            });
                        });
                    }
                    BenchDiagnostic::RxOnly => {
                        b.iter(|| {
                            runtime.block_on(async {
                                core.push_encoded_owned(EncodedOwnedFrame::new(
                                    encoded_metadata.clone(),
                                    Some(Bytes::copy_from_slice(&prebuilt.bytes)),
                                ))
                                .await;
                                let frame = transport
                                    .receive_owned(&case.source, None)
                                    .await
                                    .expect("diagnostic owned RX should succeed");
                                black_box(frame.payload_bytes().len());
                            });
                        });
                    }
                    BenchDiagnostic::CopyLedger => {
                        let enabled = copy_ledger_enabled();
                        b.iter(|| {
                            let estimated = if enabled { estimated_copy_bytes } else { 0 };
                            black_box(estimated);
                        });
                    }
                    _ => unreachable!("unsupported owned adapter diagnostic"),
                },
            );
        }
    }
    group.finish();
}

#[cfg(feature = "payload-contract-benchmarks")]
fn bench_payload_contract_metadata_only_matrix(
    c: &mut Criterion,
    group_name: &'static str,
    payload_cases: &[PayloadContractCase],
) {
    let _validate = DiagnosticValidate::from_env();
    let mut group = c.benchmark_group(group_name);
    for contract in payload_cases {
        let case = BenchCase::new(contract.name());
        let metadata = case.metadata(next_uuid(), None);
        let encoded = StableContainerWireFormat::encode_frame_metadata(&metadata)
            .expect("diagnostic metadata should encode");
        group.bench_function(
            BenchmarkId::new(
                "stable_zc_nozero_full_diagnostic_metadata_only",
                format!("metadata/{}/{}", contract.name(), encoded.len()),
            ),
            |b| {
                b.iter(|| {
                    let encoded =
                        StableContainerWireFormat::encode_frame_metadata(black_box(&metadata))
                            .expect("diagnostic metadata should encode");
                    let decoded =
                        StableContainerWireFormat::decode_frame_metadata(black_box(&encoded))
                            .expect("diagnostic metadata should decode");
                    black_box(decoded);
                });
            },
        );
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
        #[cfg(feature = "benchmark-owned")]
        PayloadContractPath::ProtobufOwned => {
            let payload =
                payload_contract::protobuf_encoded_bytes_for(contract, PAYLOAD_CONTRACT_SEQUENCE)
                    .expect("protobuf benchmark payload should serialize");
            let metadata = case.metadata(id, Some(ProtobufPayload::encoding()));
            let frame = UOwnedFrame::with_payload(metadata, payload)
                .expect("valid protobuf owned benchmark frame");
            transports
                .owned
                .send_owned(frame)
                .await
                .expect("iceoryx2 payload-contract protobuf send should succeed");
        }
        PayloadContractPath::StableZcNoZero => {
            let metadata = case.metadata(id, None);
            send_stable_payload_contract(transports, metadata, contract).await;
        }
        #[cfg(feature = "benchmark-owned")]
        PayloadContractPath::StableOwnedBytes => {
            let fixture =
                payload_contract::stable_owned_fixture_for(contract, PAYLOAD_CONTRACT_SEQUENCE)
                    .expect("stable owned fixture should initialize");
            let metadata = case.metadata(id, Some(fixture.encoding));
            let frame = UOwnedFrame::with_payload(metadata, fixture.bytes)
                .expect("valid stable owned benchmark frame");
            transports
                .owned
                .send_owned(frame)
                .await
                .expect("iceoryx2 payload-contract stable owned bytes send should succeed");
        }
    }
}

#[cfg(all(feature = "payload-contract-benchmarks", feature = "benchmark-owned"))]
async fn send_prebuilt_owned_payload_contract_path(
    transports: &BenchTransports,
    path: PayloadContractPath,
    case: &BenchCase,
    id: UUID,
    prebuilt: &PrebuiltOwnedPayload,
) {
    match path {
        PayloadContractPath::ProtobufOwned | PayloadContractPath::StableOwnedBytes => {
            let metadata = case.metadata(id, Some(prebuilt.encoding.clone()));
            let frame = UOwnedFrame::with_payload(metadata, prebuilt.bytes.clone())
                .expect("valid prebuilt owned benchmark frame");
            transports
                .owned
                .send_owned(frame)
                .await
                .expect("iceoryx2 prebuilt owned send should succeed");
        }
        PayloadContractPath::StableZcNoZero => {
            panic!("prebuilt-payload diagnostic is only defined for owned payload paths")
        }
    }
}

#[cfg(feature = "payload-contract-benchmarks")]
async fn send_stable_payload_contract(
    transports: &BenchTransports,
    metadata: UFrameMetadata,
    contract: &PayloadContractCase,
) {
    match contract.kind() {
        PayloadContractCaseKind::CanClassicMax => {
            transports
                .zero_copy
                .send_uninit_stable_payload_as::<CanClassicFrameV1>(metadata, |payload| {
                    payload_contract::init_can_classic_max(payload, PAYLOAD_CONTRACT_SEQUENCE)
                })
                .await
        }
        PayloadContractCaseKind::CanFdMax => {
            transports
                .zero_copy
                .send_uninit_stable_payload_as::<CanFdFrameV1>(metadata, |payload| {
                    payload_contract::init_can_fd_max(payload, PAYLOAD_CONTRACT_SEQUENCE)
                })
                .await
        }
        PayloadContractCaseKind::SomeIpSingleMtu => {
            transports
                .zero_copy
                .send_uninit_stable_payload_as::<SomeIpSignalBatchMtuV1>(metadata, |payload| {
                    payload_contract::init_someip_single_mtu(payload, PAYLOAD_CONTRACT_SEQUENCE)
                })
                .await
        }
        PayloadContractCaseKind::Streamer4k => {
            transports
                .zero_copy
                .send_uninit_stable_payload_as::<StreamChunk4kV1>(metadata, |payload| {
                    payload_contract::init_streamer_4k(payload, PAYLOAD_CONTRACT_SEQUENCE)
                })
                .await
        }
        PayloadContractCaseKind::RadarArs548DetectionList => {
            transports
                .zero_copy
                .send_uninit_stable_payload_as::<RadarDetectionListArs548V1>(metadata, |payload| {
                    payload_contract::init_radar_ars548_detection_list(
                        payload,
                        PAYLOAD_CONTRACT_SEQUENCE,
                    )
                })
                .await
        }
        PayloadContractCaseKind::Streamer64k => {
            transports
                .zero_copy
                .send_uninit_stable_payload_as::<StreamChunk64kV1>(metadata, |payload| {
                    payload_contract::init_streamer_64k(payload, PAYLOAD_CONTRACT_SEQUENCE)
                })
                .await
        }
        #[cfg(feature = "payload-contract-large-benchmarks")]
        PayloadContractCaseKind::LidarHesaiAt128PointCloud => {
            transports
                .zero_copy
                .send_uninit_stable_payload_as::<LidarPointCloudHesaiAt128V1>(metadata, |payload| {
                    payload_contract::init_lidar_hesai_at128_point_cloud(
                        payload,
                        PAYLOAD_CONTRACT_SEQUENCE,
                    )
                })
                .await
        }
        #[cfg(feature = "payload-contract-large-benchmarks")]
        PayloadContractCaseKind::Camera8mpBayerRggb12p => {
            transports
                .zero_copy
                .send_uninit_stable_payload_as::<CameraBayerRggb12pFrame8mpV1>(
                    metadata,
                    |payload| {
                        payload_contract::init_camera_8mp_bayer_rggb12p(
                            payload,
                            PAYLOAD_CONTRACT_SEQUENCE,
                        )
                    },
                )
                .await
        }
    }
    .expect("iceoryx2 payload-contract stable no-zero send should succeed");
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
            #[cfg(feature = "benchmark-owned")]
            PayloadContractPath::ProtobufOwned => tokio::time::timeout(
                remaining,
                transports.owned.receive_owned(&case.source, None),
            )
            .await
            .expect("timed out waiting for iceoryx2 payload-contract owned receive")
            .map(|frame| protobuf_payload_contract_ack(frame, contract)),
            PayloadContractPath::StableZcNoZero => tokio::time::timeout(
                remaining,
                transports.zero_copy.receive_zero_copy(&case.source, None),
            )
            .await
            .expect("timed out waiting for iceoryx2 payload-contract zero-copy receive")
            .map(|frame| stable_payload_contract_ack(&frame, contract)),
            #[cfg(feature = "benchmark-owned")]
            PayloadContractPath::StableOwnedBytes => tokio::time::timeout(
                remaining,
                transports.owned.receive_owned(&case.source, None),
            )
            .await
            .expect("timed out waiting for iceoryx2 payload-contract stable owned receive")
            .map(|frame| stable_owned_payload_contract_ack(frame, contract)),
        };
        match result {
            Ok(ack) if &ack.id == expected_id => {
                assert_eq!(ack.message_type, UMessageType::Publish);
                assert_eq!(ack.case_id, contract.case_id());
                assert_eq!(ack.sequence, PAYLOAD_CONTRACT_SEQUENCE);
                assert_eq!(
                    ack.semantic_reference_len,
                    contract.semantic_reference_len()
                );
                assert_eq!(
                    ack.transported_payload_len,
                    expected_transported_payload_len
                );
                return ack;
            }
            Ok(_) => continue,
            Err(status) if status.get_code() == UCode::NotFound => {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            Err(status) => panic!("unexpected iceoryx2 payload-contract receive error: {status:?}"),
        }
    }
}

#[cfg(all(feature = "payload-contract-benchmarks", feature = "benchmark-owned"))]
fn protobuf_payload_contract_ack(
    frame: UOwnedFrame,
    contract: &PayloadContractCase,
) -> PayloadContractAck {
    let transported_payload_len = frame.payload_bytes().len();
    let id = frame.metadata().attributes().id().clone();
    let message_type = frame.metadata().attributes().type_();
    payload_contract::validate_protobuf_bytes(
        contract,
        PAYLOAD_CONTRACT_SEQUENCE,
        frame.payload_bytes(),
    )
    .expect("protobuf payload-contract frame should validate");
    PayloadContractAck {
        id,
        message_type,
        case_id: contract.case_id(),
        sequence: PAYLOAD_CONTRACT_SEQUENCE,
        semantic_reference_len: contract.semantic_reference_len(),
        transported_payload_len,
    }
}

#[cfg(all(feature = "payload-contract-benchmarks", feature = "benchmark-owned"))]
fn stable_owned_payload_contract_ack(
    frame: UOwnedFrame,
    contract: &PayloadContractCase,
) -> PayloadContractAck {
    payload_contract::validate_stable_owned_bytes(
        contract,
        PAYLOAD_CONTRACT_SEQUENCE,
        frame.metadata().payload_encoding(),
        frame.payload_bytes(),
    )
    .expect("stable owned payload-contract frame should validate");
    PayloadContractAck {
        id: frame.metadata().attributes().id().clone(),
        message_type: frame.metadata().attributes().type_(),
        case_id: contract.case_id(),
        sequence: PAYLOAD_CONTRACT_SEQUENCE,
        semantic_reference_len: contract.semantic_reference_len(),
        transported_payload_len: frame.payload_bytes().len(),
    }
}

#[cfg(feature = "payload-contract-benchmarks")]
fn stable_payload_contract_ack(
    frame: &impl ULoanedContiguousZeroCopyRxFrame,
    contract: &PayloadContractCase,
) -> PayloadContractAck {
    black_box(
        frame
            .payload_loan_provenance()
            .expect("stable payload should be loan-backed"),
    );
    validate_stable_payload_for_case(frame, contract);
    PayloadContractAck {
        id: frame.metadata().attributes().id().clone(),
        message_type: frame.metadata().attributes().type_(),
        case_id: contract.case_id(),
        sequence: PAYLOAD_CONTRACT_SEQUENCE,
        semantic_reference_len: contract.semantic_reference_len(),
        transported_payload_len: frame.payload_len(),
    }
}

#[cfg(feature = "payload-contract-benchmarks")]
fn validate_stable_payload_for_case(
    frame: &impl ULoanedContiguousZeroCopyRxFrame,
    contract: &PayloadContractCase,
) {
    match contract.kind() {
        PayloadContractCaseKind::CanClassicMax => payload_contract::validate_stable_payload(
            contract,
            PAYLOAD_CONTRACT_SEQUENCE,
            frame
                .borrow_stable_payload::<CanClassicFrameV1>()
                .expect("CAN Classic stable payload-contract frame should borrow"),
        ),
        PayloadContractCaseKind::CanFdMax => payload_contract::validate_stable_payload(
            contract,
            PAYLOAD_CONTRACT_SEQUENCE,
            frame
                .borrow_stable_payload::<CanFdFrameV1>()
                .expect("CAN FD stable payload-contract frame should borrow"),
        ),
        PayloadContractCaseKind::SomeIpSingleMtu => payload_contract::validate_stable_payload(
            contract,
            PAYLOAD_CONTRACT_SEQUENCE,
            frame
                .borrow_stable_payload::<SomeIpSignalBatchMtuV1>()
                .expect("SOME/IP stable payload-contract frame should borrow"),
        ),
        PayloadContractCaseKind::Streamer4k => payload_contract::validate_stable_payload(
            contract,
            PAYLOAD_CONTRACT_SEQUENCE,
            frame
                .borrow_stable_payload::<StreamChunk4kV1>()
                .expect("stream 4K stable payload-contract frame should borrow"),
        ),
        PayloadContractCaseKind::RadarArs548DetectionList => {
            payload_contract::validate_stable_payload(
                contract,
                PAYLOAD_CONTRACT_SEQUENCE,
                frame
                    .borrow_stable_payload::<RadarDetectionListArs548V1>()
                    .expect("radar stable payload-contract frame should borrow"),
            )
        }
        PayloadContractCaseKind::Streamer64k => payload_contract::validate_stable_payload(
            contract,
            PAYLOAD_CONTRACT_SEQUENCE,
            frame
                .borrow_stable_payload::<StreamChunk64kV1>()
                .expect("stream 64K stable payload-contract frame should borrow"),
        ),
        #[cfg(feature = "payload-contract-large-benchmarks")]
        PayloadContractCaseKind::LidarHesaiAt128PointCloud => {
            payload_contract::validate_stable_payload(
                contract,
                PAYLOAD_CONTRACT_SEQUENCE,
                frame
                    .borrow_stable_payload::<LidarPointCloudHesaiAt128V1>()
                    .expect("LiDAR stable payload-contract frame should borrow"),
            )
        }
        #[cfg(feature = "payload-contract-large-benchmarks")]
        PayloadContractCaseKind::Camera8mpBayerRggb12p => {
            payload_contract::validate_stable_payload(
                contract,
                PAYLOAD_CONTRACT_SEQUENCE,
                frame
                    .borrow_stable_payload::<CameraBayerRggb12pFrame8mpV1>()
                    .expect("camera stable payload-contract frame should borrow"),
            )
        }
    }
    .expect("stable payload-contract frame should validate");
}

#[cfg(feature = "payload-contract-benchmarks")]
fn payload_contract_transported_len(
    path: PayloadContractPath,
    contract: &PayloadContractCase,
) -> usize {
    match path {
        #[cfg(feature = "benchmark-owned")]
        PayloadContractPath::ProtobufOwned => {
            payload_contract::protobuf_encoded_len(contract, PAYLOAD_CONTRACT_SEQUENCE)
        }
        PayloadContractPath::StableZcNoZero => payload_contract::stable_payload_len(contract),
        #[cfg(feature = "benchmark-owned")]
        PayloadContractPath::StableOwnedBytes => payload_contract::stable_payload_len(contract),
    }
}

fn bench_transport(c: &mut Criterion) {
    let _suite = BenchSuite::from_env();
    bench_payload_contract(c, BenchProfile::from_env(), BenchDiagnostic::from_env());
}

#[cfg(feature = "payload-contract-benchmarks")]
fn bench_payload_contract(c: &mut Criterion, profile: BenchProfile, diagnostic: BenchDiagnostic) {
    let runtime = Runtime::new().expect("tokio runtime");
    let path_mode = PayloadContractPathMode::from_env();
    if profile.includes_core() {
        let transports = diagnostic
            .uses_real_transports()
            .then(|| runtime.block_on(async { BenchTransports::build(CORE_STATIC_ALLOCATION) }));
        bench_payload_contract_for_diagnostic(
            c,
            &runtime,
            transports.as_ref(),
            diagnostic,
            "transport_payload_contract_core",
            payload_contract::core_cases(),
            BENCH_TIMEOUT,
            path_mode,
        );
    }
    if profile.includes_camera() {
        let transports = diagnostic
            .uses_real_transports()
            .then(|| runtime.block_on(async { BenchTransports::build(CAMERA_STATIC_ALLOCATION) }));
        bench_payload_contract_for_diagnostic(
            c,
            &runtime,
            transports.as_ref(),
            diagnostic,
            "transport_payload_contract_large_sensor",
            payload_contract::large_sensor_cases(),
            LARGE_SENSOR_BENCH_TIMEOUT,
            path_mode,
        );
    }
}

#[cfg(feature = "payload-contract-benchmarks")]
fn bench_payload_contract_for_diagnostic(
    c: &mut Criterion,
    runtime: &Runtime,
    transports: Option<&BenchTransports>,
    diagnostic: BenchDiagnostic,
    group_name: &'static str,
    payload_cases: &[PayloadContractCase],
    timeout: Duration,
    path_mode: PayloadContractPathMode,
) {
    match diagnostic {
        BenchDiagnostic::Authority => bench_payload_contract_matrix(
            c,
            runtime,
            transports.expect("authority diagnostics require real transports"),
            group_name,
            payload_cases,
            timeout,
            path_mode,
        ),
        #[cfg(feature = "benchmark-owned")]
        BenchDiagnostic::PrebuiltPayload => bench_payload_contract_prebuilt_payload_matrix(
            c,
            runtime,
            transports.expect("prebuilt-payload diagnostics require real transports"),
            group_name,
            payload_cases,
            timeout,
            path_mode,
        ),
        BenchDiagnostic::MetadataOnly => {
            bench_payload_contract_metadata_only_matrix(c, group_name, payload_cases);
        }
        #[cfg(feature = "benchmark-owned")]
        BenchDiagnostic::TxOnly | BenchDiagnostic::RxOnly | BenchDiagnostic::CopyLedger => {
            bench_payload_contract_owned_adapter_diagnostic_matrix(
                c,
                runtime,
                diagnostic,
                group_name,
                payload_cases,
                path_mode,
            );
        }
        #[cfg(not(feature = "benchmark-owned"))]
        BenchDiagnostic::PrebuiltPayload
        | BenchDiagnostic::TxOnly
        | BenchDiagnostic::RxOnly
        | BenchDiagnostic::CopyLedger => {
            panic!("TRANSPORT_BENCH_DIAGNOSTIC mode requires feature benchmark-owned")
        }
    }
}

#[cfg(not(feature = "payload-contract-benchmarks"))]
fn bench_payload_contract(
    _c: &mut Criterion,
    _profile: BenchProfile,
    _diagnostic: BenchDiagnostic,
) {
    panic!("TRANSPORT_BENCH_SUITE=payload-contract requires feature payload-contract-benchmarks");
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

fn resource_id(sequence: u64) -> u16 {
    let offset = u16::try_from(sequence % 0x0fff).expect("resource offset fits in u16");
    0x9000u16.checked_add(offset).expect("resource id fits")
}

criterion_group!(transport_criterion, bench_transport);
criterion_main!(transport_criterion);
