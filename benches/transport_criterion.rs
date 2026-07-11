// ################################################################################
// Copyright (c) 2026 Contributors to the Eclipse Foundation
//
// SPDX-License-Identifier: Apache-2.0
// ################################################################################

#![allow(
    clippy::missing_panics_doc,
    clippy::too_many_arguments,
    clippy::too_many_lines
)]

#[cfg(feature = "perf-diagnostics")]
use std::{
    fs::File,
    io::{BufWriter, Write},
};
use std::{sync::Arc, time::Duration, time::Instant, time::SystemTime};

#[cfg(feature = "benchmark-owned")]
use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use tokio::runtime::Runtime;
#[cfg(feature = "perf-diagnostics")]
use up_rust::UninitStableSendPhases;
#[cfg(feature = "payload-contract-benchmarks")]
use up_rust::bench_fixtures::payload_contract::{self, *};
use up_rust::selected_wire_user_api::{StableContainerWireTransport, UWithNativePrefixWire};
use up_rust::transport_implementer_api::UEncodedRxFrame;
#[cfg(feature = "benchmark-owned")]
use up_rust::transport_implementer_api::{
    EncodedOwnedFrame, PreparedOwnedFrame, UEncodedOwnedListener, UOwnedTransportCore,
};
use up_rust::wire_implementer_api::{
    NativePrefixFrameMetadataCodec, StableContainerWireFormat, UWire, UWireMetadataCodec,
};
use up_rust::{
    PayloadEncoding, UCode, UFrameMetadata, UFrameView, ULoanedContiguousZeroCopyRxFrame,
    UMessageType, UUID, UUri, UZeroCopyTransport, UZeroCopyUninitTransportExt,
};
#[cfg(feature = "benchmark-owned")]
use up_rust::{ProtobufPayload, UOwnedFrame, UOwnedTransport, UStatus};
#[cfg(feature = "benchmark-owned")]
use up_transport_iceoryx2_rust::{BenchmarkOwnedIceoryx2Core, Iceoryx2OwnedCore};
use up_transport_iceoryx2_rust::{Iceoryx2PubSub, Iceoryx2PubSubConfig, UProtocolHeader};

const BENCH_TIMEOUT: Duration = Duration::from_secs(5);
const LARGE_SENSOR_BENCH_TIMEOUT: Duration = Duration::from_secs(30);
const CORE_STATIC_ALLOCATION: usize = 128 * 1_024;
const CAMERA_STATIC_ALLOCATION: usize = 16 * 1_024 * 1_024;
const UUID_LSB_BASE: u64 = 0x8000_0000_0000_0000;
#[cfg(feature = "payload-contract-benchmarks")]
const PAYLOAD_CONTRACT_SEQUENCE: u32 = 1;
#[cfg(feature = "perf-diagnostics")]
const R8_UP_RUST_HEAD: &str = "eef9c445e2bdddacd6999739c83c1cd7c79a041d";

type StableIceoryx2Transport = StableContainerWireTransport<Iceoryx2PubSub>;
#[cfg(feature = "benchmark-owned")]
type StableOwnedIceoryx2Transport = StableContainerWireTransport<BenchmarkOwnedIceoryx2Core>;
#[cfg(feature = "payload-contract-benchmarks")]
type StableIceoryx2Rx = <StableIceoryx2Transport as UZeroCopyTransport>::Rx;

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
    ProtobufEncodeOnly,
    ProtobufValidateOnly,
    ProtobufOwnedAckOnly,
    ZcInitOnly,
    ZcSendOnly,
    ZcRxOnly,
    ZcValidationOnly,
    ZcFilterOnly,
    ZcCopyLedger,
    ZcSourcePrefilterOnly,
    ZcSourcePrefilterNonmatchOnly,
    ZcWildcardSourceDeliveryOnly,
    ZcSinkQueueDropOnly,
    ZcRxIceoryx2DeliveryOnly,
    ZcRxMetadataPrefixDecodeOnly,
    ZcRxAdapterFilterDropOnly,
    ZcRxSinkQueueDropOnly,
    ZcRxListenerDispatchOnly,
    ZcLoanProvenanceCheck,
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
            "protobuf-encode-only" => Self::ProtobufEncodeOnly,
            "protobuf-validate-only" => Self::ProtobufValidateOnly,
            "protobuf-owned-ack-only" => Self::ProtobufOwnedAckOnly,
            "zc-init-only" => Self::ZcInitOnly,
            "zc-send-only" => Self::ZcSendOnly,
            "zc-rx-only" => Self::ZcRxOnly,
            "zc-validation-only" => Self::ZcValidationOnly,
            "zc-filter-only" => Self::ZcFilterOnly,
            "zc-copy-ledger" => Self::ZcCopyLedger,
            "zc-source-prefilter-only" => Self::ZcSourcePrefilterOnly,
            "zc-source-prefilter-nonmatch-only" => Self::ZcSourcePrefilterNonmatchOnly,
            "zc-wildcard-source-delivery-only" => Self::ZcWildcardSourceDeliveryOnly,
            "zc-sink-queue-drop-only" => Self::ZcSinkQueueDropOnly,
            "zc-rx-iceoryx2-delivery-only" => Self::ZcRxIceoryx2DeliveryOnly,
            "zc-rx-metadata-prefix-decode-only" => Self::ZcRxMetadataPrefixDecodeOnly,
            "zc-rx-adapter-filter-drop-only" => Self::ZcRxAdapterFilterDropOnly,
            "zc-rx-sink-queue-drop-only" => Self::ZcRxSinkQueueDropOnly,
            "zc-rx-listener-dispatch-only" => Self::ZcRxListenerDispatchOnly,
            "zc-loan-provenance-check" => Self::ZcLoanProvenanceCheck,
            other => panic!(
                "TRANSPORT_BENCH_DIAGNOSTIC must be one of authority, prebuilt-payload, metadata-only, tx-only, rx-only, copy-ledger, protobuf-encode-only, protobuf-validate-only, protobuf-owned-ack-only, zc-init-only, zc-send-only, zc-rx-only, zc-validation-only, zc-filter-only, zc-copy-ledger, zc-source-prefilter-only, zc-source-prefilter-nonmatch-only, zc-wildcard-source-delivery-only, zc-sink-queue-drop-only, zc-rx-iceoryx2-delivery-only, zc-rx-metadata-prefix-decode-only, zc-rx-adapter-filter-drop-only, zc-rx-sink-queue-drop-only, zc-rx-listener-dispatch-only, zc-loan-provenance-check; got {other}"
            ),
        }
    }

    fn uses_real_transports(self) -> bool {
        matches!(
            self,
            Self::Authority
                | Self::PrebuiltPayload
                | Self::ZcSendOnly
                | Self::ZcRxOnly
                | Self::ZcSourcePrefilterOnly
                | Self::ZcSourcePrefilterNonmatchOnly
                | Self::ZcWildcardSourceDeliveryOnly
                | Self::ZcSinkQueueDropOnly
                | Self::ZcRxIceoryx2DeliveryOnly
                | Self::ZcRxMetadataPrefixDecodeOnly
                | Self::ZcRxAdapterFilterDropOnly
                | Self::ZcRxSinkQueueDropOnly
                | Self::ZcRxListenerDispatchOnly
                | Self::ZcLoanProvenanceCheck
        )
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
#[derive(Clone, Copy, PartialEq, Eq)]
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
#[derive(Clone, Copy, Default)]
struct DiagnosticTxSinkCore;

#[cfg(all(feature = "payload-contract-benchmarks", feature = "benchmark-owned"))]
#[async_trait::async_trait]
impl UOwnedTransportCore for DiagnosticTxSinkCore {
    async fn send_prepared_owned(&self, frame: PreparedOwnedFrame) -> Result<(), UStatus> {
        black_box(frame.encoded_metadata().len());
        black_box(frame.payload().map_or(0, Bytes::len));
        Ok(())
    }

    async fn receive_encoded_owned(
        &self,
        _source_filter: &UUri,
        _sink_filter: Option<&UUri>,
    ) -> Result<EncodedOwnedFrame, UStatus> {
        unreachable!("diagnostic TX sink does not support receive")
    }

    async fn register_encoded_owned_listener(
        &self,
        _source_filter: &UUri,
        _sink_filter: Option<&UUri>,
        _listener: Arc<dyn UEncodedOwnedListener>,
    ) -> Result<(), UStatus> {
        unreachable!("diagnostic TX sink does not support listeners")
    }

    async fn unregister_encoded_owned_listener(
        &self,
        _source_filter: &UUri,
        _sink_filter: Option<&UUri>,
        _listener: Arc<dyn UEncodedOwnedListener>,
    ) -> Result<(), UStatus> {
        unreachable!("diagnostic TX sink does not support listeners")
    }
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
    nonmatching_source: UUri,
    wildcard_source_filter: UUri,
}

impl BenchCase {
    fn new(case_name: &str) -> Self {
        let authority = format!("iox-userializer-bench-{}-{case_name}", std::process::id());
        Self {
            source: UUri::try_from_parts(&authority, 0x4210, 1, resource_id(next_sequence()))
                .expect("valid benchmark source URI"),
            nonmatching_source: UUri::try_from_parts(
                &authority,
                0x4211,
                1,
                resource_id(next_sequence()),
            )
            .expect("valid benchmark nonmatching source URI"),
            wildcard_source_filter: UUri::try_from_parts(&authority, 0x4210, 1, u16::MAX)
                .expect("valid benchmark wildcard source URI"),
        }
    }

    fn metadata(&self, id: UUID, encoding: Option<PayloadEncoding>) -> UFrameMetadata {
        self.metadata_for_source(self.source.clone(), id, encoding)
    }

    fn nonmatching_metadata(&self, id: UUID, encoding: Option<PayloadEncoding>) -> UFrameMetadata {
        self.metadata_for_source(self.nonmatching_source.clone(), id, encoding)
    }

    fn metadata_for_source(
        &self,
        source: UUri,
        id: UUID,
        encoding: Option<PayloadEncoding>,
    ) -> UFrameMetadata {
        let mut builder = UFrameMetadata::publish(source).with_id(id);
        if let Some(encoding) = encoding {
            builder = builder.with_payload_encoding(encoding);
        }
        builder.build().expect("valid metadata")
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
    zero_copy: Arc<StableIceoryx2Transport>,
    #[cfg(feature = "perf-diagnostics")]
    zero_copy_core: Iceoryx2PubSub,
    #[cfg(feature = "benchmark-owned")]
    owned: Arc<StableOwnedIceoryx2Transport>,
}

#[cfg(feature = "payload-contract-benchmarks")]
#[derive(Default)]
struct P51Iceoryx2Sample<'a> {
    selector: &'a str,
    fixture: &'a str,
    scenario: &'a str,
    publish_attempts: usize,
    exact_source_deliveries: usize,
    wildcard_source_deliveries: usize,
    sink_queue_deliveries: usize,
    source_prefiltered_count: usize,
    wildcard_delivered_count: usize,
    sink_filtered_count: usize,
    adapter_dropped_count: usize,
    listener_dispatched_count: usize,
    metadata_prefix_bytes: usize,
    metadata_copy_bytes: usize,
    payload_copy_bytes: usize,
    user_header_bytes: usize,
    metadata_prefix_encode_allocations: usize,
    metadata_prefix_encode_bytes: usize,
    metadata_prefix_decode_allocations: usize,
    metadata_prefix_decode_bytes: usize,
    source_drop_allocations: usize,
    source_drop_bytes: usize,
    sink_drop_allocations: usize,
    sink_drop_bytes: usize,
    receive_drop_allocations: usize,
    receive_drop_bytes: usize,
}

#[cfg(feature = "payload-contract-benchmarks")]
fn emit_p51_iceoryx2_sample(sample: &P51Iceoryx2Sample<'_>) {
    println!(
        "P51_ICEORYX2_SAMPLE selector={} fixture={} scenario={} publish_attempts={} exact_source_deliveries={} wildcard_source_deliveries={} sink_queue_deliveries={} source_prefiltered_count={} wildcard_delivered_count={} sink_filtered_count={} adapter_dropped_count={} listener_dispatched_count={} metadata_prefix_bytes={} metadata_copy_bytes={} payload_copy_bytes={} user_header_bytes={} metadata_prefix_encode_allocations={} metadata_prefix_encode_bytes={} metadata_prefix_decode_allocations={} metadata_prefix_decode_bytes={} source_drop_allocations={} source_drop_bytes={} sink_drop_allocations={} sink_drop_bytes={} receive_drop_allocations={} receive_drop_bytes={}",
        sample.selector,
        sample.fixture,
        sample.scenario,
        sample.publish_attempts,
        sample.exact_source_deliveries,
        sample.wildcard_source_deliveries,
        sample.sink_queue_deliveries,
        sample.source_prefiltered_count,
        sample.wildcard_delivered_count,
        sample.sink_filtered_count,
        sample.adapter_dropped_count,
        sample.listener_dispatched_count,
        sample.metadata_prefix_bytes,
        sample.metadata_copy_bytes,
        sample.payload_copy_bytes,
        sample.user_header_bytes,
        sample.metadata_prefix_encode_allocations,
        sample.metadata_prefix_encode_bytes,
        sample.metadata_prefix_decode_allocations,
        sample.metadata_prefix_decode_bytes,
        sample.source_drop_allocations,
        sample.source_drop_bytes,
        sample.sink_drop_allocations,
        sample.sink_drop_bytes,
        sample.receive_drop_allocations,
        sample.receive_drop_bytes
    );
}

impl BenchTransports {
    fn build(max_slice_len: usize) -> Self {
        let config = Iceoryx2PubSubConfig::static_allocation(max_slice_len)
            .with_pull_mismatch_queue_capacity(4_096);
        let core = Iceoryx2PubSub::with_config(config);
        #[cfg(feature = "perf-diagnostics")]
        let zero_copy_core = core.clone();
        let zero_copy = Arc::new(core.clone().into_stable_container_transport());
        #[cfg(feature = "benchmark-owned")]
        let owned =
            Arc::new(BenchmarkOwnedIceoryx2Core::new(core).into_stable_container_transport());
        Self {
            zero_copy,
            #[cfg(feature = "perf-diagnostics")]
            zero_copy_core,
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
            let rx_transport = core.clone().into_stable_container_transport();
            let tx_transport = DiagnosticTxSinkCore.into_stable_container_transport();
            let encoded_metadata = NativePrefixFrameMetadataCodec
                .encode_frame_metadata(
                    StableContainerWireFormat::metadata_context(),
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
                                tx_transport
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
                                let frame = rx_transport
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
        let encoded = NativePrefixFrameMetadataCodec
            .encode_frame_metadata(StableContainerWireFormat::metadata_context(), &metadata)
            .expect("diagnostic metadata should encode");
        emit_p51_iceoryx2_sample(&P51Iceoryx2Sample {
            selector: "metadata-only",
            fixture: contract.name(),
            scenario: "metadata-only",
            metadata_prefix_bytes: encoded.len(),
            metadata_copy_bytes: encoded.len(),
            user_header_bytes: std::mem::size_of::<UProtocolHeader>(),
            ..P51Iceoryx2Sample::default()
        });
        group.bench_function(
            BenchmarkId::new(
                "stable_zc_nozero_full_diagnostic_metadata_only",
                format!("metadata/{}/{}", contract.name(), encoded.len()),
            ),
            |b| {
                b.iter(|| {
                    let encoded = NativePrefixFrameMetadataCodec
                        .encode_frame_metadata(
                            StableContainerWireFormat::metadata_context(),
                            black_box(&metadata),
                        )
                        .expect("diagnostic metadata should encode");
                    let decoded = NativePrefixFrameMetadataCodec
                        .decode_frame_metadata(
                            StableContainerWireFormat::metadata_context(),
                            black_box(&encoded),
                        )
                        .expect("diagnostic metadata should decode");
                    black_box(decoded);
                });
            },
        );
    }
    group.finish();
}

#[cfg(all(feature = "payload-contract-benchmarks", feature = "benchmark-owned"))]
fn bench_payload_contract_protobuf_fixture_diagnostic_matrix(
    c: &mut Criterion,
    diagnostic: BenchDiagnostic,
    group_name: &'static str,
    payload_cases: &[PayloadContractCase],
) {
    let _validate = DiagnosticValidate::from_env();
    let mut group = c.benchmark_group(group_name);
    for contract in payload_cases {
        let case = BenchCase::new(contract.name());
        let id_label = match diagnostic {
            BenchDiagnostic::ProtobufEncodeOnly => "diagnostic_protobuf_encode_only",
            BenchDiagnostic::ProtobufValidateOnly => "diagnostic_protobuf_validate_only",
            BenchDiagnostic::ProtobufOwnedAckOnly => "diagnostic_protobuf_owned_ack_only",
            _ => unreachable!("unsupported protobuf fixture diagnostic"),
        };
        let expected_len =
            payload_contract::protobuf_encoded_len(contract, PAYLOAD_CONTRACT_SEQUENCE);
        let prebuilt =
            payload_contract::protobuf_encoded_bytes_for(contract, PAYLOAD_CONTRACT_SEQUENCE)
                .expect("protobuf diagnostic payload should serialize");
        group.bench_function(
            BenchmarkId::new(
                format!("protobuf_owned_full_{id_label}"),
                format!(
                    "fixture/{}/{}/{}",
                    contract.name(),
                    contract.semantic_reference_len(),
                    expected_len
                ),
            ),
            |b| match diagnostic {
                BenchDiagnostic::ProtobufEncodeOnly => {
                    b.iter(|| {
                        let bytes = payload_contract::protobuf_encoded_bytes_for(
                            black_box(contract),
                            PAYLOAD_CONTRACT_SEQUENCE,
                        )
                        .expect("protobuf diagnostic payload should serialize");
                        black_box(bytes.len());
                    });
                }
                BenchDiagnostic::ProtobufValidateOnly => {
                    b.iter(|| {
                        payload_contract::validate_protobuf_bytes(
                            black_box(contract),
                            PAYLOAD_CONTRACT_SEQUENCE,
                            black_box(&prebuilt),
                        )
                        .expect("protobuf diagnostic payload should validate");
                    });
                }
                BenchDiagnostic::ProtobufOwnedAckOnly => {
                    b.iter(|| {
                        let metadata =
                            case.metadata(next_uuid(), Some(ProtobufPayload::encoding()));
                        let frame =
                            UOwnedFrame::with_payload(metadata, Bytes::copy_from_slice(&prebuilt))
                                .expect("valid protobuf diagnostic owned frame");
                        let ack = protobuf_payload_contract_ack(frame, black_box(contract));
                        black_box(ack.transported_payload_len);
                    });
                }
                _ => unreachable!("unsupported protobuf fixture diagnostic"),
            },
        );
    }
    group.finish();
}

#[cfg(feature = "payload-contract-benchmarks")]
fn bench_payload_contract_zero_copy_diagnostic_matrix(
    c: &mut Criterion,
    runtime: &Runtime,
    transports: Option<&BenchTransports>,
    diagnostic: BenchDiagnostic,
    group_name: &'static str,
    payload_cases: &[PayloadContractCase],
    timeout: Duration,
    path_mode: PayloadContractPathMode,
) {
    let _validate = DiagnosticValidate::from_env();
    let mut group = c.benchmark_group(group_name);
    for contract in payload_cases {
        if !payload_contract_paths(path_mode).contains(&PayloadContractPath::StableZcNoZero) {
            continue;
        }
        let case = BenchCase::new(contract.name());
        let metadata = case.metadata(next_uuid(), None);
        let encoded_metadata = NativePrefixFrameMetadataCodec
            .encode_frame_metadata(StableContainerWireFormat::metadata_context(), &metadata)
            .expect("diagnostic zero-copy metadata should encode");
        if let Some(sample) =
            p51_iceoryx2_sample_for(diagnostic, contract.name(), encoded_metadata.len())
        {
            emit_p51_iceoryx2_sample(&sample);
        }
        let payload_len = payload_contract::stable_payload_len(contract);
        let parameter = format!(
            "{}/{}/{}/{}",
            zero_copy_diagnostic_parameter_prefix(diagnostic),
            contract.name(),
            contract.semantic_reference_len(),
            payload_len
        );
        if let Some(transports) = transports {
            runtime.block_on(prime_subscriber(transports, &case));
        }
        group.bench_function(
            BenchmarkId::new(zero_copy_diagnostic_label(diagnostic), parameter),
            |b| match diagnostic {
                BenchDiagnostic::ZcInitOnly => {
                    b.iter(|| {
                        let fixture = payload_contract::stable_owned_fixture_for(
                            black_box(contract),
                            PAYLOAD_CONTRACT_SEQUENCE,
                        )
                        .expect("zero-copy init diagnostic fixture should initialize");
                        black_box(fixture.stable_transport_len);
                        black_box(fixture.stable_align);
                    });
                }
                BenchDiagnostic::ZcSendOnly => {
                    let transports = transports.expect("zc-send-only requires real transports");
                    b.iter_custom(|iters| {
                        runtime.block_on(async {
                            let mut elapsed = Duration::ZERO;
                            for _ in 0..iters {
                                let id = next_uuid();
                                let metadata = case.metadata(id.clone(), None);
                                let start = Instant::now();
                                send_stable_payload_contract(transports, metadata, contract).await;
                                elapsed += start.elapsed();
                                receive_zero_copy_no_validate(
                                    transports,
                                    &case,
                                    &id,
                                    payload_len,
                                    timeout,
                                )
                                .await;
                            }
                            elapsed
                        })
                    });
                }
                BenchDiagnostic::ZcRxOnly => {
                    let transports = transports.expect("zc-rx-only requires real transports");
                    b.iter_custom(|iters| {
                        runtime.block_on(async {
                            let mut elapsed = Duration::ZERO;
                            for _ in 0..iters {
                                let id = next_uuid();
                                let metadata = case.metadata(id.clone(), None);
                                send_stable_payload_contract(transports, metadata, contract).await;
                                let start = Instant::now();
                                receive_zero_copy_no_validate(
                                    transports,
                                    &case,
                                    &id,
                                    payload_len,
                                    timeout,
                                )
                                .await;
                                elapsed += start.elapsed();
                            }
                            elapsed
                        })
                    });
                }
                BenchDiagnostic::ZcValidationOnly => {
                    let fixture = payload_contract::stable_owned_fixture_for(
                        contract,
                        PAYLOAD_CONTRACT_SEQUENCE,
                    )
                    .expect("zero-copy validation diagnostic fixture should initialize");
                    b.iter(|| {
                        payload_contract::validate_stable_owned_bytes(
                            black_box(contract),
                            PAYLOAD_CONTRACT_SEQUENCE,
                            Some(&fixture.encoding),
                            black_box(&fixture.bytes),
                        )
                        .expect("zero-copy validation diagnostic fixture should validate");
                    });
                }
                BenchDiagnostic::ZcFilterOnly => {
                    b.iter(|| {
                        let decoded = NativePrefixFrameMetadataCodec
                            .decode_frame_metadata(
                                StableContainerWireFormat::metadata_context(),
                                black_box(&encoded_metadata),
                            )
                            .expect("zero-copy filter diagnostic metadata should decode");
                        let source_matches = case.source.matches(decoded.source());
                        let sink_matches = decoded.sink().is_none();
                        black_box(source_matches && sink_matches);
                    });
                }
                BenchDiagnostic::ZcCopyLedger => {
                    let enabled = copy_ledger_enabled();
                    let estimated_metadata_copy_bytes = encoded_metadata.len();
                    let estimated_payload_copy_bytes = 0usize;
                    b.iter(|| {
                        let estimated = if enabled {
                            estimated_metadata_copy_bytes + estimated_payload_copy_bytes
                        } else {
                            0
                        };
                        black_box(estimated);
                    });
                }
                BenchDiagnostic::ZcSourcePrefilterOnly
                | BenchDiagnostic::ZcSourcePrefilterNonmatchOnly
                | BenchDiagnostic::ZcRxIceoryx2DeliveryOnly
                | BenchDiagnostic::ZcRxListenerDispatchOnly => {
                    let transports = transports.expect("diagnostic requires real transports");
                    b.iter_custom(|iters| {
                        runtime.block_on(async {
                            let mut elapsed = Duration::ZERO;
                            for _ in 0..iters {
                                let id = next_uuid();
                                let metadata = if matches!(
                                    diagnostic,
                                    BenchDiagnostic::ZcSourcePrefilterNonmatchOnly
                                ) {
                                    case.nonmatching_metadata(id.clone(), None)
                                } else {
                                    case.metadata(id.clone(), None)
                                };
                                send_stable_payload_contract(transports, metadata, contract).await;
                                let start = Instant::now();
                                if matches!(
                                    diagnostic,
                                    BenchDiagnostic::ZcSourcePrefilterNonmatchOnly
                                ) {
                                    let observed = exact_source_observed_once(
                                        transports,
                                        &case,
                                        &id,
                                        Duration::from_millis(1),
                                    )
                                    .await;
                                    assert!(
                                        !observed,
                                        "nonmatching source reached exact source receive filter"
                                    );
                                    elapsed += start.elapsed();
                                    black_box(observed);
                                } else {
                                    let frame =
                                        receive_zero_copy_frame(transports, &case, &id, timeout)
                                            .await;
                                    elapsed += start.elapsed();
                                    black_box(frame.raw().encoded_metadata().len());
                                    black_box(frame.payload_len());
                                }
                            }
                            elapsed
                        })
                    });
                }
                BenchDiagnostic::ZcWildcardSourceDeliveryOnly => {
                    let transports =
                        transports.expect("wildcard delivery requires real transports");
                    b.iter_custom(|iters| {
                        runtime.block_on(async {
                            let mut elapsed = Duration::ZERO;
                            for _ in 0..iters {
                                let id = next_uuid();
                                let metadata = case.metadata(id.clone(), None);
                                send_stable_payload_contract(transports, metadata, contract).await;
                                let start = Instant::now();
                                let frame = receive_zero_copy_frame_for_filter(
                                    transports,
                                    &case.wildcard_source_filter,
                                    &id,
                                    timeout,
                                )
                                .await;
                                elapsed += start.elapsed();
                                black_box(frame.raw().encoded_metadata().len());
                            }
                            elapsed
                        })
                    });
                }
                BenchDiagnostic::ZcRxMetadataPrefixDecodeOnly => {
                    let transports =
                        transports.expect("metadata prefix decode requires real transports");
                    b.iter_custom(|iters| {
                        runtime.block_on(async {
                            let mut elapsed = Duration::ZERO;
                            for _ in 0..iters {
                                let id = next_uuid();
                                let metadata = case.metadata(id.clone(), None);
                                send_stable_payload_contract(transports, metadata, contract).await;
                                let frame =
                                    receive_zero_copy_frame(transports, &case, &id, timeout).await;
                                let start = Instant::now();
                                let decoded = NativePrefixFrameMetadataCodec
                                    .decode_frame_metadata(
                                        StableContainerWireFormat::metadata_context(),
                                        frame.raw().encoded_metadata(),
                                    )
                                    .expect("metadata prefix should decode");
                                elapsed += start.elapsed();
                                black_box(decoded);
                            }
                            elapsed
                        })
                    });
                }
                BenchDiagnostic::ZcRxAdapterFilterDropOnly => {
                    b.iter(|| {
                        let decoded = NativePrefixFrameMetadataCodec
                            .decode_frame_metadata(
                                StableContainerWireFormat::metadata_context(),
                                black_box(&encoded_metadata),
                            )
                            .expect("adapter drop diagnostic metadata should decode");
                        let dropped = !case.wildcard_source_filter.matches(decoded.source())
                            || decoded.sink().is_some();
                        black_box(dropped);
                    });
                }
                BenchDiagnostic::ZcSinkQueueDropOnly | BenchDiagnostic::ZcRxSinkQueueDropOnly => {
                    let transports =
                        transports.expect("sink queue diagnostic requires real transports");
                    b.iter_custom(|iters| {
                        runtime.block_on(async {
                            let mut elapsed = Duration::ZERO;
                            for _ in 0..iters {
                                let id = next_uuid();
                                let metadata = case.metadata(id.clone(), None);
                                send_stable_payload_contract(transports, metadata, contract).await;
                                let start = Instant::now();
                                let frame =
                                    receive_zero_copy_frame(transports, &case, &id, timeout).await;
                                elapsed += start.elapsed();
                                black_box(frame.payload_len());
                            }
                            elapsed
                        })
                    });
                }
                BenchDiagnostic::ZcLoanProvenanceCheck => {
                    let transports =
                        transports.expect("zc-loan-provenance-check requires real transports");
                    b.iter(|| {
                        runtime.block_on(async {
                            let id = next_uuid();
                            let metadata = case.metadata(id.clone(), None);
                            send_stable_payload_contract(transports, metadata, contract).await;
                            let frame =
                                receive_zero_copy_frame(transports, &case, &id, timeout).await;
                            black_box(
                                frame
                                    .payload_loan_provenance()
                                    .expect("zero-copy diagnostic frame should be loan-backed"),
                            );
                            black_box(frame.payload_len());
                        });
                    });
                }
                _ => unreachable!("unsupported zero-copy diagnostic"),
            },
        );
    }
    group.finish();
}

#[cfg(feature = "payload-contract-benchmarks")]
fn zero_copy_diagnostic_label(diagnostic: BenchDiagnostic) -> &'static str {
    match diagnostic {
        BenchDiagnostic::ZcInitOnly => "stable_zc_nozero_full_diagnostic_zc_init_only",
        BenchDiagnostic::ZcSendOnly => "stable_zc_nozero_full_diagnostic_zc_send_only",
        BenchDiagnostic::ZcRxOnly => "stable_zc_nozero_full_diagnostic_zc_rx_only",
        BenchDiagnostic::ZcValidationOnly => "stable_zc_nozero_full_diagnostic_zc_validation_only",
        BenchDiagnostic::ZcFilterOnly => "stable_zc_nozero_full_diagnostic_zc_filter_only",
        BenchDiagnostic::ZcCopyLedger => "stable_zc_nozero_full_diagnostic_zc_copy_ledger",
        BenchDiagnostic::ZcSourcePrefilterOnly => {
            "stable_zc_nozero_full_diagnostic_zc_source_prefilter_only"
        }
        BenchDiagnostic::ZcSourcePrefilterNonmatchOnly => {
            "stable_zc_nozero_full_diagnostic_zc_source_prefilter_nonmatch_only"
        }
        BenchDiagnostic::ZcWildcardSourceDeliveryOnly => {
            "stable_zc_nozero_full_diagnostic_zc_wildcard_source_delivery_only"
        }
        BenchDiagnostic::ZcSinkQueueDropOnly => {
            "stable_zc_nozero_full_diagnostic_zc_sink_queue_drop_only"
        }
        BenchDiagnostic::ZcRxIceoryx2DeliveryOnly => {
            "stable_zc_nozero_full_diagnostic_zc_rx_iceoryx2_delivery_only"
        }
        BenchDiagnostic::ZcRxMetadataPrefixDecodeOnly => {
            "stable_zc_nozero_full_diagnostic_zc_rx_metadata_prefix_decode_only"
        }
        BenchDiagnostic::ZcRxAdapterFilterDropOnly => {
            "stable_zc_nozero_full_diagnostic_zc_rx_adapter_filter_drop_only"
        }
        BenchDiagnostic::ZcRxSinkQueueDropOnly => {
            "stable_zc_nozero_full_diagnostic_zc_rx_sink_queue_drop_only"
        }
        BenchDiagnostic::ZcRxListenerDispatchOnly => {
            "stable_zc_nozero_full_diagnostic_zc_rx_listener_dispatch_only"
        }
        BenchDiagnostic::ZcLoanProvenanceCheck => {
            "stable_zc_nozero_full_diagnostic_zc_loan_provenance_check"
        }
        _ => unreachable!("unsupported zero-copy diagnostic"),
    }
}

#[cfg(feature = "payload-contract-benchmarks")]
fn zero_copy_diagnostic_parameter_prefix(diagnostic: BenchDiagnostic) -> &'static str {
    match diagnostic {
        BenchDiagnostic::ZcInitOnly => "zc-init",
        BenchDiagnostic::ZcSendOnly => "zc-send",
        BenchDiagnostic::ZcRxOnly => "zc-rx",
        BenchDiagnostic::ZcValidationOnly => "zc-validation",
        BenchDiagnostic::ZcFilterOnly => "zc-filter",
        BenchDiagnostic::ZcCopyLedger => "zc-copy-ledger",
        BenchDiagnostic::ZcSourcePrefilterOnly => "zc-source-prefilter",
        BenchDiagnostic::ZcSourcePrefilterNonmatchOnly => "zc-source-prefilter-nonmatch",
        BenchDiagnostic::ZcWildcardSourceDeliveryOnly => "zc-wildcard-source-delivery",
        BenchDiagnostic::ZcSinkQueueDropOnly => "zc-sink-queue-drop",
        BenchDiagnostic::ZcRxIceoryx2DeliveryOnly => "zc-rx-iceoryx2-delivery",
        BenchDiagnostic::ZcRxMetadataPrefixDecodeOnly => "zc-rx-metadata-prefix-decode",
        BenchDiagnostic::ZcRxAdapterFilterDropOnly => "zc-rx-adapter-filter-drop",
        BenchDiagnostic::ZcRxSinkQueueDropOnly => "zc-rx-sink-queue-drop",
        BenchDiagnostic::ZcRxListenerDispatchOnly => "zc-rx-listener-dispatch",
        BenchDiagnostic::ZcLoanProvenanceCheck => "zc-loan-provenance",
        _ => unreachable!("unsupported zero-copy diagnostic"),
    }
}

#[cfg(feature = "payload-contract-benchmarks")]
fn p51_iceoryx2_sample_for<'a>(
    diagnostic: BenchDiagnostic,
    fixture: &'a str,
    encoded_metadata_len: usize,
) -> Option<P51Iceoryx2Sample<'a>> {
    let selector = zero_copy_diagnostic_parameter_prefix(diagnostic);
    let mut sample = P51Iceoryx2Sample {
        selector,
        fixture,
        scenario: selector,
        metadata_prefix_bytes: encoded_metadata_len,
        user_header_bytes: std::mem::size_of::<UProtocolHeader>(),
        ..P51Iceoryx2Sample::default()
    };
    match diagnostic {
        BenchDiagnostic::ZcCopyLedger => {
            sample.metadata_copy_bytes = encoded_metadata_len;
            sample.payload_copy_bytes = 0;
            Some(sample)
        }
        BenchDiagnostic::ZcSourcePrefilterOnly | BenchDiagnostic::ZcFilterOnly => {
            sample.publish_attempts = 1;
            sample.exact_source_deliveries = 1;
            Some(sample)
        }
        BenchDiagnostic::ZcSourcePrefilterNonmatchOnly => {
            sample.scenario = "source-nonmatch-not-observed";
            sample.publish_attempts = 1;
            sample.source_prefiltered_count = 1;
            Some(sample)
        }
        BenchDiagnostic::ZcWildcardSourceDeliveryOnly => {
            sample.publish_attempts = 1;
            sample.wildcard_source_deliveries = 1;
            sample.wildcard_delivered_count = 1;
            Some(sample)
        }
        BenchDiagnostic::ZcRxOnly => {
            sample.publish_attempts = 1;
            sample.exact_source_deliveries = 1;
            sample.metadata_prefix_decode_bytes = encoded_metadata_len;
            Some(sample)
        }
        BenchDiagnostic::ZcSinkQueueDropOnly | BenchDiagnostic::ZcRxSinkQueueDropOnly => {
            sample.publish_attempts = 1;
            sample.sink_queue_deliveries = 1;
            sample.sink_filtered_count = 1;
            Some(sample)
        }
        BenchDiagnostic::ZcRxIceoryx2DeliveryOnly => {
            sample.publish_attempts = 1;
            sample.exact_source_deliveries = 1;
            Some(sample)
        }
        BenchDiagnostic::ZcRxMetadataPrefixDecodeOnly => {
            sample.publish_attempts = 1;
            sample.exact_source_deliveries = 1;
            sample.metadata_prefix_decode_bytes = encoded_metadata_len;
            Some(sample)
        }
        BenchDiagnostic::ZcRxAdapterFilterDropOnly => {
            sample.publish_attempts = 1;
            sample.exact_source_deliveries = 1;
            sample.adapter_dropped_count = 1;
            Some(sample)
        }
        BenchDiagnostic::ZcRxListenerDispatchOnly => {
            sample.publish_attempts = 1;
            sample.exact_source_deliveries = 1;
            sample.listener_dispatched_count = 1;
            Some(sample)
        }
        _ => None,
    }
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
                .send_uninit_stable_payload::<CanClassicFrameV1>(metadata, |payload| {
                    payload_contract::init_can_classic_max(
                        payload.into_init(),
                        PAYLOAD_CONTRACT_SEQUENCE,
                    )
                })
                .await
        }
        PayloadContractCaseKind::CanFdMax => {
            transports
                .zero_copy
                .send_uninit_stable_payload::<CanFdFrameV1>(metadata, |payload| {
                    payload_contract::init_can_fd_max(
                        payload.into_init(),
                        PAYLOAD_CONTRACT_SEQUENCE,
                    )
                })
                .await
        }
        PayloadContractCaseKind::SomeIpSingleMtu => {
            transports
                .zero_copy
                .send_uninit_stable_payload::<SomeIpSignalBatchMtuV1>(metadata, |payload| {
                    payload_contract::init_someip_single_mtu(
                        payload.into_init(),
                        PAYLOAD_CONTRACT_SEQUENCE,
                    )
                })
                .await
        }
        PayloadContractCaseKind::Streamer4k => {
            transports
                .zero_copy
                .send_uninit_stable_payload::<StreamChunk4kV1>(metadata, |payload| {
                    payload_contract::init_streamer_4k(
                        payload.into_init(),
                        PAYLOAD_CONTRACT_SEQUENCE,
                    )
                })
                .await
        }
        PayloadContractCaseKind::RadarArs548DetectionList => {
            transports
                .zero_copy
                .send_uninit_stable_payload::<RadarDetectionListArs548V1>(metadata, |payload| {
                    payload_contract::init_radar_ars548_detection_list(
                        payload.into_init(),
                        PAYLOAD_CONTRACT_SEQUENCE,
                    )
                })
                .await
        }
        PayloadContractCaseKind::Streamer64k => {
            transports
                .zero_copy
                .send_uninit_stable_payload::<StreamChunk64kV1>(metadata, |payload| {
                    payload_contract::init_streamer_64k(
                        payload.into_init(),
                        PAYLOAD_CONTRACT_SEQUENCE,
                    )
                })
                .await
        }
        #[cfg(feature = "payload-contract-large-benchmarks")]
        PayloadContractCaseKind::LidarHesaiAt128PointCloud => {
            transports
                .zero_copy
                .send_uninit_stable_payload::<LidarPointCloudHesaiAt128V1>(metadata, |payload| {
                    payload_contract::init_lidar_hesai_at128_point_cloud(
                        payload.into_init(),
                        PAYLOAD_CONTRACT_SEQUENCE,
                    )
                })
                .await
        }
        #[cfg(feature = "payload-contract-large-benchmarks")]
        PayloadContractCaseKind::Camera8mpBayerRggb12p => {
            transports
                .zero_copy
                .send_uninit_stable_payload::<CameraBayerRggb12pFrame8mpV1>(metadata, |payload| {
                    payload_contract::init_camera_8mp_bayer_rggb12p(
                        payload.into_init(),
                        PAYLOAD_CONTRACT_SEQUENCE,
                    )
                })
                .await
        }
    }
    .expect("iceoryx2 payload-contract stable no-zero send should succeed");
}

#[cfg(feature = "perf-diagnostics")]
async fn send_stable_payload_contract_phased(
    transports: &BenchTransports,
    metadata: UFrameMetadata,
    contract: &PayloadContractCase,
) -> Result<UninitStableSendPhases, UStatus> {
    match contract.kind() {
        PayloadContractCaseKind::CanClassicMax => {
            transports
                .zero_copy
                .send_uninit_stable_payload_phased::<CanClassicFrameV1>(metadata, |payload| {
                    payload_contract::init_can_classic_max(
                        payload.into_init(),
                        PAYLOAD_CONTRACT_SEQUENCE,
                    )
                })
                .await
        }
        PayloadContractCaseKind::CanFdMax => {
            transports
                .zero_copy
                .send_uninit_stable_payload_phased::<CanFdFrameV1>(metadata, |payload| {
                    payload_contract::init_can_fd_max(
                        payload.into_init(),
                        PAYLOAD_CONTRACT_SEQUENCE,
                    )
                })
                .await
        }
        PayloadContractCaseKind::SomeIpSingleMtu => {
            transports
                .zero_copy
                .send_uninit_stable_payload_phased::<SomeIpSignalBatchMtuV1>(metadata, |payload| {
                    payload_contract::init_someip_single_mtu(
                        payload.into_init(),
                        PAYLOAD_CONTRACT_SEQUENCE,
                    )
                })
                .await
        }
        PayloadContractCaseKind::Streamer4k => {
            transports
                .zero_copy
                .send_uninit_stable_payload_phased::<StreamChunk4kV1>(metadata, |payload| {
                    payload_contract::init_streamer_4k(
                        payload.into_init(),
                        PAYLOAD_CONTRACT_SEQUENCE,
                    )
                })
                .await
        }
        PayloadContractCaseKind::RadarArs548DetectionList => {
            transports
                .zero_copy
                .send_uninit_stable_payload_phased::<RadarDetectionListArs548V1>(
                    metadata,
                    |payload| {
                        payload_contract::init_radar_ars548_detection_list(
                            payload.into_init(),
                            PAYLOAD_CONTRACT_SEQUENCE,
                        )
                    },
                )
                .await
        }
        PayloadContractCaseKind::Streamer64k => {
            transports
                .zero_copy
                .send_uninit_stable_payload_phased::<StreamChunk64kV1>(metadata, |payload| {
                    payload_contract::init_streamer_64k(
                        payload.into_init(),
                        PAYLOAD_CONTRACT_SEQUENCE,
                    )
                })
                .await
        }
        PayloadContractCaseKind::LidarHesaiAt128PointCloud => {
            transports
                .zero_copy
                .send_uninit_stable_payload_phased::<LidarPointCloudHesaiAt128V1>(
                    metadata,
                    |payload| {
                        payload_contract::init_lidar_hesai_at128_point_cloud(
                            payload.into_init(),
                            PAYLOAD_CONTRACT_SEQUENCE,
                        )
                    },
                )
                .await
        }
        PayloadContractCaseKind::Camera8mpBayerRggb12p => {
            transports
                .zero_copy
                .send_uninit_stable_payload_phased::<CameraBayerRggb12pFrame8mpV1>(
                    metadata,
                    |payload| {
                        payload_contract::init_camera_8mp_bayer_rggb12p(
                            payload.into_init(),
                            PAYLOAD_CONTRACT_SEQUENCE,
                        )
                    },
                )
                .await
        }
    }
}

#[cfg(feature = "perf-diagnostics")]
type PhaseTraceContext<'a> = (
    &'a BenchTransports,
    &'a BenchCase,
    &'a PayloadContractCase,
    &'a mut BufWriter<File>,
);

#[cfg(feature = "perf-diagnostics")]
async fn phase_trace_sample(
    context: PhaseTraceContext<'_>,
    stage: &str,
    run: usize,
    iteration: usize,
) {
    let (transports, case, contract, writer) = context;
    let id = next_uuid();
    let full_loop_start = Instant::now();
    let phases =
        send_stable_payload_contract_phased(transports, case.metadata(id.clone(), None), contract)
            .await
            .expect("phase trace selected-wire send should succeed");

    let receive_start = Instant::now();
    let frame = receive_zero_copy_frame(transports, case, &id, LARGE_SENSOR_BENCH_TIMEOUT).await;
    let receive = receive_start.elapsed();

    let validate_start = Instant::now();
    assert_eq!(frame.metadata().id(), &id);
    assert_eq!(
        frame.metadata().kind().to_legacy_type(),
        UMessageType::Publish
    );
    assert_eq!(
        frame.payload_len(),
        payload_contract_transported_len(PayloadContractPath::StableZcNoZero, contract)
    );
    black_box(
        frame
            .payload_loan_provenance()
            .expect("phase trace payload should retain loan provenance"),
    );
    validate_stable_payload_for_case(&frame, contract);
    let validate = validate_start.elapsed();

    let release_start = Instant::now();
    drop(frame);
    let release = release_start.elapsed();
    let full_loop_total = full_loop_start.elapsed();

    let row = serde_json::json!({
        "record_type": "sample",
        "transport": "iceoryx2",
        "case": contract.name(),
        "semantic_reference_len": contract.semantic_reference_len(),
        "transported_payload_len": payload_contract_transported_len(PayloadContractPath::StableZcNoZero, contract),
        "stage": stage,
        "run": run,
        "iteration": iteration,
        "cold_first_sample": stage == "warmup" && iteration == 0,
        "prepare_ns": phases.prepare.as_nanos(),
        "loan_ns": phases.loan.as_nanos(),
        "verify_ns": phases.verify.as_nanos(),
        "initialize_ns": phases.initialize.as_nanos(),
        "witness_ns": phases.witness.as_nanos(),
        "commit_ns": phases.commit.as_nanos(),
        "send_total_ns": phases.total.as_nanos(),
        "send_residual_ns": phases.residual.as_nanos(),
        "receive_ns": receive.as_nanos(),
        "validate_ns": validate.as_nanos(),
        "release_ns": release.as_nanos(),
        "full_loop_total_ns": full_loop_total.as_nanos(),
    });
    serde_json::to_writer(&mut *writer, &row).expect("serialize phase trace row");
    writer.write_all(b"\n").expect("write phase trace newline");
}

#[cfg(feature = "perf-diagnostics")]
fn phase_trace_case_filter_allows(contract: &PayloadContractCase) -> bool {
    let Ok(filter) = std::env::var("TRANSPORT_BENCH_CASE_FILTER") else {
        return true;
    };
    filter
        .split(',')
        .map(str::trim)
        .filter(|case| !case.is_empty())
        .any(|case| case == contract.name())
}

#[cfg(feature = "perf-diagnostics")]
fn run_phase_trace_cases(
    runtime: &Runtime,
    writer: &mut BufWriter<File>,
    cases: &[PayloadContractCase],
    static_allocation: usize,
    run: usize,
) {
    for contract in cases
        .iter()
        .filter(|contract| phase_trace_case_filter_allows(contract))
    {
        let transports = runtime.block_on(async { BenchTransports::build(static_allocation) });
        let case = BenchCase::new(contract.name());
        runtime.block_on(async {
            prime_subscriber(&transports, &case).await;
            for iteration in 0..16 {
                phase_trace_sample(
                    (&transports, &case, contract, writer),
                    "warmup",
                    0,
                    iteration,
                )
                .await;
            }
            for iteration in 0..64 {
                phase_trace_sample(
                    (&transports, &case, contract, writer),
                    "measured",
                    run,
                    iteration,
                )
                .await;
            }
            let queue = transports
                .zero_copy_core
                .pull_mismatch_queue_diagnostics()
                .await;
            let lifecycle = serde_json::json!({
                "record_type": "lifecycle",
                "transport": "iceoryx2",
                "case": contract.name(),
                "run": run,
                "static_allocation": static_allocation,
                "sent": 80,
                "received": 80,
                "released": 80,
                "mismatch_queue_depth": queue.current_depth,
                "dropped_mismatches": queue.dropped_mismatches,
                "rejected_mismatches": queue.rejected_mismatches,
            });
            serde_json::to_writer(&mut *writer, &lifecycle)
                .expect("serialize phase trace lifecycle");
            writer
                .write_all(b"\n")
                .expect("write phase trace lifecycle newline");
        });
        writer.flush().expect("flush phase trace output");
    }
}

#[cfg(feature = "perf-diagnostics")]
fn run_phase_trace() {
    let output = std::env::var("TRANSPORT_BENCH_PHASE_TRACE_OUTPUT")
        .expect("TRANSPORT_BENCH_PHASE_TRACE_OUTPUT is required for phase trace mode");
    let run = std::env::var("TRANSPORT_BENCH_PHASE_RUN_ID")
        .unwrap_or_else(|_| "1".to_string())
        .parse::<usize>()
        .expect("TRANSPORT_BENCH_PHASE_RUN_ID should be an unsigned integer");
    let file = File::create(output).expect("create phase trace output");
    let mut writer = BufWriter::new(file);
    let metadata = serde_json::json!({
        "record_type": "metadata",
        "transport": "iceoryx2",
        "up_rust_head": R8_UP_RUST_HEAD,
        "warmup_iterations": 16,
        "measured_runs": 1,
        "run_id": run,
        "iterations_per_run": 64,
        "clock_overhead_ns": UninitStableSendPhases::calibrate_clock_overhead(10_000).as_nanos(),
        "sequence": PAYLOAD_CONTRACT_SEQUENCE,
    });
    serde_json::to_writer(&mut writer, &metadata).expect("serialize phase trace metadata");
    writer
        .write_all(b"\n")
        .expect("write phase trace metadata newline");

    let runtime = Runtime::new().expect("tokio runtime");
    run_phase_trace_cases(
        &runtime,
        &mut writer,
        payload_contract::core_cases(),
        CORE_STATIC_ALLOCATION,
        run,
    );
    run_phase_trace_cases(
        &runtime,
        &mut writer,
        payload_contract::large_sensor_cases(),
        CAMERA_STATIC_ALLOCATION,
        run,
    );
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

#[cfg(feature = "payload-contract-benchmarks")]
async fn receive_zero_copy_frame(
    transports: &BenchTransports,
    case: &BenchCase,
    expected_id: &UUID,
    timeout: Duration,
) -> StableIceoryx2Rx {
    receive_zero_copy_frame_for_filter(transports, &case.source, expected_id, timeout).await
}

#[cfg(feature = "payload-contract-benchmarks")]
async fn receive_zero_copy_frame_for_filter(
    transports: &BenchTransports,
    source_filter: &UUri,
    expected_id: &UUID,
    timeout: Duration,
) -> StableIceoryx2Rx {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "timed out waiting for matching iceoryx2 zero-copy diagnostic frame"
        );
        let result = tokio::time::timeout(
            remaining,
            transports.zero_copy.receive_zero_copy(source_filter, None),
        )
        .await
        .expect("timed out waiting for iceoryx2 zero-copy diagnostic receive");
        match result {
            Ok(frame) if frame.metadata().id() == expected_id => return frame,
            Ok(_) => continue,
            Err(status) if status.get_code() == UCode::NotFound => {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            Err(status) => {
                panic!("unexpected iceoryx2 zero-copy diagnostic receive error: {status:?}")
            }
        }
    }
}

#[cfg(feature = "payload-contract-benchmarks")]
async fn exact_source_observed_once(
    transports: &BenchTransports,
    case: &BenchCase,
    unexpected_id: &UUID,
    timeout: Duration,
) -> bool {
    match tokio::time::timeout(
        timeout,
        transports.zero_copy.receive_zero_copy(&case.source, None),
    )
    .await
    {
        Ok(Ok(frame)) => frame.metadata().id() == unexpected_id,
        Ok(Err(status)) if status.get_code() == UCode::NotFound => false,
        Ok(Err(status)) => {
            panic!("unexpected iceoryx2 source-prefilter probe error: {status:?}")
        }
        Err(_) => false,
    }
}

#[cfg(feature = "payload-contract-benchmarks")]
async fn receive_zero_copy_no_validate(
    transports: &BenchTransports,
    case: &BenchCase,
    expected_id: &UUID,
    expected_payload_len: usize,
    timeout: Duration,
) {
    let frame = receive_zero_copy_frame(transports, case, expected_id, timeout).await;
    black_box(
        frame
            .payload_loan_provenance()
            .expect("zero-copy diagnostic frame should be loan-backed"),
    );
    assert_eq!(frame.payload_len(), expected_payload_len);
    black_box(frame.payload_len());
}

#[cfg(all(feature = "payload-contract-benchmarks", feature = "benchmark-owned"))]
fn protobuf_payload_contract_ack(
    frame: UOwnedFrame,
    contract: &PayloadContractCase,
) -> PayloadContractAck {
    let transported_payload_len = frame.payload_bytes().len();
    let id = frame.metadata().id().clone();
    let message_type = frame.metadata().kind().to_legacy_type();
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
        id: frame.metadata().id().clone(),
        message_type: frame.metadata().kind().to_legacy_type(),
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
        id: frame.metadata().id().clone(),
        message_type: frame.metadata().kind().to_legacy_type(),
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
    if std::env::var_os("TRANSPORT_BENCH_PHASE_TRACE").is_some() {
        #[cfg(feature = "perf-diagnostics")]
        {
            run_phase_trace();
            return;
        }
        #[cfg(not(feature = "perf-diagnostics"))]
        panic!("TRANSPORT_BENCH_PHASE_TRACE requires feature perf-diagnostics");
    }
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
        #[cfg(feature = "benchmark-owned")]
        BenchDiagnostic::ProtobufEncodeOnly
        | BenchDiagnostic::ProtobufValidateOnly
        | BenchDiagnostic::ProtobufOwnedAckOnly => {
            bench_payload_contract_protobuf_fixture_diagnostic_matrix(
                c,
                diagnostic,
                group_name,
                payload_cases,
            );
        }
        BenchDiagnostic::ZcInitOnly
        | BenchDiagnostic::ZcSendOnly
        | BenchDiagnostic::ZcRxOnly
        | BenchDiagnostic::ZcValidationOnly
        | BenchDiagnostic::ZcFilterOnly
        | BenchDiagnostic::ZcCopyLedger
        | BenchDiagnostic::ZcSourcePrefilterOnly
        | BenchDiagnostic::ZcSourcePrefilterNonmatchOnly
        | BenchDiagnostic::ZcWildcardSourceDeliveryOnly
        | BenchDiagnostic::ZcSinkQueueDropOnly
        | BenchDiagnostic::ZcRxIceoryx2DeliveryOnly
        | BenchDiagnostic::ZcRxMetadataPrefixDecodeOnly
        | BenchDiagnostic::ZcRxAdapterFilterDropOnly
        | BenchDiagnostic::ZcRxSinkQueueDropOnly
        | BenchDiagnostic::ZcRxListenerDispatchOnly
        | BenchDiagnostic::ZcLoanProvenanceCheck => {
            bench_payload_contract_zero_copy_diagnostic_matrix(
                c,
                runtime,
                transports,
                diagnostic,
                group_name,
                payload_cases,
                timeout,
                path_mode,
            );
        }
        #[cfg(not(feature = "benchmark-owned"))]
        BenchDiagnostic::PrebuiltPayload
        | BenchDiagnostic::TxOnly
        | BenchDiagnostic::RxOnly
        | BenchDiagnostic::CopyLedger
        | BenchDiagnostic::ProtobufEncodeOnly
        | BenchDiagnostic::ProtobufValidateOnly
        | BenchDiagnostic::ProtobufOwnedAckOnly => {
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
