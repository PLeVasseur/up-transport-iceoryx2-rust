#!/usr/bin/env bash
#
# Copyright (c) 2026 Contributors to the Eclipse Foundation
#
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

readonly DEFAULT_CRITERION_ARGS="--sample-size 60 --warm-up-time 3 --measurement-time 12 --noise-threshold 0.02"
readonly DEFAULT_LARGE_SENSOR_CRITERION_ARGS="--sample-size 20 --warm-up-time 2 --measurement-time 8 --noise-threshold 0.03"
readonly BASELINE_NAME="transport_owned_zc_baseline"
readonly TRANSPORT_NAME="iceoryx2"
readonly DEFAULT_REPORT_DIR="target/transport-perf/$TRANSPORT_NAME"

CRITERION_ARGS="${CRITERION_ARGS:-$DEFAULT_CRITERION_ARGS}"
LARGE_SENSOR_CRITERION_ARGS="${LARGE_SENSOR_CRITERION_ARGS:-$DEFAULT_LARGE_SENSOR_CRITERION_ARGS}"
BENCH_PIN_PREFIX="${BENCH_PIN_PREFIX:-}"
TRANSPORT_BENCH_PROFILE="${TRANSPORT_BENCH_PROFILE:-all}"
TRANSPORT_BENCH_SUITE="${TRANSPORT_BENCH_SUITE:-raw}"
TRANSPORT_BENCH_REPORT_DIR="${TRANSPORT_BENCH_REPORT_DIR:-$DEFAULT_REPORT_DIR}"

usage() {
    cat <<'USAGE'
Usage:
  scripts/bench_transport_criterion.sh baseline
  scripts/bench_transport_criterion.sh candidate <phase_candidate>
  scripts/bench_transport_criterion.sh guardrail <phase_candidate> <report_path>
  scripts/bench_transport_criterion.sh export

Set TRANSPORT_BENCH_SUITE=raw, payload-contract, or all. The default is raw.
USAGE
}

run_cargo_bench() {
    local profile="$1"
    local criterion_args="$2"
    local cargo_features="benchmark-owned"
    shift 2

    case "$TRANSPORT_BENCH_SUITE" in
        raw) ;;
        payload-contract|all)
            cargo_features="$cargo_features payload-contract-benchmarks"
            ;;
        *)
            echo "TRANSPORT_BENCH_SUITE must be one of raw, payload-contract, all" >&2
            exit 2
            ;;
    esac

    if [[ -n "$BENCH_PIN_PREFIX" ]]; then
        read -r -a pin_parts <<<"$BENCH_PIN_PREFIX"
        TRANSPORT_BENCH_SUITE="$TRANSPORT_BENCH_SUITE" TRANSPORT_BENCH_PROFILE="$profile" "${pin_parts[@]}" cargo bench --features "$cargo_features" --bench transport_criterion -- $criterion_args "$@"
    else
        TRANSPORT_BENCH_SUITE="$TRANSPORT_BENCH_SUITE" TRANSPORT_BENCH_PROFILE="$profile" cargo bench --features "$cargo_features" --bench transport_criterion -- $criterion_args "$@"
    fi
}

run_selected_profiles() {
    local baseline_flag="$1"
    local baseline_value="$2"
    shift 2
    case "$TRANSPORT_BENCH_PROFILE" in
        core)
            run_cargo_bench core "$CRITERION_ARGS" "$baseline_flag" "$baseline_value" "$@"
            ;;
        camera)
            run_cargo_bench camera "$LARGE_SENSOR_CRITERION_ARGS" "$baseline_flag" "$baseline_value" "$@"
            ;;
        all)
            run_cargo_bench core "$CRITERION_ARGS" "$baseline_flag" "$baseline_value" "$@"
            run_cargo_bench camera "$LARGE_SENSOR_CRITERION_ARGS" "$baseline_flag" "$baseline_value" "$@"
            ;;
        *)
            echo "TRANSPORT_BENCH_PROFILE must be one of core, camera, all" >&2
            exit 2
            ;;
    esac
}

write_summary() {
    local report_dir="$1"
    local summary="$report_dir/README.md"
    local rust_version
    local cpu_model
    rust_version="$(rustc --version)"
    cpu_model="$(awk -F': ' '/model name/ { print $2; exit }' /proc/cpuinfo 2>/dev/null || true)"
    cpu_model="${cpu_model:-unknown}"

    cat >"$summary" <<SUMMARY
# iceoryx2 Owned vs Zero-Copy Transport Benchmarks

## Environment

- Transport: iceoryx2 with feature flag \`benchmark-owned\`
- Rust: \`$rust_version\`
- OS: \`$(uname -srmo)\`
- CPU: \`$cpu_model\`
- Core Criterion args: \`$CRITERION_ARGS\`
- Large sensor Criterion args: \`$LARGE_SENSOR_CRITERION_ARGS\`
- Suite: \`$TRANSPORT_BENCH_SUITE\`
- Pinning prefix: \`${BENCH_PIN_PREFIX:-none}\`

## Methodology

The \`owned\` path uses \`BenchmarkOwnedIceoryx2PubSub\`, a benchmark-only wrapper behind \`benchmark-owned\` that copies owned frame payload bytes into iceoryx2 transmit loans and copies receive leases back into owned frames. It is not the generic copying adapter.

The headline comparator is \`owned\` vs \`zero_copy_loan_copy\`. The \`zero_copy_uninit_direct\` path is supporting best-case direct true-zero-copy transmit data.

Payload-contract suite: \`protobuf_owned_full\` constructs generated protobuf \`BenchPayload\`, serializes through \`ProtobufPayload\`, sends over \`BenchmarkOwnedIceoryx2PubSub\`, receives owned bytes, deserializes, and validates scalar fields plus representative payload bytes. \`stable_zc_nozero_full\` initializes nested \`StableBenchPayloadN\` structs directly in zero-copy loan storage with compile-time checked no-zero \`StablePayloadInit\`, receives a loan-backed frame, borrows the stable typed view, and validates the same public payload contract.

Payload-contract transported bytes are intentionally contract-specific: protobuf reports encoded \`BenchPayload\` bytes, while stable reports \`size_of::<StableBenchPayloadN>()\` (logical payload bytes plus a 16-byte header/checksum). This is application payload-contract data, not RawBytes transport-boundary data.

Core payload cases: \`empty_present\` 0 B, \`can_classic_max\` 8 B, \`can_fd_max\` 64 B, \`someip_single_mtu\` 1456 B, \`streamer_4k\` 4096 B, \`radar_ars548_detection_list\` 35336 B, and \`streamer_64k\` 65536 B.

Large sensor payload case: \`camera_8mp_3840x2160_raw12_packed\` 12441600 B.

## Ratio Table

Use \`bench-data/criterion-compare-bencher.txt\` and \`criterion-html/\` for exported measurements and plots.

## Caveats

The core matrix uses static allocation of 128 KiB. The camera matrix uses static allocation of 16 MiB. Payload-contract v1 is Publish-only and does not replace the existing RawBytes transport-boundary matrix. Stale iceoryx2 shared-memory services or samples from interrupted prior runs can affect local smoke runs; rerun after host cleanup if service creation reports stale-state conflicts.
SUMMARY
}

export_results() {
    local report_dir="$TRANSPORT_BENCH_REPORT_DIR"
    local bench_data_dir="$report_dir/bench-data"
    local report_path="$bench_data_dir/criterion-compare-bencher.txt"
    mkdir -p "$bench_data_dir"
    run_selected_profiles --baseline "$BASELINE_NAME" --output-format bencher | tee "$report_path"
    rm -rf "$report_dir/criterion-html"
    mkdir -p "$report_dir/criterion-html"
    if [[ -d target/criterion ]]; then
        cp -a target/criterion/. "$report_dir/criterion-html/"
    fi
    if [[ ! -f "$report_dir/guardrail.json" ]]; then
        cat >"$report_dir/guardrail.json" <<JSON
{"status":"unavailable","reason":"criterion-guardrail utility is not available in this standalone repository"}
JSON
    fi
    write_summary "$report_dir"
}

if [[ $# -lt 1 ]]; then
    usage
    exit 1
fi

subcommand="$1"
shift

case "$subcommand" in
    baseline)
        run_selected_profiles --save-baseline "$BASELINE_NAME"
        ;;
    candidate)
        if [[ $# -ne 1 ]]; then
            usage
            exit 1
        fi
        run_selected_profiles --save-baseline "$1"
        ;;
    guardrail)
        if [[ $# -ne 2 ]]; then
            usage
            exit 1
        fi
        mkdir -p "$(dirname "$2")"
        cat >"$2" <<JSON
{"status":"unavailable","candidate":"$1","reason":"criterion-guardrail utility is not available in this standalone repository"}
JSON
        ;;
    export)
        export_results
        ;;
    *)
        usage
        exit 1
        ;;
esac
