#!/usr/bin/env bash
#
# Copyright (c) 2026 Contributors to the Eclipse Foundation
#
# See the NOTICE file(s) distributed with this work for additional
# information regarding copyright ownership.
#
# This program and the accompanying materials are made available under the
# terms of the Apache License Version 2.0 which is available at
# https://www.apache.org/licenses/LICENSE-2.0
#
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

readonly TRANSPORT_NAME="iceoryx2"
readonly DEFAULT_REPORT_DIR="target/transport-perf/$TRANSPORT_NAME"
readonly DEFAULT_CRITERION_ARGS="--output-format bencher --sample-size 10 --warm-up-time 1 --measurement-time 2 --noise-threshold 0.05"
readonly BASELINE_NAME="payload_contract_representative_v1"

TRANSPORT_BENCH_SUITE="${TRANSPORT_BENCH_SUITE:-payload-contract}"
TRANSPORT_BENCH_PROFILE="${TRANSPORT_BENCH_PROFILE:-all}"
TRANSPORT_BENCH_REPORT_DIR="${TRANSPORT_BENCH_REPORT_DIR:-$DEFAULT_REPORT_DIR}"
CRITERION_ARGS="${CRITERION_ARGS:-$DEFAULT_CRITERION_ARGS}"
BENCH_PIN_PREFIX="${BENCH_PIN_PREFIX:-}"

usage() {
    cat <<'USAGE'
Usage:
  scripts/bench_transport_criterion.sh baseline
  scripts/bench_transport_criterion.sh candidate <phase_candidate>
  scripts/bench_transport_criterion.sh guardrail <phase_candidate> <report_path>
  scripts/bench_transport_criterion.sh export

Environment:
  TRANSPORT_BENCH_REPORT_DIR  Report output directory. Default: target/transport-perf/iceoryx2
  TRANSPORT_BENCH_SUITE       raw, payload-contract, or all. Default: payload-contract
  TRANSPORT_BENCH_PROFILE     core, camera, or all. Default: all
  CRITERION_ARGS              Criterion args. Default matches representative-v1.
  BENCH_PIN_PREFIX            Optional command prefix for CPU pinning, etc.
USAGE
}

validate_suite() {
    case "$TRANSPORT_BENCH_SUITE" in
        raw | payload-contract | all) ;;
        *)
            printf 'TRANSPORT_BENCH_SUITE must be one of raw, payload-contract, all\n' >&2
            exit 2
            ;;
    esac
}

validate_profile() {
    case "$TRANSPORT_BENCH_PROFILE" in
        core | camera | all) ;;
        *)
            printf 'TRANSPORT_BENCH_PROFILE must be one of core, camera, all\n' >&2
            exit 2
            ;;
    esac
}

cargo_features() {
    local path="$1"

    validate_suite
    case "$path:$TRANSPORT_BENCH_SUITE" in
        zero-copy:raw)
            printf '%s\n' "zero-copy"
            ;;
        zero-copy:payload-contract | zero-copy:all)
            printf '%s\n' "zero-copy,payload-contract-large-benchmarks"
            ;;
        owned:raw)
            printf '%s\n' "benchmark-owned"
            ;;
        owned:payload-contract | owned:all)
            printf '%s\n' "benchmark-owned,payload-contract-large-benchmarks"
            ;;
        *)
            printf 'unknown benchmark path: %s\n' "$path" >&2
            exit 2
            ;;
    esac
}

run_cargo_bench() {
    local path="$1"
    shift

    validate_profile

    local features
    features="$(cargo_features "$path")"

    read -r -a criterion_parts <<<"$CRITERION_ARGS"
    if [[ -n "$BENCH_PIN_PREFIX" ]]; then
        read -r -a pin_parts <<<"$BENCH_PIN_PREFIX"
        TRANSPORT_BENCH_SUITE="$TRANSPORT_BENCH_SUITE" \
            TRANSPORT_BENCH_PROFILE="$TRANSPORT_BENCH_PROFILE" \
            "${pin_parts[@]}" cargo bench --features "$features" --bench transport_criterion -- "${criterion_parts[@]}" "$@"
    else
        TRANSPORT_BENCH_SUITE="$TRANSPORT_BENCH_SUITE" \
            TRANSPORT_BENCH_PROFILE="$TRANSPORT_BENCH_PROFILE" \
            cargo bench --features "$features" --bench transport_criterion -- "${criterion_parts[@]}" "$@"
    fi
}

run_representative_benches() {
    run_cargo_bench zero-copy "$@"
    run_cargo_bench owned "$@"
}

git_value() {
    git "$@" 2>/dev/null || printf '%s\n' "unknown"
}

write_summary() {
    local report_dir="$1"
    local zero_copy_raw_output="$2"
    local owned_raw_output="$3"
    local summary="$report_dir/README.md"
    local zero_copy_features
    local owned_features
    zero_copy_features="$(cargo_features zero-copy)"
    owned_features="$(cargo_features owned)"

    cat >"$summary" <<SUMMARY
# iceoryx2 Payload-Contract Representative Benchmarks

## Commands

Zero-copy:

\`\`\`bash
TRANSPORT_BENCH_SUITE=$TRANSPORT_BENCH_SUITE TRANSPORT_BENCH_PROFILE=$TRANSPORT_BENCH_PROFILE cargo bench --features $zero_copy_features --bench transport_criterion -- $CRITERION_ARGS
\`\`\`

Owned:

\`\`\`bash
TRANSPORT_BENCH_SUITE=$TRANSPORT_BENCH_SUITE TRANSPORT_BENCH_PROFILE=$TRANSPORT_BENCH_PROFILE cargo bench --features $owned_features --bench transport_criterion -- $CRITERION_ARGS
\`\`\`

## Environment

- Transport: iceoryx2
- Git head: \`$(git_value rev-parse HEAD)\`
- Git branch: \`$(git_value branch --show-current)\`
- Rust: \`$(rustc --version)\`
- Cargo: \`$(cargo --version)\`
- OS: \`$(uname -srmo)\`
- Suite: \`$TRANSPORT_BENCH_SUITE\`
- Profile: \`$TRANSPORT_BENCH_PROFILE\`
- Zero-copy features: \`$zero_copy_features\`
- Owned features: \`$owned_features\`
- Criterion args: \`$CRITERION_ARGS\`
- Pinning prefix: \`${BENCH_PIN_PREFIX:-none}\`
- Zero-copy raw output: \`$zero_copy_raw_output\`
- Owned raw output: \`$owned_raw_output\`

## Required Labels

- \`stable_zc_nozero_full\`
- \`protobuf_owned_full\`
- \`stable_owned_bytes_full\`

## Notes

This script is the Phase 07C2 authority wrapper for the 07C1 representative-v1 command shapes. The owned path uses the benchmark-only copying \`BenchmarkOwnedIceoryx2PubSub\` wrapper behind \`benchmark-owned\`; it is not direct true zero-copy. Artifacts are written only under the caller-selected report directory.
SUMMARY
}

export_results() {
    local report_dir="$TRANSPORT_BENCH_REPORT_DIR"
    local bench_data_dir="$report_dir/bench-data"
    local zero_copy_raw_output="$bench_data_dir/transport-criterion-zero-copy-bencher.txt"
    local owned_raw_output="$bench_data_dir/transport-criterion-owned-bencher.txt"

    mkdir -p "$bench_data_dir"
    run_cargo_bench zero-copy | tee "$zero_copy_raw_output"
    run_cargo_bench owned | tee "$owned_raw_output"

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

    write_summary "$report_dir" "$zero_copy_raw_output" "$owned_raw_output"
}

if [[ $# -lt 1 ]]; then
    usage
    exit 1
fi

subcommand="$1"
shift

case "$subcommand" in
    baseline)
        run_cargo_bench zero-copy --save-baseline "${BASELINE_NAME}_zero_copy"
        run_cargo_bench owned --save-baseline "${BASELINE_NAME}_owned"
        ;;
    candidate)
        if [[ $# -ne 1 ]]; then
            usage
            exit 1
        fi
        run_cargo_bench zero-copy --save-baseline "$1_zero_copy"
        run_cargo_bench owned --save-baseline "$1_owned"
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
        exit 2
        ;;
esac
