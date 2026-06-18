#!/usr/bin/env bash
#
# Copyright (c) 2026 Contributors to the Eclipse Foundation
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
CARGO_BIN="${CARGO_BIN:-cargo}"

usage() {
    cat <<'USAGE'
Usage:
  scripts/bench_transport_criterion.sh baseline
  scripts/bench_transport_criterion.sh candidate <phase_candidate>
  scripts/bench_transport_criterion.sh guardrail <phase_candidate> <report_path>
  scripts/bench_transport_criterion.sh export

Environment:
  TRANSPORT_BENCH_REPORT_DIR  Report output directory. Default: target/transport-perf/iceoryx2
  TRANSPORT_BENCH_SUITE       payload-contract. Default: payload-contract
  TRANSPORT_BENCH_PROFILE     core, camera, or all. Default: all
  TRANSPORT_BENCH_PATH        zero-copy or owned. Set by the script for export.
  CRITERION_ARGS              Criterion args. Default matches USR-10B2 C1.
  BENCH_PIN_PREFIX            Optional command prefix for CPU pinning, etc.
  CARGO_BIN                   Cargo command. Default: cargo
USAGE
}

cargo_features() {
    local path="$1"

    case "$path:$TRANSPORT_BENCH_SUITE" in
        zero-copy:payload-contract)
            printf '%s\n' "zero-copy,payload-contract-large-benchmarks"
            ;;
        owned:payload-contract)
            printf '%s\n' "benchmark-owned,payload-contract-large-benchmarks"
            ;;
        *)
            printf 'TRANSPORT_BENCH_SUITE must be payload-contract\n' >&2
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

run_cargo_bench() {
    local path="$1"
    shift

    validate_profile

    local features
    features="$(cargo_features "$path")"

    read -r -a criterion_parts <<<"$CRITERION_ARGS"
    read -r -a cargo_parts <<<"$CARGO_BIN"
    if [[ -n "$BENCH_PIN_PREFIX" ]]; then
        read -r -a pin_parts <<<"$BENCH_PIN_PREFIX"
        TRANSPORT_BENCH_SUITE="$TRANSPORT_BENCH_SUITE" \
            TRANSPORT_BENCH_PROFILE="$TRANSPORT_BENCH_PROFILE" \
            TRANSPORT_BENCH_PATH="$path" \
            "${pin_parts[@]}" "${cargo_parts[@]}" bench --features "$features" --bench transport_criterion -- "${criterion_parts[@]}" "$@"
    else
        TRANSPORT_BENCH_SUITE="$TRANSPORT_BENCH_SUITE" \
            TRANSPORT_BENCH_PROFILE="$TRANSPORT_BENCH_PROFILE" \
            TRANSPORT_BENCH_PATH="$path" \
            "${cargo_parts[@]}" bench --features "$features" --bench transport_criterion -- "${criterion_parts[@]}" "$@"
    fi
}

git_value() {
    git "$@" 2>/dev/null || printf '%s\n' "unknown"
}

copy_criterion_artifacts() {
    local destination="$1"

    rm -rf "$destination"
    mkdir -p "$destination"
    if [[ -d target/criterion ]]; then
        cp -a target/criterion/. "$destination/"
    fi
}

run_export_path() {
    local path="$1"
    local raw_output="$2"
    local criterion_dir="$3"

    rm -rf target/criterion
    run_cargo_bench "$path" | tee "$raw_output"
    copy_criterion_artifacts "$criterion_dir"
}

write_summary() {
    local report_dir="$1"
    local zero_copy_raw_output="$2"
    local owned_raw_output="$3"
    local zero_copy_criterion_dir="$4"
    local owned_criterion_dir="$5"
    local summary="$report_dir/README.md"
    local zero_copy_features
    local owned_features
    zero_copy_features="$(cargo_features zero-copy)"
    owned_features="$(cargo_features owned)"
    read -r -a cargo_parts <<<"$CARGO_BIN"

    cat >"$summary" <<SUMMARY
# iceoryx2 Userializer Payload-Contract Representative Benchmarks

## Commands

Zero-copy:

\`\`\`bash
TRANSPORT_BENCH_SUITE=$TRANSPORT_BENCH_SUITE TRANSPORT_BENCH_PROFILE=$TRANSPORT_BENCH_PROFILE CARGO_BIN="$CARGO_BIN" scripts/bench_transport_criterion.sh export
\`\`\`

Owned comparison blocker artifact:

\`\`\`bash
cat $owned_raw_output
\`\`\`

## Environment

- Transport: iceoryx2
- Phase: USR-10B2
- Git head: \`$(git_value rev-parse HEAD)\`
- Git branch: \`$(git_value branch --show-current)\`
- Default Rust: \`$(rustc --version)\`
- Default Cargo: \`$(cargo --version)\`
- Cargo command: \`$CARGO_BIN\`
- Cargo command version: \`$("${cargo_parts[@]}" --version)\`
- OS: \`$(uname -srmo)\`
- Suite: \`$TRANSPORT_BENCH_SUITE\`
- Profile: \`$TRANSPORT_BENCH_PROFILE\`
- Zero-copy features: \`$zero_copy_features\`
- Owned features: \`$owned_features\`
- Criterion args: \`$CRITERION_ARGS\`
- Pinning prefix: \`${BENCH_PIN_PREFIX:-none}\`
- Zero-copy raw output: \`$zero_copy_raw_output\`
- Owned raw output: \`$owned_raw_output\`
- Zero-copy Criterion artifacts: \`$zero_copy_criterion_dir\`
- Owned Criterion artifacts: \`$owned_criterion_dir\`

## Required Labels

- \`stable_zc_nozero_full\`
- \`protobuf_owned_full\` blocked/support-only for USR-10B2
- \`stable_owned_bytes_full\` blocked/support-only for USR-10B2

## Claim Boundary

This script is the USR-10B2 authority wrapper for representative command shape and artifact separation. Owned comparison rows are blocked/support-only because the current feature-gated owned core records prepared bytes and does not provide comparable loopback receive semantics.
SUMMARY
}

export_results() {
    local report_dir="$TRANSPORT_BENCH_REPORT_DIR"
    local bench_data_dir="$report_dir/bench-data"
    local zero_copy_raw_output="$bench_data_dir/transport-criterion-zero-copy-bencher.txt"
    local owned_raw_output="$bench_data_dir/transport-criterion-owned-bencher.txt"
    local zero_copy_criterion_dir="$report_dir/criterion-html-zero-copy"
    local owned_criterion_dir="$report_dir/criterion-html-owned"

    mkdir -p "$bench_data_dir"
    rm -rf "$report_dir/criterion-html"
    run_export_path zero-copy "$zero_copy_raw_output" "$zero_copy_criterion_dir"
    rm -rf "$owned_criterion_dir"
    mkdir -p "$owned_criterion_dir"
    cat >"$owned_raw_output" <<'OWNED_BLOCKED'
status: blocked/support-only
phase: USR-10B2
reason: Iceoryx2OwnedCore is a feature-gated prepared-frame/logging proof core; send_owned records prepared bytes and receive_owned returns only explicitly injected frames, so owned comparison rows are not comparable loopback benchmark authority in this phase.
OWNED_BLOCKED
    cat >"$report_dir/guardrail.json" <<JSON
{"status":"blocked","phase":"USR-10B2","reason":"aggregate guard comparison is not established by this script alone"}
JSON
    write_summary "$report_dir" "$zero_copy_raw_output" "$owned_raw_output" "$zero_copy_criterion_dir" "$owned_criterion_dir"
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
{"status":"blocked","phase":"USR-10B2","candidate":"$1","reason":"aggregate guard comparison is not established by this script alone"}
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
