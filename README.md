# up-transport-iceoryx2-rust
Rust uTransport implementation for iceoryx2

Native selected-wire unregister uses bounded callback quiescence (five seconds by
default), with re-entrant self-unregister handling and ownership-safe timeout.
See the [listener lifecycle guide](docs/listener-lifecycle.md) for configuration,
`QuiesceTimedOut`, retained loans and the separate classic lifecycle.

## Transport APIs

`Iceoryx2PreparedCore` implements the SDK's `PreparedTransportCore`. Create its
finite physical catalog with `Iceoryx2RouteReservation`, static native slices and
explicit route/registration capacities. SDK routes resolve those existing
publishers; synchronous loan/publish writes metadata and payload in native slots.
Attach creates a native event wait set and independent subscribers; the SDK owns
the generic async handler worker. See the [prepared-core guide](docs/prepared.md)
for setup, first-contact RPC, allocation measurement and token-wipe limits.
Its native outcome counters keep physical rejection/error/borrowed-credit
observations distinct from SDK admission and application inbox drops.

`Iceoryx2PubSub` supplies the allocating owned benchmark control's native mechanics.
Its dynamic discovery and collection work is separate from the prepared path.

Each listener registration gets a native subscriber per matching service.
iceoryx2 performs shared-chunk fan-out and each callback owns an independent native
sample. The SDK checks full source/sink filters. See
[listener lifecycle](docs/listener-lifecycle.md) for native port and lease ownership.
Receive leases retain the actual native subscriber until the sample is released,
protecting them against publisher pool reuse after unregister or endpoint drop.

Fixed-budget deployments can opt into exact native service limits and hard static
first-loan capacity through `Iceoryx2PubSubConfig::require_exact_service_limits`.
Use `Iceoryx2PubSub::try_with_config` for typed setup errors. See the
[capacity guide](docs/capacity.md) for participant compatibility, slot accounting
and the distinction between physical pools and application receive queues.

Linux exact/static publishers may additionally set disabled-default
`publisher_prewarm` to materialize the existing backing object's pages before
loan admission, preserving bytes and native slot/TX limits. See
[backing-page preallocation](docs/capacity.md#linux-static-backing-page-preallocation).

The [public API and placement guide](docs/api-contract.md) explains header/layout
fields and mismatch diagnostics. Public documentation uses `missing_docs = "deny"`.
`UProtocolHeader::reserved` explicitly occupies bytes 1–7 and must be zero. Header
writers initialize it; receivers reject nonzero values. Raw producers initialize
the new field while the 40-byte, alignment 8 ABI and physical major version remain
unchanged. The guide lists every offset and a checked construction example.

`UTransportIceoryx2` preserves the classic protobuf-serialized `UTransport`
entry point. `Iceoryx2PreparedCore` is the setup-reserved shared-memory core for
the SDK's concrete prepared routes and generic registrations. The feature-gated
`BenchmarkOwnedIceoryx2Core::with_selected_wire` composes the allocating owned
control over `Iceoryx2PubSub` mechanics.

Both paths store encoded metadata in the sample prefix and use the iceoryx2 user
header for physical placement. Prepared physical addressing follows the finite
catalog; the SDK validates carried metadata, selected-wire identity and decoded
source/sink filters before exposing a checked public lease. Dynamic owned
controls use compatible immutable native service attributes for discovery.
Prepared services carry those same fields. Shared method-addressed request services
use a shared-request marker instead of fixing one client as their source; decoded
SDK source/sink checks remain authoritative. See [prepared discovery](docs/prepared.md).
Broad owned registration completes subscriptions to already offered services
before reporting readiness, so prepared senders need no per-message discovery work.

Receive samples retain their backing subscriber state after listener unregister
and transport destruction. Pending poller snapshots are invalidated when their
registration is removed; an already-entered callback may finish. Real IPC tests
hold the same payload address across teardown and exercise the snapshot race.

Broad subscriptions discover dynamically created services. For finite senders,
`Iceoryx2PubSubConfig::with_publisher_readiness(minimum, timeout)` optionally waits
for an actual subscriber count before returning a TX loan, after the publisher's
real data segment exists. Missing peers return a bounded `DeadlineExceeded`;
there is no fixed stabilization delay or duplicate application send. The default
minimum is zero, preserving ordinary publish-without-a-peer semantics and existing
history policy. This is transport discovery, not an application acknowledgement.

`Iceoryx2PubSubConfig::with_namespace(root_path, prefix)` selects a checked native
namespace for one instance without changing process-wide environment variables.
A bridge must use distinct native prefixes for independent ingress/egress buses;
different uProtocol authorities alone do not provide that isolation. Multiple
instances in one process may use different namespaces and carry identical
logical source/sink URIs while remaining physically separate.

Set `UP_ICEORYX2_TEST_ROOT` to a short, isolated absolute path for native tests
and the Criterion benchmark. Native Unix event sockets include the service
directory, namespace prefix, native ID and suffix; a deep package path can exceed
the operating system's socket-path limit. Set `UP_ICEORYX2_FINCORE_BIN` to the
actual util-linux `fincore` executable for the Linux backing-page tests.

## Feature-Gated Owned Support

`Iceoryx2OwnedCore` is available only with `--features benchmark-owned`. It is a
disabled-by-default owned-frame support path for measurements and selected-wire
owned tests; it is not zero-copy evidence.

Receiver-only prepared reservations avoid unused native publisher pools. See [Receiver-only physical reservations](docs/prepared.md#receiver-only-physical-reservations).
