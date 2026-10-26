# up-transport-iceoryx2-rust
Rust uTransport implementation for iceoryx2

Native selected-wire unregister uses bounded callback quiescence (five seconds by
default), with re-entrant self-unregister handling and ownership-safe timeout.
See the [listener lifecycle guide](docs/listener-lifecycle.md) for configuration,
`QuiesceTimedOut`, retained loans and the separate classic lifecycle.

## Transport APIs

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

The [public API and placement guide](docs/api-contract.md) explains header/layout
fields and mismatch diagnostics. Public documentation uses `missing_docs = "deny"`.
`UProtocolHeader::reserved` explicitly occupies bytes1–7 and must be zero. Header
writers initialize it; receivers reject nonzero values. Raw producers initialize
the new field while the40-byte, alignment8 ABI and physical major version remain
unchanged. The guide lists every offset and a checked construction example.

`UTransportIceoryx2` preserves the classic protobuf-serialized `UTransport`
entry point. `Iceoryx2PubSub` is the real shared-memory selected-wire core. Wrap
it with `with_selected_wire` to compose a wire selected by the application.

The selected-wire core stores encoded frame metadata in the sample prefix and
uses the iceoryx2 user header only for physical payload placement. Source URI
provenance is carried as immutable iceoryx2 service attributes and checked
before a subscriber attaches. Safe receive always runs selected-wire decoding
and filter validation before exposing a frame.

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

Real IPC tests default to `target/r19-iceoryx2-runtime`. Set
`UP_ICEORYX2_TEST_ROOT` to a shorter persistent path when validating from a
deep package extraction directory.

## Feature-Gated Owned Support

`Iceoryx2OwnedCore` is available only with `--features benchmark-owned`. It is a
disabled-by-default owned-frame support path for measurements and selected-wire
owned tests; it is not zero-copy evidence.
