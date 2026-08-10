# up-transport-iceoryx2-rust
Rust uTransport implementation for iceoryx2

## Transport APIs

`UTransportIceoryx2` preserves the classic protobuf-serialized `UTransport`
entry point. `Iceoryx2PubSub` is the real shared-memory selected-wire core. Wrap
it with `with_selected_wire` to compose a wire selected by the application.

The selected-wire core stores encoded frame metadata in the sample prefix and
uses the iceoryx2 user header only for physical payload placement. Source URI
provenance is carried as immutable iceoryx2 service attributes and checked
before a subscriber attaches. Safe receive always runs selected-wire decoding
and filter validation before exposing a frame.

Real IPC tests default to `target/r19-iceoryx2-runtime`. Set
`UP_ICEORYX2_TEST_ROOT` to a shorter persistent path when validating from a
deep package extraction directory.

## Feature-Gated Owned Support

`Iceoryx2OwnedCore` is available only with `--features benchmark-owned`. It is a
disabled-by-default owned-frame support path for measurements and selected-wire
owned tests; it is not zero-copy evidence.
