# iceoryx2 on the `up-rust-core` transport boundary

Status: source design on `codex/up-rust-core-adapter`; no build or native claim.

The intended layering is the same as LoLa:

```text
up-rust-core::TransportSetup
    │
    ├─ Iceoryx2Setup::recover_predecessor()
    ├─ Iceoryx2Setup::prepare_route()
    └─ TransportSetup::enter_runtime()
          │
          ▼
    Iceoryx2Running: LendingRuntime
          │
          ├─ loan()      → iceoryx2 publisher loan
          ├─ publish()   → iceoryx2 sample send
          └─ receive()   → iceoryx2 subscriber queue
```

The application does not use an iceoryx2-specific send/receive API:

```rust
let setup = Iceoryx2Setup::new(config, expected_peers)
    .with_limits(limits);
let transport = TransportSetup::lending(setup)?;
let runtime = transport.enter_runtime(memory, clock)?;
// Generic uProtocol roles now use the returned TransportRuntime.
```

The candidate two-mode source already has the correct conceptual split in the
SDK integration snapshot: `Iceoryx2Setup` implements `BindingSetup`, while
`Iceoryx2Running` implements `LendingRuntime`. The fork branch must bring that
source across with the exact current core dependency and preserve the existing
iceoryx2 service/QoS/loan semantics.

## What must remain explicit

- iceoryx2 owns the sample storage and its queue remains the route queue;
  the adapter must not add a second unread queue.
- setup creates nodes, services, publishers, subscribers, listeners and peer
  tables; runtime methods must not perform setup work.
- peer-change allocation behavior is an explicit iceoryx2 permit. It is not
  hidden behind a generic Rust claim of “no allocation.”
- native sample owners remain alive through `Iceoryx2Rx` until the SDK drops the
  frame; the `Frame` implementation must preserve that lease.
- peer identity/incarnation and stale-peer handling remain part of the core
  contract, not an optional transport diagnostic.
- `AtomicU64` diagnostics must be isolated from any 32-bit runtime image.

## Next branch work

1. Select the exact up-rust-core revision carrying `BindingSetup` and
   `LendingRuntime`.
2. Port the prepared `two_mode.rs` candidate into this fork without changing
   the existing classic APIs.
3. Reconcile the supporting modules and feature gates.
4. Run independent unsafe-source review, then stable/MSRV host compilation.
5. Push the reviewable feature branch to `PLeVasseur/up-transport-iceoryx2-rust`
   only after the source and host result are coherent.

No target, native, middleware, or ASIL qualification is claimed here.
