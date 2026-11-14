# iceoryx2 on the `up-rust-core` transport boundary

The additive crate is [up-transport-iceoryx2-core](../core-adapter/README.md).
It directly implements `up_rust_core::transport::{BindingSetup, LendingRuntime}`;
the sibling classic crate retains its existing SDK dependency and API.

```rust
use up_rust_core::{memory::StartupMemory, platform::PlatformInit,
    transport::{StartupError, TransportSetup}};
use up_transport_iceoryx2_core::{Iceoryx2Config, Iceoryx2Peers, Iceoryx2Setup};

fn prepare(memory: &StartupMemory, platform: &'static dyn PlatformInit)
    -> Result<TransportSetup, StartupError>
{
    Iceoryx2Setup::new(Iceoryx2Config::default(), Iceoryx2Peers::default())
        .into_transport_on(memory, platform)
}
```

Setup creates native nodes/services/ports and reserves routes. Runtime lends
exclusive native transmit storage, consumes initialized frames on publication,
and returns a receive owner retaining the native sample. The middleware queue
remains the unread route queue. Every raw receive attempt consumes the caller's
core `ReceiveBudget`; native receive errors latch a route fault.

Native route/node resources are retained for the process lifetime, including
when a direct runtime handle is dropped. Header/layout checks preserve metadata
and payload extents, absent/present-empty semantics, and initialized padding.
Setup sizing checks precede native unchecked arithmetic and use the opened
service's actual type details and capacity getters.

Independent review identified the required checked-sizing repair (R1), concurrent
Wait deadline reset (R2), the selected overflowing-queue B1 race (R3), and evidence
overstatements (R4). The R1/R4 correction received focused review; its final
packed-index guard matches the selected native representation. Stable 1.98.1 and
MSRV 1.88 checks and seven sizing regressions passed on each. R2/R3 remain
runtime acceptance blockers. The old peer-change permit is not authorization to
violate the product's no-heap/free/panic constraint. Aggregate queue depth, actual
arrival accounting, fault containment, physical page reservation and complete
lifecycle behavior remain unqualified. This POSIX implementation also retains
64-bit atomics and has no MCU claim.

The core-adapter README carries the current dependency and host validation status.
No native, middleware, target, or ASIL qualification is transferred from prior
snapshots to this branch.
