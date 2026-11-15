# iceoryx2 core adapter

This experimental crate implements `BindingSetup` and `LendingRuntime` directly
from `up-rust-core`. It preserves the sibling classic crate and its SDK dependency.
The core source is pinned to `PLeVasseur/up-rust` commit
`dc7f22da2bf90d98d94d468621df26caadf853ca` on the experimental
`codex/allocator-mcu-spike` branch. At this pin, `cargo check --locked --lib`
passed with stable 1.98.1, Rust 1.88.0 and Rust 1.100.0 on an aarch64 macOS host,
with the same single dead-code warning for three private layout helpers; no test,
example or native service ran.

On macOS, Linux and Windows x86-64 MSVC this SDK revision selects its host
execution model automatically. For `aarch64-apple-darwin`,
`aarch64-unknown-linux-gnu` and `x86_64-unknown-linux-gnu` it also builds a small
deadline C shim natively with `clang` and `ar` (no cross-compilation). For
`x86_64-pc-windows-msvc` it builds that shim with `clang` for the explicit MSVC
target and archives it with `llvm-lib`; `CC`/`AR` may name them, and
cross-compilation is supported. Both x86-64 targets must build for a CX16 minimum
ISA, for example `-C target-feature=+cmpxchg16b`; the Windows GNU target is not
supported. The SDK's Windows support is limited to its private deadline
primitive, and no Windows build of this crate has been checked. See the
[SDK core README](https://github.com/PLeVasseur/up-rust/blob/dc7f22da2bf90d98d94d468621df26caadf853ca/core/README.md).

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

Callers supply startup memory and the platform to the core. Route declarations
reserve each native service/port; SDK role builders then use the same core API.
Runtime methods loan native storage, publish an `InitializedTx`, and retain
native receive samples. The native queue remains the unread route queue.

Native resources are deliberately reserved for the process lifetime. Dropping a
runtime handle cannot unregister a subscriber behind an outstanding sample.
Receive attempts share the core `ReceiveBudget`; native receive failures latch a
route fault. Transmit layout/header recording and truncation validate bounds.

This branch is an adapter port, not native or ASIL acceptance. Independent unsafe
source review led to checked sizing and packed native-index admission fixes.
Retained middleware deviations
include peer-change heap activity, blocking/cleanup behavior, aggregate depth
with multiple publishers, arrival accounting and peer fault containment. The
old runtime permit does not override the product's no-heap/free/panic rule.
The binding uses 64-bit atomics and POSIX services; it does not claim MCU support.
Physical backing-page preallocation is not established by the setup's first loan.

The lockfile records the actual resolution: iceoryx2, bb-posix and bb-system-types
are 0.9.2, while some transitive crates resolve to 0.9.3. Existing native receipts
are not transferred to that graph. No native tests or middleware installation
have been run for this crate.

The following November 14 record was committed with the earlier SDK pin
`20ed116605551fa96fa5bdf8f7ded57e75eae13f`; it is not evidence for the current pin.

Host validation on macOS/aarch64, November 14, 2026:

| Check | Rust 1.98.1 (stable) | Rust 1.88.0 (MSRV) |
| --- | --- | --- |
| `cargo check --locked` | Passed | Passed |
| `cargo test --locked --lib sizing` | 7 passed | 7 passed |

The test filter excludes 26 existing tests and runs no native service. It covers
frame limits, signed/aligned native layouts, sample/segment overflow, the actual
service-capacity model, the native packed-index boundary and safe rejection before
node creation. Both checks retain one dead-code warning for three private imported
layout helpers. The first compile failure (missing constructor-trait import and
refusal formatting) was retained; its two integration fixes are in this source.
Runtime behavior and middleware soundness are outside these host results.

Independent source review identified unchecked native sizing (R1); the correction
checks frame, native layout, actual service sample counts and segment arithmetic,
including the selected pool's strict count limit of fewer than 16,777,215 samples.
Wait concurrency can reset another send's shared deadline (R2); its declared bound
is not established. The resolved overflowing index queue retains known B1 (R3).
These remain runtime acceptance blockers, independently of host compilation.
