# Exact native service capacities

Prepared transport uses `Iceoryx2PreparedCore::new` with a finite physical route
catalog and explicit handle-table capacities. It always requires static allocation
and rejects a reservation larger than the configured slice, including the first
loan. `prepare_route` returns a fixed-size setup error for a missing destination,
oversized layout or exhausted logical table. `loan` reports fixed-size pool/slot
exhaustion. Native registration exhaustion returns `SetupError::TableFull`.
The [prepared guide](prepared.md) documents resource creation and event delivery.

Ordinary `Iceoryx2PubSubConfig` retains the middleware's default open/create and
publisher-growth behavior. For a deployment with a fixed memory budget, set
`require_exact_service_limits = true`, provide the native configuration and use
`Iceoryx2PubSub::try_with_config` for typed setup errors.

The native configuration supplies `max_subscribers`,
`subscriber_max_buffer_size`, `subscriber_max_borrowed_samples`,
`publisher_history_size`, `publisher_max_loaned_samples` and overflow policy.
Exact mode checks nonzero subscriber/buffer/borrow/TX/slice values, readiness
against the subscriber ceiling, and checked slot/minimum-byte arithmetic before
creating the node. `InvalidArgument` and `OutOfRange` distinguish invalid values
from arithmetic overflow. Native node failures return `Internal`.

Before creating a subscriber or publisher port, exact mode compares the opened
service's actual subscriber ceiling, buffer, borrowed-sample, history and overflow
settings against the node's configured values. Any difference returns
`FailedPrecondition`, including an existing service with a *larger* ceiling.
It does not rewrite the service or attach a port to an incompatible deployment.
Every participant must provision compatible settings and an isolated namespace.

With static allocation, `publisher_initial_max_slice_len` is a hard capacity from
the first loan in exact mode. An oversized request returns `ResourceExhausted`
instead of enlarging the first publisher allocation. The slice includes encoded
metadata, alignment slack and payload; compute it using the selected codec and
actual message options. The user header and native management allocations are
additional physical storage. Dynamic strategies retain their configured growth
behavior; exact service ceilings alone do not promise a fixed dynamic byte pool.

The pinned middleware's publisher sample budget is
`max_subscribers * (buffer + borrowed) + history + TX loans`. For example,
one subscriber, buffer 2, borrowed 2, history 0 and TX 1 require five slots per
publisher. This is neither a consumer application-queue limit nor a process RSS
prediction. A second subscriber cannot join a one-subscriber service; diagnostics
must not consume the sole receiver slot. Allocator failure is an error, not an
application backpressure/drop allowance. Existing callback quiescence and
retained-sample ownership contracts remain independent.

One subscriber is a deployment example, not a transport restriction. The native
default remains eight subscribers, and exact mode accepts larger configured
ceilings. Two subscribers with buffer 2, borrowed 2, history 0 and TX 1 need nine publisher
slots; both can retain independent loans to a publication. Different virtual
mapping addresses do not imply payload copies. Unregistering or destroying one
subscriber never revokes the other's retained sample. Native tests cover fan-out,
independent retained loans and rejection only beyond the configured ceiling.

Each listener registration creates a native subscriber for every matching service;
budget overlapping directed and sinkless filters separately. Wildcards can attach
to several services. Native fan-out delivers independently retained sample handles
backed by the same published chunk, without a custom per-message ownership box.
Application-held receive leases keep their native subscriber ports alive after
logical unregister or endpoint destruction. Budget those retained ports; a newly
requested subscriber can be rejected until the final old lease is released.

## Linux static backing-page preallocation

`publisher_prewarm = true` is an optional exact/static publisher setting. Its
disabled default preserves ordinary sparse backing. Before returning the first
loan, the binding identifies that live native loan's actual writable POSIX SHM
mapping, opens the same backing object, verifies device/inode/extent, and requests
whole-file backing allocation through the safe rustix filesystem API. It then
queries kernel-reported hole/data extents and materializes only zero-valued holes
with bounded zero-buffer file writes. Existing data extents are never rewritten.
This preserves logical contents while turning reservation-only pages into actual
data pages. It is construction-time backing access, not per-message receive or
forwarding behavior; no raw sample reference or mapping is formed. The setup
does not resize the object, overwrite sample/header bytes, change native slots or
TX limits, create another publisher/port, or introduce a payload-copy fallback.

Only Linux exact/static geometry is supported. Other platforms return
`Unimplemented`; nonstatic/nonexact requests return `InvalidArgument`. Descriptor
or native setup failures return `Internal`; backing allocation failure returns
`ResourceExhausted`, with no admitted publisher or smaller-pool fallback. The
descriptor is released after setup; original publisher/sample mappings retain
their existing ownership. Native callbacks, subscriber retention and quiescence
remain independent.

`NATIVE_POOL_PREALLOCATED` reports the actual backing descriptor identity and byte
extent. That allocation receipt and a VMA's RSS are not the same measurement:
an existing alias can retain sparse page-table entries even after the backing
object's pages are materialized. Native tests independently query whole-file
residency with util-linux `fincore` and verify existing bytes and TX bounds.
