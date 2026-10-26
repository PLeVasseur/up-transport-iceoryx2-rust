# Prepared native routes and registrations

`Iceoryx2PreparedCore::new` creates a closed catalog of exact source/destination
pairs. Each `Iceoryx2RouteReservation` declares maximum encoded metadata, payload
length and alignment. Native pools use static allocation and the configured
slice capacity from creation; setup rejects undersized storage without enlarging
it. Route and registration tables are allocated once with caller-selected limits.
Exhaustion is explicit. The classic `UTransportIceoryx2` entry point remains
separate; dynamically discovering owned benchmark controls are allocating paths.

Native URI-to-service addressing is unchanged: requests address a method,
responses and notifications address a source/destination pair, and publications
address their source. Request services can have multiple clients. Physical
delivery is a hint; the SDK's in-place header admission and decoded filters
remain authoritative. `sink=None` matches sinkless catalog entries only.

Prepared and dynamically discovered owned paths use compatible immutable native
service attributes. Publications, notifications and responses retain their exact
source discovery fields. A request service is shared by clients addressing one
method; `up.request.shared=1` with `up.transport=uprotocol` marks that physical
candidate without making the first caller the service's source authority. Dynamic
discovery may therefore consider it for multiple source filters. The SDK still
checks actual carried source, sink, kind and selected-wire identity before delivery.
These setup attributes neither alter service names/frame bytes nor authenticate
a publisher. They do not require additional native slots, ports or RX capacity.
For shared requests, discovery also matches the configured sink against the method
in the unchanged native service name. A reverse response listener therefore cannot
satisfy a request publisher's subscriber-readiness count. Source-only service
inspection omits shared requests because they do not have one immutable source.
Owned broad-listener registration subscribes to already offered matching services
before reporting success. Later discovery remains dynamic. This closes the
first-sample race with prepared synchronous publishers, which do not invoke the
allocating control's same-process late-discovery registry when sending.

Reserve every possible RPC response destination before starting admission.
The SDK server's first request from each client still creates its logical reply
cache entry in the measured window. Resolving that entry uses an already created
native publisher; it must not create a service, descriptor or native pool there.
An unknown physical destination is a setup error, distinct from exhaustion of
the SDK's finite client cache. Native registration creates one subscriber for
each distinct matching service, never a one-port logical-listener multiplexer.

After registration, call `wait_ready` as setup to connect native ports and apply
the configured peer minimum. Warm the actual runtime/application/delivery
threads with documented traffic before an allocation observation. Do not warm
away SDK RPC first-client cache admission. Success, rejection, cancellation,
tokens, receive and RPC must be counted with an all-thread allocator; calls and
allocated bytes are separate observations and observer costs are not subtracted.
The harness bounds scheduler warm-up and identifies each actual runtime worker
after a same-thread yield. It does not block scheduler tasks on a native barrier,
which could strand the second warm-up task on the first worker's queue.

## Event wait set and ordered delivery

Each native delivery thread creates and attaches its event descriptor wait set
before reporting readiness. The wait set uses the pinned middleware's
`iceoryx2_bb_posix::FileDescriptorSet`, with native listener owners retained for
all attachments. Its fixed descriptor set and direct readiness callbacks avoid
the per-wake event/deadline `Vec` allocations in iceoryx2 0.9.2's higher-level
`WaitSet`. Descriptor numbers are checked against its actual `FD_SETSIZE` before
attachment. A bounded 10 ms timeout also checks data if a notification is lost.

The pump moves one native sample directly into the synchronous SDK `FrameSink`.
There is no pump staging queue or task per sample. A blocking inbox push holds
the sole transient frame; closing admission releases blocked producers before
detach joins the pump. Each registration has its own native subscriber, and its
SDK generic handler worker applies typed/header admission and FIFO processing.
No registry lock is held across application work or detachment. Retained frames
occupy native borrowed-sample/subscriber capacity until their owners release them.

`Iceoryx2PreparedCore::stats` reports native publication, notification and receive
outcomes. Invalid placement/address alignment is a physical rejection; receive
errors and exhaustion of retained borrowed-sample credit have separate counters.
Those observations do not become application inbox-drop allowances. SDK admission
and registration counters separately describe typed/filter/inbox outcomes.

## Physical storage and wiping

The existing 40-byte, alignment-8 `UProtocolHeader` is unchanged, including zero
reserved bytes. Metadata and payload are disjoint checked regions in a single
native loan. The binding initializes native framing, alignment gaps and trailing
bytes; the SDK's checked initializer proves the visible regions. Receive retains
the original sample and subscriber, drops the sample first, and never copies or
coalesces payload storage. Absent and present-empty payloads are distinguished by
the encoded metadata, not by physical length alone.
The frame also retains the native node lifecycle handle until the sample and
subscriber have released their tags, avoiding premature node cleanup after the
application has dropped its endpoint.

Dropping an unpublished prepared loan wipes its owned metadata region. Once
published, iceoryx2 has no last-reader/reclaim callback at this binding boundary.
Token bytes can remain in the native segment until slot reuse, including after
a failed physical publish consumes the native loan. Neither detach nor a
quiescence timeout authorizes wiping another reader's retained sample. Service
permissions remain the storage access boundary. SDK-owned token buffers have
their separate replacement/drop wipe contract; native wipe limitations do not
exclude token operations from allocation measurement.

See [capacity](capacity.md) for native slot/borrow/buffer limits and Linux
hole-only backing preallocation, and [listener lifecycle](listener-lifecycle.md)
for independently retained subscriber ownership.
