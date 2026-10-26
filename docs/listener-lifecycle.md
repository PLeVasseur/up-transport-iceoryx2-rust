# Native listener admission and quiescence

`Iceoryx2PubSub` uses one SDK admission gate per selected-wire registration.
Unregister removes the registration and closes admission under the registry lock,
then releases that lock before waiting for entered callbacks. The configured
`Iceoryx2PubSubConfig::listener_quiesce_timeout` defaults to five seconds and is
measured from admission close. Zero/unrepresentable intervals are rejected.

Normal completion waits for entered work. A callback unregistering its own
registration skips the wait; registration-keyed context prevents waiting for itself
without treating unrelated callbacks as re-entrant. Current user callbacks execute
through SDK async dispatch on the worker, not through an invented synchronous guard.

Expiry returns `DeadlineExceeded` with `QuiesceTimedOut` in the diagnostic. Admission
stays closed; an entered callback may finish later. Callback and receive-lease
ownership retain the native sample/subscriber state after unregister or transport
handle destruction. Quiescence does not revoke externally retained loans or drain
application queues. Classic `UTransportIceoryx2` has its separate existing lifecycle.

The real IPC tests cover normal pending completion, self-unregister and a timed-out
callback retaining its sample. Freeze the timeout in operational cards before trials;
do not tune it after a failure.

## Logical listeners and physical ports

Each registration owns a native subscriber per matching service; wildcard filters
can attach to multiple services. iceoryx2 sends the same published chunk to those
subscribers and manages their independent native sample references. No local
sample-sharing layer, unsafe Sync assertion or Bytes ownership box is introduced.
Every registration retains its SDK admission and final source/sink filter gate;
sinkless remains distinct from wildcard sinks. Provision native subscriber limits
for the actual registrations, including internal directed and publication filters.

Each receive lease also retains the actual native subscriber, in both pull and
listener paths. The sample is dropped before the subscriber keepalive. This prevents
native port deregistration and publisher disconnect reclamation while application
code still holds a sample, including after the transport handle is destroyed.
Retaining the sample's mapping alone does not establish this pool-reuse guarantee.

Unregister still closes callback admission and performs bounded quiescence; it
does not wait for application-held leases or revoke their storage. A physical
subscriber slot can therefore remain occupied until its final retained lease is
dropped. This keeps native resource accounting honest: another registration can
fail its configured subscriber ceiling while an older lease survives. Final lease
release returns the native sample before releasing the slot. There is no new
per-message sharing allocation; the keepalive clones the already allocated native
subscriber Arc. Existing setup, collection and metadata allocations are separate.

Real IPC regressions keep a sample through unregister/endpoint destruction while
continuing publication through a small pool, then verify unchanged contents,
cross-thread ownership and final-slot release. Separate tests verify native
fan-out with distinct subscriber handles and shared-memory backing identity.
