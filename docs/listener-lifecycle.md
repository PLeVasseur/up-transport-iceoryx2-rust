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
