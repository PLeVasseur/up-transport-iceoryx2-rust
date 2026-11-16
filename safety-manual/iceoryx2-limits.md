# A deployment on iceoryx2 accepts that binding's limits, and no known failure as one

`uman~iceoryx2-aou-iceoryx2-limits~1`

Covers:
- aou~seooc-iceoryx2-limits~1

## Status and scope

Draft integrator guidance for the safety manual of the iceoryx2 binding, against the specification input.
The quotation states an assumption of use of the up-rust SEooC, including future behavior; it is not an assertion that the binding or the SDK implements the refusals it names.

- This entry binds an integrator that deploys on the iceoryx2 binding and claims the up-rust SEooC's safety-assurance scope.
- Its lists were clauses of the SDK's qualified-bindings-and-wires assumption, and the permit, the abort and the service depths were also named by its waiver-approval and deployment-data assumptions. Their wording is unchanged. The SDK's manual keeps the general duties.
- Named records in the quote retain their classifications. This entry does not replace the binding's README, which states each record at its exact version.
- A known failure is the binding's to close. Listing one here accepts nothing.

## Assumption (verbatim)

<!-- aou-statement:start -->
> *Target.*
> The iceoryx2 binding claims no 32-bit target, because iceoryx2 0.9 needs 64-bit atomics and its bare-metal port is a proof of concept.
>
> *Accepted limits and known anomalies.*
> The integrator accepts the limits and known anomalies that each binding's README states (`+req~ssr-binding-readme~1+`, `+aou~seooc-qualified-bindings-and-wires~7+`). For a deployment that uses iceoryx2, these include:
>
> * allocation on a peer join or restart, under the waiver (`+UPW-iceoryx2-1+`), the permit whose waiver `+aou~seooc-waiver-approval~3+` has the deployment's owner review and file;
> * sizing of the expired-connection buffer: a receive route is refused at setup unless twice its retained budget fits in it;
> * the service depth, which now comes from deployment data (`+req~ssr-binding-depth-from-deployment~1+`): the service depths are part of the deployment data that `+aou~seooc-deployment-data~6+` covers;
> * the port mutex's panic path, on lock and on unlock, which no peer can reach (`+UPD-IOX2-0001+`, revision 2);
> * the two `+blocking_send+` `+unwrap+`s, which only a failing OS clock call reaches (`+UPD-IOX2-0002+`);
> * iceoryx2's other accepted panic paths: two process-private invariants; a descriptor that other code in the process closes while a socket is set up; `+close()+` failing on a departed peer's descriptor; and a sample counter's overflow on 64-bit targets, at over 580 years at 10^9^ sends per second (`+UPD-IOX2-0003+`, `+UPD-IOX2-0004+`, `+UPD-IOX2-0005+`);
> * heap exhaustion during a peer change, which aborts, within the deployment's heap sizing under `+UPW-iceoryx2-1+` (`+UPD-IOX2-0006+`): on iceoryx2 an allocation that fails during a peer change aborts the process, so that sizing is the one `+aou~seooc-waiver-approval~3+`, "Heap sizing", requires;
> * arrival reports from its receive thread, within that thread's period plus its stated pass bound, on a drained notification of the route's data ID together with a sample in the queue or taken since the previous pass, or on the queue turning non-empty;
> * a sender preempted between `+send+` and its notification delays the report, unless the queue was empty at the previous pass; a notification that fails at the sender while the queue holds samples leaves an arrival unreported, so a miss may be counted for a gap no longer than the deadline, and the sender counts the failure;
> * while a new publisher's data connection is being set up, its notification can be reported while older samples are queued;
> * iceoryx2 0.9.2 only logs a data connection that cannot be set up, under its default handler, and retries it only when the publisher list changes. So the binding installs a degradation handler on each port at setup: a failed connection is counted as `+receive_failed+`, or on the sending route, and the route then reports only a queue that turned non-empty, until the publisher list changes and an update runs clean; a queue that a healthy second publisher keeps non-empty meanwhile shows misses;
> * the event IDs are read from the event service's static config, and a route whose lifecycle events share the data ID, or whose data ID is above the service's `+event_id_max_value+`, is refused at setup.
>
> *Known failures.*
> The README also says that a frame drop can wait for the port mutex. That is not an accepted limit, because `+req~ssr-binding-frame-drop~6+` forbids it. It is the binding's known failure `+UPF-IOX2-0001+`, open until the binding returns each dropped frame to iceoryx2 through its deferred return as pending cleanup, charged and isolated as `+req~ssr-release-lease-drop~4+` requires; a thread of its own is not by itself that isolation.
>
> iceoryx2's other known failures are not accepted limits either, because `+req~ssr-binding-no-panic~5+` forbids them: its index bug on a connection change (`+UPF-IOX2-0002+`), until iceoryx2 fixes the index; its event-deadline panics, on QM transports only, since ASIL deployments turn those deadlines off, until iceoryx2 fixes them (`+UPF-IOX2-0005+`); and the paths through which a peer that writes values its own iceoryx2 never writes can panic its survivors, until iceoryx2 validates what peers write (`+UPF-IOX2-0004+`).
>
> The wait thread's descriptor-set panics (`+UPF-IOX2-0006+`) remain until its own poll loop replaces that set, or a reviewed equivalent with a source-backed argument is adopted in its place, and unsafe name and queue-size configuration (`+UPF-IOX2-0007+`) remains until setup refuses it.
>
> The expired-buffer failure is fixed by the twice-budget setup refusal; it is recorded separately as fixed, not as an accepted anomaly.
>
> *Strict mode.*
> In strict mode, the Enforce state refuses any known failure, whatever is acknowledged (`+aou~seooc-qualified-bindings-and-wires~7+`); while iceoryx2's known failures stand, that refuses each affected route at ASIL, including publishers affected by shared-state faults.
<!-- aou-statement:end -->

## Integrator actions

The integrator SHALL complete and retain the following checklist for the item:

- [ ] **Keep three registers apart.**
  - Separate accepted UPD anomalies, open UPF failures and UPW heap waivers by ID/revision.
  - The expired-buffer failure UPF-IOX2-0003 is recorded as fixed, not an accepted anomaly.
- [ ] **Verify iceoryx2's sizing and limits.**
  - Verify retained-budget/expired-buffer sizing, deployment service depths, event/data-ID separation and maximum, connection degradation behavior, arrival-thread period and notification/preemption limits.
  - Keep event-deadline restrictions and allocation bounds explicit.
  - iceoryx2 0.9 has no 32-bit target claim here.
- [ ] **File the waiver and size the heap.**
  - Treat UPW-iceoryx2-1 as a middleware heap waiver, under the SDK's waiver-approval entry, and review UPD-IOX2-0006's abort residual separately.
  - Size the affected heap for worst-case peer changes and concurrent load.
- [ ] **Do not approve deployments affected by known failures.**
  - Do not approve deployments affected by frame-drop blocking, shared-state faults (publishers included), connection-change index failures, wait-thread descriptor faults or unsafe setup configuration merely by acknowledging them.
  - Verify each fix and its affected paths before removing the restriction.
  - For the wait-thread descriptor faults, accept only the binding-owned poll loop or a reviewed, source-backed equivalent, with every claimed eliminated panic site shown unreachable.

## Violation manifestations

- A deployment affected by a known failure can block frame release, panic on a peer fault or corrupt shared state.
- An undersized heap aborts the process during a peer change.

## Detection limits

- Acceptance of an anomaly is not acceptance of an open failure.
- Strict known-failure refusal is required but not claimed implemented by this entry.
- Process death must be handled by the external supervisor.
