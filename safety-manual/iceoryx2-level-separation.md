# On iceoryx2, each integrity level has its own processes, domain and OS user

`uman~iceoryx2-aou-iceoryx2-level-separation~1`

Covers:
- aou~seooc-iceoryx2-level-separation~1

## Status and scope

Draft integrator guidance for the safety manual of the iceoryx2 binding, against the specification input.
The quotation states an assumption of use of the up-rust SEooC, including future behavior; it is not an assertion that the binding or the SDK implements the refusal it relies on.

- This entry binds an integrator that deploys on the iceoryx2 binding and claims the up-rust SEooC's safety-assurance scope.
- Its sentences were clauses of the SDK's integrity-levels and peer-access assumptions, and their wording is unchanged. The SDK's manual keeps the duties of every transport: access to transport resources, one level per transport, and one level per two-way service.
- These are deployment duties. The specification requires no check of a domain or an OS user from the binding or the SDK, so this entry promises none: the integrator verifies the separation itself.

## Assumption (verbatim)

<!-- aou-statement:start -->
> For a deployment that uses iceoryx2, the integrator provides the following.
>
> * *The process's level.* On iceoryx2 a transport runs at its process's level.
> * *One level per service.* iceoryx2 is a binding whose ports write shared state both ways, so `+aou~seooc-peer-access~5+`, "Two-way shared state", binds every iceoryx2 service.
> * *Domains and OS users.* Each level runs in its own iceoryx2 domain, and its processes under their own OS user, so that a faulty lower process cannot open, map or remove a higher level's shared memory.
<!-- aou-statement:end -->

## Integrator actions

The integrator SHALL complete and retain the following checklist for the item:

- [ ] **Give each iceoryx2 transport its process's level.**
  - Record the development level of every process, and give every iceoryx2 transport in it that level.
- [ ] **Keep one level per service.**
  - Inventory every service, with every process that opens a port on it, subscribers included.
  - Give a lower-integrity consumer a copy route. Do not let it open the service.
- [ ] **Separate the levels by domain and OS user.**
  - Run each integrity level in its own iceoryx2 domain.
  - Run the processes of each level under an OS user of their own, and verify that a lower level's user cannot open, map or remove a higher level's shared memory.
  - Exercise that separation in deployment qualification. Do not rely on the binding or the SDK to report a shared domain or OS user.

## Violation manifestations

- A process at another level on a service can crash its peers or corrupt their memory, publishers included.
- A lower-level process that can reach a higher level's shared memory can remove or rewrite it.

## Detection limits

- The specification requires setup to refuse a port whose declared level differs from its service's recorded level. That refusal compares declared levels: it does not show that a process was built to the level it declares.
- The specification requires no automatic check of domains or OS users, so no detection of a shared one is guaranteed. That is a limit of the contract, not a statement about what a given release can see. Only the deployment's own verification covers the separation.
