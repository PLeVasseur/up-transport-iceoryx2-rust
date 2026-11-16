# Safety manual of the iceoryx2 binding: draft integrator guidance

This folder states the two assumptions of use of the up-rust SEooC that bind only a deployment on the iceoryx2 binding, one entry each.
It is draft guidance, not a certified release, safety case, assessment, deployment approval or implementation claim.

- The specification states both assumptions in `up-spec/safety/up-l1/iceoryx2.adoc`, and it governs where this folder and it differ.
- Each entry quotes its whole statement, then separates integrator actions, violation manifestations and detection limits. The entries use the layout of the SDK's safety manual, so the same exact-quotation check can read them.
- The SDK's safety manual states the duties of every transport and has no entry for these two. An integrator that deploys on iceoryx2 completes these entries as well as the SDK's.
- A documentary entry neither validates a deployment nor shows that the binding meets a requirement.
- The repository that holds this folder decides how it carries and checks it. The SDK's checker reads these entries only when it is given this folder, and otherwise reports them as not checked.

## Entries

- [iceoryx2-level-separation](iceoryx2-level-separation.md)
- [iceoryx2-limits](iceoryx2-limits.md)
