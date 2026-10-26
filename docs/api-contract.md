# Public API and sample placement

`UTransportIceoryx2::build` creates the classic publish/subscribe transport and
returns `Unimplemented` for other messaging patterns. Selected-wire applications
instead configure `Iceoryx2PubSub` and compose their chosen SDK wire adapter.

`UProtocolHeader` carries the physical header version and metadata/payload extents.
The metadata prefix and visible payload share the sample byte region; payload
offsets are relative to that region, not the separate user header. The declared
alignment must be a nonzero power of two. Layout validation checks conversion,
ordering, checked end offsets and the actual sample length before exposing slices.
These placement fields do not replace the selected-wire identity or final filters.

`Iceoryx2PayloadLayout` exposes validated lengths, alignment and an end-exclusive
payload range. The bounded pull mismatch queue reports retained depth, dropped
oldest samples, rejected newest samples and the latest mismatch diagnostic.

See [listener lifecycle](listener-lifecycle.md) for quiescence and retained sample
ownership. Public items, example targets and test harnesses are checked with
`missing_docs = "deny"`; strict Rustdoc/doctests are part of the same owner checks.
