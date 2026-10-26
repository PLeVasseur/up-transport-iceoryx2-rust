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

The header has explicit reserved bytes at offsets1–7, in place of implicit Rust
padding. Writers initialize them to zero, and receivers reject nonzero values
before exposing payload slices. Raw producers and Rust struct literals must set
`reserved: [0; 7]` (or start from `Default`). The physical major version is unchanged:
size40, alignment8 and the existing field offsets are retained.

| Field | Byte offset |
| --- | --- |
| `uprotocol_major_version` | 0 |
| `reserved` | 1 |
| `metadata_len` | 8 |
| `payload_offset` | 16 |
| `payload_len` | 24 |
| `payload_alignment` | 32 |

```rust
use up_transport_iceoryx2_rust::UProtocolHeader;
let header = UProtocolHeader::default();
assert_eq!(header.reserved, [0; 7]);
assert_eq!(std::mem::size_of_val(&header), 40);
assert_eq!(std::mem::align_of::<UProtocolHeader>(), 8);
```

Selected-wire metadata contexts combine the selected wire and metadata codec;
layout identity belongs to the codec. Loaned payload views use the SDK's safe
constructor while retaining the original iceoryx2 sample as the storage owner.

`Iceoryx2PayloadLayout` exposes validated lengths, alignment and an end-exclusive
payload range. The bounded pull mismatch queue reports retained depth, dropped
oldest samples, rejected newest samples and the latest mismatch diagnostic.

See [listener lifecycle](listener-lifecycle.md) for quiescence and retained sample
ownership. Public items, example targets and test harnesses are checked with
`missing_docs = "deny"`; strict Rustdoc/doctests are part of the same owner checks.
