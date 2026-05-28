# up-transport-iceoryx2-rust
Rust uTransport implementation for iceoryx2

This crate implements the native `up-rust` zero-copy transport capability. Payload serializers write into an iceoryx2 transmit loan through `UTxBuffer`, and subscribers receive lease-backed frames through `UZeroCopyRxFrame`. Native frame metadata is fixed when the loan is reserved, then split between the fixed user header and an implementation metadata prefix so `UAttributes` and `PayloadEncoding` are preserved without exposing the prefix as application payload bytes.

`Iceoryx2PubSub` does not implement `UOwnedTransport` directly. Use `UOwnedFrameEndpoint::from_zero_copy_copying_adapter` when an owned-frame boundary is intentional; that adapter copies at the boundary.

## How The Pieces Fit

| uProtocol frame part | iceoryx2 representation |
| --- | --- |
| Fixed protocol/version fields | iceoryx2 user header |
| Variable `UAttributes` fields | Hidden `UFM1` metadata prefix |
| `PayloadEncoding` | Hidden `UFM1` metadata prefix |
| Alignment padding | Hidden between metadata prefix and payload |
| Application payload bytes | Exposed loan/lease payload slice only |

The payload returned by `Iceoryx2TxLoan::payload_mut()` and `Iceoryx2RxLease::contiguous_payload()` excludes the user header, metadata prefix, and padding. Transport implementers should keep that rule intact for any future framing changes: application payload views must contain exactly the bytes produced by the selected `up_rust::payload::PayloadFormat` serializer.

## Typed Payloads

Already-encoded bytes can be sent without first constructing a `UOwnedFrame`:

```rust
use up_rust::{payload::RawBytes, zero_copy::UZeroCopyTransportExt, UFrameMetadata};

async fn send<T>(transport: &T, metadata: UFrameMetadata) -> Result<(), up_rust::UStatus>
where
    T: up_rust::zero_copy::UZeroCopyTransport,
{
let payload: &[u8] = b"payload";
transport
    .send_serialized_zero_copy::<RawBytes, _>(metadata, &payload)
    .await
}
```

On receive, use `UZeroCopyRxFrame::deserialize_from_reader::<Codec, T>()` for generic leases or `UContiguousZeroCopyRxFrame::deserialize_borrowed::<Codec, T>()` when the decoded value needs to borrow from the contiguous iceoryx2 sample.

Stable typed payloads can be constructed in shared memory without first
default-initializing the application payload region:

```rust
use up_rust::{payload::StableContainerPayload, UFrameMetadata, UZeroCopyUninitTransportExt};

#[repr(C)]
#[derive(Clone, Copy, up_rust::StablePayload, up_rust::ByteBackedStablePayload)]
#[stable_payload(type_name = "example.vehicle.VehiclePose")]
struct VehiclePose {
    x: u64,
    y: u64,
}

async fn send<T>(transport: &T, metadata: UFrameMetadata) -> Result<(), up_rust::UStatus>
where
    T: up_rust::UZeroCopyUninitTransport,
{
    transport
        .send_uninit_loaned_payload_as::<StableContainerPayload<VehiclePose>, VehiclePose>(
            metadata,
            |slot| Ok(slot.write(VehiclePose { x: 1, y: 2 })),
        )
        .await
}
```

Use `Iceoryx2PubSubConfig::static_allocation(max_slice_len)` with
`UTransportIceoryx2::build_with_config` for deterministic runs that fail instead
of growing shared memory when a frame exceeds the configured capacity. Size the
capacity for the hidden metadata prefix, alignment padding, and application
payload bytes. The transport requests one deterministic worst-case sample length
of `metadata_len + payload_len + alignment - 1`; any unused suffix is hidden
transport padding and is never exposed through application payload views.

The transport preserves the distinction between no payload and a present empty
payload: no payload has no `PayloadEncoding`, while a present empty payload keeps
its encoding and reports payload presence with length zero. Payload bytes with no
encoding are rejected before send.

Filtered pull receive preserves nonmatching samples in an internal per-service
queue so another matching receive call can still observe them. The queue is not
currently bounded by a public resource policy; deployments that rely heavily on
mismatched pull filters should treat this as a resource consideration.

## Verification

```sh
cargo check --all-targets
cargo test --test zero_copy_transport -- --nocapture
cargo doc --no-deps
```
