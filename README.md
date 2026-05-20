# up-transport-iceoryx2-rust
Rust uTransport implementation for iceoryx2

This crate implements the native `up-rust` zero-copy transport capability. Payload serializers write into an iceoryx2 transmit loan through `UTxBuffer`, and subscribers receive lease-backed frames through `UZeroCopyRxFrame`. Native frame metadata is fixed when the loan is reserved, then split between the fixed user header and an implementation metadata prefix so `UAttributes`, `UEncoding.format_id`, `UEncoding.content_type`, and optional `UEncoding.schema_ref` are preserved without exposing the prefix as application payload bytes.

The owned `UOwnedTransport` implementation is an adapter over the zero-copy path: sending an owned frame reserves a loan and copies the owned payload into it. Use the zero-copy extension helpers when the caller can serialize directly into the loan.

## How The Pieces Fit

| uProtocol frame part | iceoryx2 representation |
| --- | --- |
| Fixed protocol/version fields | iceoryx2 user header |
| Variable `UAttributes` fields | Hidden `UFM1` metadata prefix |
| `UEncoding.format_id` / `content_type` / `schema_ref` | Hidden `UFM1` metadata prefix |
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

## Verification

```sh
cargo check --all-targets
cargo test --test zero_copy_transport -- --nocapture
cargo doc --no-deps
```
