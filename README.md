# up-transport-iceoryx2-rust
Rust uTransport implementation for iceoryx2

This branch implements the native `up-rust` zero-copy transport capability. Payload serializers write into an iceoryx2 transmit loan through `UTxBuffer`, and subscribers receive lease-backed frames through `UZeroCopyRxFrame`. Native frame metadata is fixed when the loan is reserved, then split between the fixed user header and an implementation metadata prefix so `UAttributes`, `UEncoding.format_id`, `UEncoding.content_type`, and optional `UEncoding.schema_ref` are preserved without exposing the prefix as application payload bytes.

The owned `UOwnedTransport` implementation is an adapter over the zero-copy path: sending an owned frame reserves a loan and copies the owned payload into it. Use the zero-copy extension helpers when the caller can serialize directly into the loan.
