# up-transport-iceoryx2-rust
Rust uTransport implementation for iceoryx2

## Feature-Gated Owned Support

`Iceoryx2OwnedCore` is available only with `--features benchmark-owned`. It is a disabled-by-default owned-frame support path for benchmark/support measurements and selected-wire owned tests; it is not zero-copy evidence and is not part of the default product-real iceoryx2 transport API.
