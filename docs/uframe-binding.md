# iceoryx2 UFrame Binding

Binding id: `iceoryx2.uframe.prefix.v1`

## Physical Placement

Selected-wire UFrame metadata is carried at the beginning of the iceoryx2 sample payload. The prefix contains the UFrame metadata envelope (see up-spec `basics/uframe.adoc`, Metadata envelope and identity registry): magic/version, selected-wire identity, payload-family identity, metadata-layout identity, and the selected metadata profile bytes.

The application payload starts at the aligned payload offset recorded in the iceoryx2 user header. The hidden metadata prefix is not part of the application payload exposed by selected-wire receive APIs.

## Metadata Profiles

The default profile is the canonical UFrame field-block metadata profile identified by `org.eclipse.uprotocol.metadata.uframe-fields`.

The legacy protobuf-`UAttributes` metadata profile remains compatibility-only and must be selected explicitly by a legacy-named API. Mixed-profile decode is rejected as an unknown metadata layout before a frame is exposed to users.

## Routing Mirror Validation

iceoryx2 service names continue to mirror the source and optional sink filters used by the transport. Received selected-wire frames are decoded from the metadata prefix and then checked against the requested source and sink filters using semantic UFrame metadata accessors.

## Malformed Input

Missing, malformed, wrong-wire, wrong-payload-family, wrong-profile, and filter-mismatched prefixes are rejected before public selected-wire frame exposure. Listener paths drop rejected frames instead of dispatching them.

## ABI User Header

This binding intentionally keeps the existing hidden-prefix placement. A fixed ABI user-header UFrame profile remains future work and is not implemented by this phase.
