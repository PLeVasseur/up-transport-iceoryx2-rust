// Copyright (c) 2026 Contributors to the Eclipse Foundation
// SPDX-License-Identifier: Apache-2.0

//! Who sent a frame, computed from the frame alone.
//!
//! The SDK's [`PeerRef`] needs an id that survives a restart and an incarnation
//! that rises across one. Both are pure functions of what a frame carries, so
//! nothing is tracked, nothing is shared between receiving threads, and no table
//! can fill or forget:
//!
//! - The id is a hash of the frame's source URI, which a restart does not change.
//! - The incarnation is the sending port's creation time on the host's monotonic
//!   clock. iceoryx2 records it in every sample's origin. A restarted port was
//!   created later, so its incarnation is higher, however frames of different
//!   generations are interleaved in a queue. A frame from a dead generation
//!   reports a lower incarnation and the SDK refuses it as stale (R40).
//!
//! Shared memory means one host and so one monotonic clock. Peers in different
//! Linux time namespaces would not share one, and are not supported.

use up_rust_core::transport::PeerRef;
use up_rust_core::uri::UUri;

const NANOS_PER_SECOND: u64 = 1_000_000_000;

/// The frame's peer. `id` is its stable id; `origin` is the sample's origin.
pub(crate) fn peer_ref(id: u64, origin: u128) -> PeerRef {
    PeerRef {
        peer: id,
        incarnation: incarnation(origin),
    }
}

/// The sending port's creation time, in nanoseconds on the monotonic clock.
///
/// `UniqueSystemId` is `repr(C)`: `pid`, `seconds`, `nanoseconds`, `counter`, and
/// its `value()` is those 16 bytes as a native-endian integer. Reading the two
/// time fields here avoids the `sysconf` call `creation_time()` makes on every
/// frame. iceoryx2 is pinned, and a test holds this against `creation_time()`.
fn incarnation(origin: u128) -> u64 {
    let [_, _, _, _, s0, s1, s2, s3, n0, n1, n2, n3, ..] = origin.to_ne_bytes();
    let seconds = u64::from(u32::from_ne_bytes([s0, s1, s2, s3]));
    let nanoseconds = u64::from(u32::from_ne_bytes([n0, n1, n2, n3]));
    // Cannot overflow: both are u32, so the largest value, (2^32 - 1) * 10^9 +
    // (2^32 - 1), is below 2^64, whatever a peer wrote into the origin. Checked
    // anyway, so the bound is in the code (req~ssr-refuse-no-panic~1); the
    // fallback is never taken. At least 1: the SDK reads incarnation 0 as
    // "nothing recorded yet".
    seconds
        .checked_mul(NANOS_PER_SECOND)
        .and_then(|nanos| nanos.checked_add(nanoseconds))
        .unwrap_or(u64::MAX)
        .max(1)
}

/// The id a frame from `uri` reports.
pub(crate) fn uri_peer_id(uri: &UUri) -> u64 {
    stable_peer_id(
        uri.authority_name(),
        (u32::from(uri.ue_instance_id()) << 16) | u32::from(uri.ue_type_id()),
        uri.uentity_major_version(),
        uri.resource_id(),
    )
}

/// A stable 64-bit identity for a peer, from the fields of its source URI.
///
/// FNV-1a, with zero mapped to 1 because the SDK reads peer id zero as "no
/// identity known". The same URI always gives the same id, in every process.
///
/// A collision is acceptable because it cannot go unnoticed where it would
/// matter. A publish, notification or response route has one source URI, fixed
/// by its service name. Only a request route has several, and the SDK refuses a
/// request from any client not in the server's `.clients(..)` list. Setup refuses
/// two different declared clients with the same id. A hash can therefore only
/// collide with a URI nobody declared, whose requests are refused anyway.
pub(crate) fn stable_peer_id(authority: &str, entity_id: u32, version: u8, resource: u16) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = OFFSET;
    let mut mix = |byte: u8| {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(PRIME);
    };
    for byte in authority.bytes() {
        mix(byte);
    }
    mix(0xFF);
    for byte in entity_id.to_le_bytes() {
        mix(byte);
    }
    mix(version);
    for byte in resource.to_le_bytes() {
        mix(byte);
    }
    hash.max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An origin with these time fields, built as `UniqueSystemId::value()` is.
    fn origin(seconds: u32, nanoseconds: u32) -> u128 {
        let mut bytes = [0_u8; 16];
        let (pid, rest) = bytes.split_at_mut(4);
        pid.copy_from_slice(&1234_u32.to_ne_bytes());
        let (time, _counter) = rest.split_at_mut(8);
        let (s, n) = time.split_at_mut(4);
        s.copy_from_slice(&seconds.to_ne_bytes());
        n.copy_from_slice(&nanoseconds.to_ne_bytes());
        u128::from_ne_bytes(bytes)
    }

    #[test]
    fn an_incarnation_is_the_creation_time_in_nanoseconds() {
        assert_eq!(incarnation(origin(3, 5)), 3_000_000_005);
    }

    #[test]
    fn the_time_fields_are_where_creation_time_reads_them() {
        #[cfg(not(miri))]
        {
            let id = iceoryx2_bb_posix::unique_system_id::UniqueSystemId::new().expect("id");
            let time = id.creation_time();
            assert_eq!(
                incarnation(id.value()),
                (time.seconds() * NANOS_PER_SECOND + u64::from(time.nanoseconds())).max(1)
            );
        }
    }

    #[test]
    fn a_zero_time_still_reports_a_valid_incarnation() {
        assert_eq!(incarnation(origin(0, 0)), 1);
    }

    #[test]
    fn a_later_port_is_a_higher_incarnation_in_any_observation_order() {
        let older = peer_ref(7, origin(10, 900));
        let newer = peer_ref(7, origin(11, 1));
        assert!(newer.incarnation > older.incarnation);
        // Seen the other way round, the numbers are the same: there is no memory.
        assert_eq!(peer_ref(7, origin(10, 900)), older);
    }

    #[test]
    fn a_frame_from_any_old_generation_stays_older_than_a_later_one() {
        let generations: Vec<u128> = (0..64).map(|second| origin(second, 0)).collect();
        for pair in generations.windows(2) {
            if let [earlier, later] = pair {
                assert!(incarnation(*later) > incarnation(*earlier));
            }
        }
    }

    #[test]
    fn concurrent_receivers_cannot_blend_a_stale_frame_with_a_newer_incarnation() {
        let older = origin(5, 0);
        let newer = origin(6, 0);
        std::thread::scope(|scope| {
            for thread in 0..4_u32 {
                scope.spawn(move || {
                    for round in 0..100_u32 {
                        let which = if (round + thread) % 2 == 0 {
                            older
                        } else {
                            newer
                        };
                        assert_eq!(peer_ref(7, which).incarnation, incarnation(which));
                    }
                });
            }
        });
    }

    #[test]
    fn stable_ids_are_nonzero_and_differ_by_field() {
        let a = stable_peer_id("door", 0x0001_0000, 1, 0x8001);
        let b = stable_peer_id("door", 0x0001_0000, 1, 0x8002);
        let c = stable_peer_id("door", 0x0002_0000, 1, 0x8001);
        assert_ne!(a, 0);
        assert_ne!(a, b);
        assert_ne!(a, c);
        assert_eq!(a, stable_peer_id("door", 0x0001_0000, 1, 0x8001));
    }

    #[test]
    fn a_uri_reports_the_id_its_frames_report() {
        let uri = UUri::try_from_wire_parts("door", 0x0002_0001, 3, 0x8001).expect("uri");
        assert_eq!(
            uri_peer_id(&uri),
            stable_peer_id("door", 0x0002_0001, 3, 0x8001)
        );
    }

    /// FNV-1a on realistic URIs: a million of them, no collision.
    #[test]
    fn realistic_uris_do_not_collide() {
        let count: u32 = if cfg!(miri) { 256 } else { 1 << 20 };
        let mut seen = std::collections::HashSet::with_capacity(count as usize);
        for index in 0..count {
            let authority = format!("ecu-{}.vehicle", index % 1024);
            let id = stable_peer_id(
                &authority,
                index.wrapping_mul(2_654_435_761),
                (index % 8) as u8,
                (index % 0xffff) as u16,
            );
            assert!(seen.insert(id), "collision at {index}");
        }
    }
}
