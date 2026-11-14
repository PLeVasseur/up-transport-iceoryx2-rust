// Copyright (c) 2026 Contributors to the Eclipse Foundation
// SPDX-License-Identifier: Apache-2.0

//! Checked boundaries before the selected native implementation's unchecked sizing.

use core::{
    alloc::Layout,
    fmt::Debug,
    mem::{align_of, size_of},
};
use iceoryx2::{
    config::Config,
    identifiers::UniqueNodeId,
    service::{
        dynamic_config::{
            DynamicConfig,
            event::{ListenerDetails, NotifierDetails},
            publish_subscribe::{PublisherDetails, SubscriberDetails},
        },
        static_config::{
            message_type_details::MessageTypeDetails, publish_subscribe::StaticConfig,
        },
    },
};
use iceoryx2_bb_concurrency::atomic::AtomicU64;
use iceoryx2_bb_lock_free::mpmc::container::Container;
use up_rust_core::transport::BindingRefusal;

pub(crate) const MAX_FRAME_LEN: usize = 1 << 30;
// iceoryx2-bb-memory 0.9.2 PoolAllocator uses bb-lock-free 0.9.3
// UniqueIndexSet: its new_uninit requires capacity < 2^24 - 1 and
// HeadDetails packs both head and borrowed count into 24 bits.
const NATIVE_POOL_CAPACITY_EXCLUSIVE: usize = (1 << 24) - 1;
const REFUSAL: BindingRefusal = BindingRefusal("native capacity or layout is not representable");

/// # Safety-usable invariant
/// Success bounds the full metadata/slack/payload slice by MAX_FRAME_LEN and a
/// valid signed Rust layout. Alignment is nonzero and a power of two.
pub(crate) fn frame_len(
    metadata: usize,
    payload: usize,
    alignment: usize,
) -> Result<usize, BindingRefusal> {
    if metadata == 0 || !alignment.is_power_of_two() {
        return Err(REFUSAL);
    }
    let size = metadata
        .checked_add(alignment - 1)
        .and_then(|n| n.checked_add(payload))
        .ok_or(REFUSAL)?;
    if size > MAX_FRAME_LEN {
        return Err(REFUSAL);
    }
    Layout::from_size_align(size, alignment).map_err(|_| REFUSAL)?;
    Ok(size)
}

/// # Safety-usable invariant
/// Matches iceoryx2 0.9.2 MessageTypeDetails::sample_layout's exact sum and
/// rounding, with checked intermediate arithmetic. The resulting Layout proves
/// the rounded size fits isize::MAX and its alignment is a nonzero power of two.
pub(crate) fn sample_layout(
    details: &MessageTypeDetails,
    elements: usize,
) -> Result<Layout, BindingRefusal> {
    sample_layout_parts(
        details.header.size(),
        details.header.alignment(),
        details.user_header.size(),
        details.user_header.alignment(),
        details.payload.size(),
        details.payload.alignment(),
        elements,
    )
}

fn sample_layout_parts(
    header: usize,
    header_align: usize,
    user: usize,
    user_align: usize,
    element: usize,
    payload_align: usize,
    elements: usize,
) -> Result<Layout, BindingRefusal> {
    if !header_align.is_power_of_two()
        || !user_align.is_power_of_two()
        || !payload_align.is_power_of_two()
    {
        return Err(REFUSAL);
    }
    let size = header
        .checked_add(user)
        .and_then(|n| n.checked_add(user_align - 1))
        .and_then(|n| element.checked_mul(elements).and_then(|p| n.checked_add(p)))
        .and_then(|n| n.checked_add(payload_align - 1))
        .ok_or(REFUSAL)?;
    Ok(Layout::from_size_align(size, header_align)
        .map_err(|_| REFUSAL)?
        .pad_to_align())
}

#[derive(Clone, Copy)]
pub(crate) struct Capacities {
    pub subscribers: usize,
    pub buffer: usize,
    pub borrowed: usize,
    pub history: usize,
}

impl Capacities {
    pub(crate) fn actual(config: &StaticConfig) -> Self {
        Self {
            subscribers: config.max_subscribers(),
            buffer: config.subscriber_max_buffer_size(),
            borrowed: config.subscriber_max_borrowed_samples(),
            history: config.history_size(),
        }
    }

    /// # Safety-usable invariant
    /// Exact checked form of iceoryx2 0.9.2 required_amount_of_samples_per_data_segment.
    pub(crate) fn samples(self, loans: usize) -> Result<usize, BindingRefusal> {
        self.buffer
            .checked_add(self.borrowed)
            .and_then(|n| self.subscribers.checked_mul(n))
            .and_then(|n| n.checked_add(self.history))
            .and_then(|n| n.checked_add(loans))
            .ok_or(REFUSAL)
    }
}

/// # Safety-usable invariant
/// Exact checked form of iceoryx2 0.9.2 DataSegment::create_static_segment's
/// chunk-size * count + alignment - 1, also constrained by the signed Layout
/// limit. Count is strictly below the selected UniqueIndexSet's packed capacity
/// bound. This proves arithmetic validity, not allocation success or residency.
fn segment_len(chunk: Layout, count: usize) -> Result<usize, BindingRefusal> {
    if count >= NATIVE_POOL_CAPACITY_EXCLUSIVE {
        return Err(REFUSAL);
    }
    let size = chunk
        .size()
        .checked_mul(count)
        .and_then(|n| n.checked_add(chunk.align() - 1))
        .ok_or(REFUSAL)?;
    Layout::from_size_align(size, chunk.align()).map_err(|_| REFUSAL)?;
    Ok(size)
}

/// # Safety-usable invariant
/// Before a publisher is created, validate the actual opened service's immutable
/// type details and capacities, including a compatible service larger than the
/// requested settings. No native sizing function is called by this guard.
pub(crate) fn publisher(
    config: &StaticConfig,
    elements: usize,
    loans: usize,
) -> Result<(), BindingRefusal> {
    let chunk = sample_layout(config.message_type_details(), elements)?;
    let count = Capacities::actual(config).samples(loans)?;
    segment_len(chunk, count)?;
    Ok(())
}

fn array_with_slack<T>(count: usize) -> Result<usize, BindingRefusal> {
    let layout = Layout::array::<T>(count).map_err(|_| REFUSAL)?;
    layout.size().checked_add(layout.align() - 1).ok_or(REFUSAL)
}

// bb-lock-free 0.9.3 Container::const_memory_size consists of the robust index
// set (one AtomicU64 array), generation array, and T array, each with slack.
// Use the actual dependency types; no byte constants stand in for their layout.
fn container_bytes<T: Copy + Debug>(count: usize) -> Result<usize, BindingRefusal> {
    let atomic = array_with_slack::<AtomicU64>(count)?;
    let size = atomic
        .checked_mul(2)
        .and_then(|n| {
            array_with_slack::<T>(count)
                .ok()
                .and_then(|t| n.checked_add(t))
        })
        .ok_or(REFUSAL)?;
    // Container::new_uninit also computes signed relative positions including
    // its inline header. This conservative bound covers those smaller offsets.
    let total = size.checked_add(size_of::<Container<T>>()).ok_or(REFUSAL)?;
    Layout::from_size_align(total, align_of::<Container<T>>().max(align_of::<T>()))
        .map_err(|_| REFUSAL)?;
    Ok(size)
}

fn service_storage(first: usize, second: usize, nodes: usize) -> Result<(), BindingRefusal> {
    let supplementary = first
        .checked_add(second)
        .and_then(|n| {
            container_bytes::<UniqueNodeId>(nodes)
                .ok()
                .and_then(|p| n.checked_add(p))
        })
        .ok_or(REFUSAL)?;
    // iceoryx2-cal 0.9.2 posix_shared_memory::Data<T> is repr(C) with exactly
    // AtomicU64, bool, T. Layout::extend + pad_to_align is that representation,
    // using the concrete native DynamicConfig as T.
    let (header, _) = Layout::new::<AtomicU64>()
        .extend(Layout::new::<bool>())
        .map_err(|_| REFUSAL)?;
    let (header, _) = header
        .extend(Layout::new::<DynamicConfig>())
        .map_err(|_| REFUSAL)?;
    let header = header.pad_to_align();
    let total = header.size().checked_add(supplementary).ok_or(REFUSAL)?;
    Layout::from_size_align(total, header.align()).map_err(|_| REFUSAL)?;
    Ok(())
}

/// # Safety-usable invariant
/// Checks local service container arrays, aggregate supplementary storage and
/// native sample-count arithmetic before the node/service builders see them.
/// It does not validate peer-controlled bytes or replace actual-service checks.
pub(crate) fn local_service_capacities(config: &Config) -> Result<(), BindingRefusal> {
    let p = &config.defaults.publish_subscribe;
    service_storage(
        container_bytes::<SubscriberDetails>(p.max_subscribers)?,
        container_bytes::<PublisherDetails>(p.max_publishers)?,
        p.max_nodes,
    )?;
    let e = &config.defaults.event;
    service_storage(
        container_bytes::<ListenerDetails>(e.max_listeners)?,
        container_bytes::<NotifierDetails>(e.max_notifiers)?,
        e.max_nodes,
    )?;
    Capacities {
        subscribers: p.max_subscribers,
        buffer: p.subscriber_max_buffer_size,
        borrowed: p.subscriber_max_borrowed_samples,
        history: p.publisher_history_size,
    }
    .samples(p.publisher_max_loaned_samples)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_boundaries_refuse_before_native_arithmetic() {
        assert_eq!(frame_len(MAX_FRAME_LEN, 0, 1).unwrap(), MAX_FRAME_LEN);
        assert!(frame_len(MAX_FRAME_LEN + 1, 0, 1).is_err());
        assert!(frame_len(MAX_FRAME_LEN, 0, 8).is_err());
        for align in [0, 3] {
            assert!(frame_len(1, 0, align).is_err());
        }
        assert!(frame_len(usize::MAX, 1, 2).is_err());
        assert!(frame_len(isize::MAX as usize, 0, 1).is_err());
    }

    #[test]
    fn native_header_rounding_and_product_are_checked() {
        assert!(sample_layout_parts(8, 8, 8, 8, 1, 1, isize::MAX as usize).is_err());
        assert!(sample_layout_parts(0, 8, 0, 1, 1, 1, isize::MAX as usize).is_err());
        assert!(sample_layout_parts(8, 8, 8, 8, usize::MAX, 1, 2).is_err());
        let layout = sample_layout_parts(8, 8, 40, 8, 1, 1, 17).unwrap();
        assert_eq!((layout.size(), layout.align()), (72, 8));
    }

    #[test]
    fn sample_count_sum_and_products_are_checked() {
        let valid = Capacities {
            subscribers: 2,
            buffer: 4,
            borrowed: 3,
            history: 2,
        };
        assert_eq!(valid.samples(1).unwrap(), 17);
        assert!(
            Capacities {
                buffer: usize::MAX,
                ..valid
            }
            .samples(1)
            .is_err()
        );
        assert!(
            Capacities {
                subscribers: usize::MAX,
                ..valid
            }
            .samples(1)
            .is_err()
        );
        assert!(valid.samples(usize::MAX).is_err());
    }

    #[test]
    fn actual_larger_service_capacity_changes_segment_admission() {
        let chunk = Layout::from_size_align(1024, 8).unwrap();
        let requested = Capacities {
            subscribers: 2,
            buffer: 4,
            borrowed: 3,
            history: 2,
        };
        let actual = Capacities {
            subscribers: (isize::MAX as usize / 1024),
            ..requested
        };
        assert!(segment_len(chunk, requested.samples(1).unwrap()).is_ok());
        assert!(segment_len(chunk, actual.samples(1).unwrap()).is_err());
    }

    #[test]
    fn native_pool_packed_count_boundary_is_enforced_without_allocation() {
        let chunk = Layout::from_size_align(8, 8).unwrap();
        assert!(segment_len(chunk, 16_777_214).is_ok());
        assert!(segment_len(chunk, 16_777_215).is_err());
        assert!(segment_len(chunk, 1 << 24).is_err());
    }

    #[test]
    fn segment_product_slack_and_service_containers_are_checked() {
        let chunk = Layout::from_size_align((isize::MAX as usize) & !7, 8).unwrap();
        assert!(segment_len(chunk, 3).is_err()); // usize product overflow, valid count
        assert!(segment_len(chunk, 1).is_err()); // signed rounding after alignment slack
        assert!(container_bytes::<PublisherDetails>(usize::MAX).is_err());
        assert!(local_service_capacities(&Config::default()).is_ok());
        let mut config = Config::default();
        config.defaults.publish_subscribe.max_subscribers = usize::MAX;
        assert!(local_service_capacities(&config).is_err());
    }
}
