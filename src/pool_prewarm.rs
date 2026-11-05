// Copyright (c) 2026 Contributors to the Eclipse Foundation
// SPDX-License-Identifier: Apache-2.0
//! Exact live-loan backing allocation without changing native layout or bytes.
use up_rust::{UCode, UStatus};

#[cfg(target_os = "linux")]
pub(crate) fn preallocate(address: usize) -> Result<(), UStatus> {
    use std::os::unix::fs::MetadataExt;
    let internal = |message: String| UStatus::fail_with_code(UCode::Internal, message);
    let maps = std::fs::read_to_string("/proc/self/maps")
        .map_err(|error| internal(format!("publisher backing mappings: {error}")))?;
    let mut selected = None;
    for line in maps.lines() {
        let columns: Vec<_> = line.split_whitespace().collect();
        let Some((start, end)) = columns.first().and_then(|range| range.split_once('-')) else {
            continue;
        };
        let start =
            usize::from_str_radix(start, 16).map_err(|error| internal(error.to_string()))?;
        let end = usize::from_str_radix(end, 16).map_err(|error| internal(error.to_string()))?;
        if (start..end).contains(&address) {
            if columns.len() < 6 || columns[1] != "rw-s" {
                return Err(internal(
                    "publisher backing is not a writable shared file mapping".into(),
                ));
            }
            selected = Some((
                start,
                end,
                columns.iter().map(|s| (*s).to_string()).collect::<Vec<_>>(),
            ));
            break;
        }
    }
    let (start, end, columns) =
        selected.ok_or_else(|| internal("native publisher backing mapping missing".into()))?;
    let path = columns[5..].join(" ");
    if !path.starts_with("/dev/shm/") || path.ends_with(" (deleted)") || columns[2] != "00000000" {
        return Err(internal(
            "publisher backing must be an existing whole POSIX SHM object".into(),
        ));
    }
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .map_err(|error| internal(format!("publisher backing open: {error}")))?;
    let metadata = file
        .metadata()
        .map_err(|error| internal(error.to_string()))?;
    let (major, minor) = columns[3]
        .split_once(':')
        .ok_or_else(|| internal("backing device missing".into()))?;
    let major = u32::from_str_radix(major, 16).map_err(|error| internal(error.to_string()))?;
    let minor = u32::from_str_radix(minor, 16).map_err(|error| internal(error.to_string()))?;
    let inode = columns[4]
        .parse::<u64>()
        .map_err(|error| internal(error.to_string()))?;
    let extent = end
        .checked_sub(start)
        .ok_or_else(|| internal("publisher mapping extent overflow".into()))?;
    if rustix::fs::major(metadata.dev()) != major
        || rustix::fs::minor(metadata.dev()) != minor
        || metadata.ino() != inode
        || metadata.len() == 0
        || metadata.len() > extent as u64
    {
        return Err(internal(
            "publisher backing descriptor does not match live native mapping".into(),
        ));
    }
    rustix::fs::fallocate(
        &file,
        rustix::fs::FallocateFlags::empty(),
        0,
        metadata.len(),
    )
    .map_err(|error| {
        UStatus::fail_with_code(
            UCode::ResourceExhausted,
            format!("native publisher backing allocation failed: {error}"),
        )
    })?;
    use std::os::unix::fs::FileExt;
    let zeroes = [0_u8; 4096];
    let mut cursor = 0_u64;
    let mut materialized_bytes = 0_u64;
    while cursor < metadata.len() {
        let offset = i64::try_from(cursor).map_err(|error| internal(error.to_string()))?;
        let hole = match rustix::fs::seek(&file, rustix::fs::SeekFrom::Hole(offset)) {
            Ok(hole) => hole,
            Err(rustix::io::Errno::NXIO) => break,
            Err(error) => return Err(internal(format!("native backing hole query: {error}"))),
        };
        if hole >= metadata.len() {
            break;
        }
        let next = match rustix::fs::seek(
            &file,
            rustix::fs::SeekFrom::Data(
                i64::try_from(hole).map_err(|error| internal(error.to_string()))?,
            ),
        ) {
            Ok(next) => next,
            Err(rustix::io::Errno::NXIO) => metadata.len(),
            Err(error) => return Err(internal(format!("native backing data query: {error}"))),
        };
        if hole < cursor || next <= hole || next > metadata.len() {
            return Err(internal("invalid native backing hole extent".into()));
        }
        let mut position = hole;
        while position < next {
            let count = usize::try_from((next - position).min(zeroes.len() as u64))
                .map_err(|error| internal(error.to_string()))?;
            file.write_all_at(&zeroes[..count], position)
                .map_err(|error| {
                    internal(format!("native backing hole materialization: {error}"))
                })?;
            position = position
                .checked_add(count as u64)
                .ok_or_else(|| internal("backing write extent overflow".into()))?;
            materialized_bytes = materialized_bytes
                .checked_add(count as u64)
                .ok_or_else(|| internal("backing byte count overflow".into()))?;
        }
        cursor = next;
    }
    let after = file
        .metadata()
        .map_err(|error| internal(error.to_string()))?;
    if after.len() != metadata.len() || after.ino() != inode || after.dev() != metadata.dev() {
        return Err(internal(
            "publisher backing identity or extent changed during preallocation".into(),
        ));
    }
    println!(
        "NATIVE_POOL_PREALLOCATED backing_device={} inode={} backing_bytes={} materialized_hole_bytes={materialized_bytes}",
        after.dev(),
        inode,
        after.len()
    );
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn preallocate(_: usize) -> Result<(), UStatus> {
    Err(UStatus::fail_with_code(
        UCode::Unimplemented,
        "publisher backing preallocation requires Linux",
    ))
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use std::io::Read;
    use up_rust::{
        PayloadEncoding, ProtobufWire, UFrameFieldsMetadataCodec, UFrameMetadata, UTxBuffer, UUri,
        UWireMetadataCodec, UWireMetadataContext,
    };

    #[test]
    fn absent_native_mapping_is_a_typed_setup_error() {
        assert_eq!(preallocate(0).unwrap_err().code(), UCode::Internal);
    }

    #[tokio::test]
    async fn sparse_materialization_preserves_existing_native_header_and_payload_bytes() {
        let root = std::env::var("UP_ICEORYX2_TEST_ROOT").unwrap();
        std::fs::create_dir_all(&root).unwrap();
        let mut config = crate::Iceoryx2PubSubConfig::static_allocation(1024 * 1024)
            .with_namespace(&root, &format!("preserve{}_", std::process::id()))
            .unwrap();
        config.require_exact_service_limits = true;
        let transport = crate::Iceoryx2PubSub::try_with_config(config).unwrap();
        let metadata = UFrameMetadata::publish(
            UUri::try_from_parts("preserve", 0x0001_4210, 1, 0x9000).unwrap(),
        )
        .with_payload_encoding(PayloadEncoding::PROTOBUF)
        .build()
        .unwrap();
        let encoded = UFrameFieldsMetadataCodec
            .encode_frame_metadata(
                UWireMetadataContext::from_wire::<ProtobufWire, UFrameFieldsMetadataCodec>(),
                &metadata,
            )
            .unwrap();
        let mut loan = transport
            .loan_encoded(metadata, &encoded, 16, 8)
            .await
            .unwrap();
        loan.payload_mut().fill(0xa5);
        let address = loan.payload().as_ptr() as usize;
        let maps = std::fs::read_to_string("/proc/self/maps").unwrap();
        let line = maps
            .lines()
            .find(|line| {
                let range = line.split_whitespace().next().unwrap();
                let (start, end) = range.split_once('-').unwrap();
                let start = usize::from_str_radix(start, 16).unwrap();
                let end = usize::from_str_radix(end, 16).unwrap();
                (start..end).contains(&address)
            })
            .unwrap();
        let columns: Vec<_> = line.split_whitespace().collect();
        let path = columns[5..].join(" ");
        let prefix = || {
            let mut bytes = [0_u8; 4096];
            std::fs::File::open(&path)
                .unwrap()
                .read_exact(&mut bytes)
                .unwrap();
            bytes
        };
        let before = prefix();
        preallocate(address).unwrap();
        assert_eq!(
            prefix(),
            before,
            "existing native management/header bytes changed"
        );
        assert_eq!(loan.payload(), &[0xa5; 16]);
    }
}
