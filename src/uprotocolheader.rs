// ################################################################################
// Copyright (c) 2025 Contributors to the Eclipse Foundation
//
// See the NOTICE file(s) distributed with this work for additional
// information regarding copyright ownership.
//
// This program and the accompanying materials are made available under the
// terms of the Apache License Version 2.0 which is available at
// https: //www.apache.org/licenses/LICENSE-2.0
//
// SPDX-License-Identifier: Apache-2.0
// ################################################################################

use iceoryx2::prelude::ZeroCopySend;
use iceoryx2_bb_container::vec::FixedSizeVec;
use std::{fmt, ops::Range};

pub const MAX_FEASIBLE_UATTRIBUTES_SERIALIZED_LENGTH: usize = 1000;
pub(crate) const FRAME_METADATA_MAGIC: [u8; 4] = *b"UFM1";

/// Also see [uAttributes Mapping to iceoryx2 user header](https://github.com/eclipse-uprotocol/up-spec/blob/0cc43c8afb7d7cbd3169ffe093be761c57308cef/up-l1/iceoryx2.adoc#411-uattributes-mapping-to-iceoryx2-user-header)
#[repr(C)]
#[derive(ZeroCopySend, Debug, Default)]
pub struct UProtocolHeader {
    pub(crate) uprotocol_major_version: u8,
    pub(crate) uattributes_serialized: FixedSizeVec<u8, MAX_FEASIBLE_UATTRIBUTES_SERIALIZED_LENGTH>,
    pub(crate) metadata_len: u64,
    pub(crate) payload_offset: u64,
    pub(crate) payload_len: u64,
    pub(crate) payload_alignment: u64,
}

impl UProtocolHeader {
    pub(crate) fn write_payload_layout(
        &mut self,
        metadata_len: usize,
        payload_len: usize,
        payload_alignment: usize,
    ) -> Result<Iceoryx2PayloadLayout, FrameContractError> {
        let layout =
            Iceoryx2PayloadLayout::for_lengths(metadata_len, payload_len, payload_alignment)?;

        self.metadata_len = u64::try_from(layout.metadata_len)
            .map_err(|_| FrameContractError::FieldTooLarge("metadata_len"))?;
        self.payload_offset = u64::try_from(layout.payload_offset)
            .map_err(|_| FrameContractError::FieldTooLarge("payload_offset"))?;
        self.payload_len = u64::try_from(layout.payload_len)
            .map_err(|_| FrameContractError::FieldTooLarge("payload_len"))?;
        self.payload_alignment = u64::try_from(layout.payload_alignment)
            .map_err(|_| FrameContractError::FieldTooLarge("payload_alignment"))?;
        Ok(layout)
    }

    pub(crate) fn payload_layout(
        &self,
        sample_payload_len: usize,
    ) -> Result<Iceoryx2PayloadLayout, FrameContractError> {
        Iceoryx2PayloadLayout::validate(
            usize::try_from(self.metadata_len)
                .map_err(|_| FrameContractError::FieldTooLarge("metadata_len"))?,
            usize::try_from(self.payload_offset)
                .map_err(|_| FrameContractError::FieldTooLarge("payload_offset"))?,
            usize::try_from(self.payload_len)
                .map_err(|_| FrameContractError::FieldTooLarge("payload_len"))?,
            usize::try_from(self.payload_alignment)
                .map_err(|_| FrameContractError::FieldTooLarge("payload_alignment"))?,
            sample_payload_len,
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Iceoryx2PayloadLayout {
    metadata_len: usize,
    payload_offset: usize,
    payload_len: usize,
    payload_alignment: usize,
}

impl Iceoryx2PayloadLayout {
    pub(crate) fn for_lengths(
        metadata_len: usize,
        payload_len: usize,
        payload_alignment: usize,
    ) -> Result<Self, FrameContractError> {
        if payload_alignment == 0 || !payload_alignment.is_power_of_two() {
            return Err(FrameContractError::InvalidAlignment(payload_alignment));
        }

        let payload_offset =
            align_up(metadata_len, payload_alignment).ok_or(FrameContractError::LayoutOverflow)?;
        let sample_payload_len = payload_offset
            .checked_add(payload_len)
            .ok_or(FrameContractError::LayoutOverflow)?;

        Self::validate(
            metadata_len,
            payload_offset,
            payload_len,
            payload_alignment,
            sample_payload_len,
        )
    }

    pub(crate) fn metadata_len(&self) -> usize {
        self.metadata_len
    }

    pub(crate) fn metadata_prefix<'a>(
        &self,
        sample_payload: &'a [u8],
    ) -> Result<&'a [u8], FrameContractError> {
        self.ensure_sample_len(sample_payload.len())?;
        sample_payload
            .get(..self.metadata_len)
            .ok_or(FrameContractError::SampleTooSmall {
                sample_payload_len: sample_payload.len(),
                required_len: self.metadata_len,
            })
    }

    pub(crate) fn payload<'a>(
        &self,
        sample_payload: &'a [u8],
    ) -> Result<&'a [u8], FrameContractError> {
        self.ensure_sample_len(sample_payload.len())?;
        sample_payload
            .get(self.payload_range())
            .ok_or(FrameContractError::SampleTooSmall {
                sample_payload_len: sample_payload.len(),
                required_len: self.payload_offset + self.payload_len,
            })
    }

    pub(crate) fn payload_range(&self) -> Range<usize> {
        self.payload_offset..self.payload_offset + self.payload_len
    }

    fn validate(
        metadata_len: usize,
        payload_offset: usize,
        payload_len: usize,
        payload_alignment: usize,
        sample_payload_len: usize,
    ) -> Result<Self, FrameContractError> {
        if payload_alignment == 0 || !payload_alignment.is_power_of_two() {
            return Err(FrameContractError::InvalidAlignment(payload_alignment));
        }
        if payload_offset < metadata_len {
            return Err(FrameContractError::PayloadOffsetBeforeMetadata {
                metadata_len,
                payload_offset,
            });
        }
        if !payload_offset.is_multiple_of(payload_alignment) {
            return Err(FrameContractError::MisalignedPayloadOffset {
                payload_offset,
                payload_alignment,
            });
        }

        let required_len = payload_offset
            .checked_add(payload_len)
            .ok_or(FrameContractError::LayoutOverflow)?;
        if required_len > sample_payload_len {
            return Err(FrameContractError::SampleTooSmall {
                sample_payload_len,
                required_len,
            });
        }

        Ok(Self {
            metadata_len,
            payload_offset,
            payload_len,
            payload_alignment,
        })
    }

    fn ensure_sample_len(&self, sample_payload_len: usize) -> Result<(), FrameContractError> {
        let required_len = self
            .payload_offset
            .checked_add(self.payload_len)
            .ok_or(FrameContractError::LayoutOverflow)?;
        if required_len > sample_payload_len {
            return Err(FrameContractError::SampleTooSmall {
                sample_payload_len,
                required_len,
            });
        }
        Ok(())
    }
}

pub(crate) fn validate_metadata_prefix(metadata: &[u8]) -> Result<(), FrameContractError> {
    let Some(actual) = metadata.get(..FRAME_METADATA_MAGIC.len()) else {
        return Err(FrameContractError::InvalidMetadataPrefix);
    };
    if actual != FRAME_METADATA_MAGIC {
        return Err(FrameContractError::InvalidMetadataPrefix);
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum FrameContractError {
    FieldTooLarge(&'static str),
    InvalidAlignment(usize),
    InvalidMetadataPrefix,
    LayoutOverflow,
    MisalignedPayloadOffset {
        payload_offset: usize,
        payload_alignment: usize,
    },
    PayloadOffsetBeforeMetadata {
        metadata_len: usize,
        payload_offset: usize,
    },
    SampleTooSmall {
        sample_payload_len: usize,
        required_len: usize,
    },
}

impl fmt::Display for FrameContractError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FieldTooLarge(field) => write!(f, "frame contract field {field} exceeds usize"),
            Self::InvalidAlignment(alignment) => write!(
                f,
                "payload alignment {alignment} must be a non-zero power of two"
            ),
            Self::InvalidMetadataPrefix => f.write_str("invalid iceoryx2 frame metadata prefix"),
            Self::LayoutOverflow => f.write_str("iceoryx2 frame layout overflows usize"),
            Self::MisalignedPayloadOffset {
                payload_offset,
                payload_alignment,
            } => write!(
                f,
                "payload offset {payload_offset} is not aligned to {payload_alignment}"
            ),
            Self::PayloadOffsetBeforeMetadata {
                metadata_len,
                payload_offset,
            } => write!(
                f,
                "payload offset {payload_offset} precedes metadata length {metadata_len}"
            ),
            Self::SampleTooSmall {
                sample_payload_len,
                required_len,
            } => write!(
                f,
                "sample payload length {sample_payload_len} is smaller than required frame length {required_len}"
            ),
        }
    }
}

impl std::error::Error for FrameContractError {}

fn align_up(value: usize, alignment: usize) -> Option<usize> {
    debug_assert!(alignment.is_power_of_two());
    value
        .checked_add(alignment - 1)
        .map(|value| value & !(alignment - 1))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn application_payload_excludes_iceoryx2_metadata_prefix() {
        let layout = Iceoryx2PayloadLayout::for_lengths(5, 4, 4).unwrap();
        let sample_payload = b"UFM1x___data";

        assert_eq!(layout.metadata_prefix(sample_payload).unwrap(), b"UFM1x");
        assert_eq!(layout.payload(sample_payload).unwrap(), b"data");
        assert_eq!(layout.payload_range(), 8..12);
    }

    #[test]
    fn payload_offset_length_and_alignment_are_explicit() {
        let mut header = UProtocolHeader::default();
        header.write_payload_layout(3, 7, 8).unwrap();
        let layout = header.payload_layout(15).unwrap();

        assert_eq!(layout.metadata_len, 3);
        assert_eq!(layout.payload_offset, 8);
        assert_eq!(layout.payload_len, 7);
        assert_eq!(layout.payload_alignment, 8);
        assert_eq!(layout.payload_range(), 8..15);
    }

    #[test]
    fn invalid_layout_fails_loudly() {
        assert_eq!(
            Iceoryx2PayloadLayout::for_lengths(3, 1, 3).unwrap_err(),
            FrameContractError::InvalidAlignment(3)
        );
        assert_eq!(
            Iceoryx2PayloadLayout::validate(5, 4, 1, 4, 8).unwrap_err(),
            FrameContractError::PayloadOffsetBeforeMetadata {
                metadata_len: 5,
                payload_offset: 4,
            }
        );
        assert_eq!(
            Iceoryx2PayloadLayout::validate(4, 6, 1, 4, 8).unwrap_err(),
            FrameContractError::MisalignedPayloadOffset {
                payload_offset: 6,
                payload_alignment: 4,
            }
        );
        assert_eq!(
            Iceoryx2PayloadLayout::validate(4, 4, 8, 4, 11).unwrap_err(),
            FrameContractError::SampleTooSmall {
                sample_payload_len: 11,
                required_len: 12,
            }
        );
    }

    #[test]
    fn metadata_prefix_change_fails_loudly() {
        validate_metadata_prefix(b"UFM1metadata").unwrap();
        assert_eq!(
            validate_metadata_prefix(b"BAD1metadata").unwrap_err(),
            FrameContractError::InvalidMetadataPrefix
        );
        assert_eq!(
            validate_metadata_prefix(b"UF").unwrap_err(),
            FrameContractError::InvalidMetadataPrefix
        );
    }
}
