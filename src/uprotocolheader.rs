// ################################################################################
// Copyright (c) 2026 Contributors to the Eclipse Foundation
//
// SPDX-License-Identifier: Apache-2.0
// ################################################################################

use iceoryx2::prelude::ZeroCopySend;
use std::{fmt, ops::Range};
use up_rust::{UCode, UStatus};

/// Physical iceoryx2 user header.
///
/// This header stores only placement facts for selected-wire metadata and the
/// visible payload. Wire and payload identities remain in the metadata bytes.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, ZeroCopySend)]
pub struct UProtocolHeader {
    pub uprotocol_major_version: u8,
    pub metadata_len: u64,
    pub payload_offset: u64,
    pub payload_len: u64,
    pub payload_alignment: u64,
}

impl UProtocolHeader {
    pub(crate) fn write_payload_layout(
        &mut self,
        layout: Iceoryx2PayloadLayout,
    ) -> Result<(), FrameContractError> {
        self.uprotocol_major_version = crate::UPROTOCOL_MAJOR_VERSION;
        self.metadata_len = u64::try_from(layout.metadata_len)
            .map_err(|_| FrameContractError::FieldTooLarge("metadata_len"))?;
        self.payload_offset = u64::try_from(layout.payload_offset)
            .map_err(|_| FrameContractError::FieldTooLarge("payload_offset"))?;
        self.payload_len = u64::try_from(layout.payload_len)
            .map_err(|_| FrameContractError::FieldTooLarge("payload_len"))?;
        self.payload_alignment = u64::try_from(layout.payload_alignment)
            .map_err(|_| FrameContractError::FieldTooLarge("payload_alignment"))?;
        Ok(())
    }

    pub(crate) fn payload_layout(
        &self,
        sample_payload_len: usize,
    ) -> Result<Iceoryx2PayloadLayout, FrameContractError> {
        if self.uprotocol_major_version != crate::UPROTOCOL_MAJOR_VERSION {
            return Err(FrameContractError::UnsupportedMajorVersion(
                self.uprotocol_major_version,
            ));
        }
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
pub struct Iceoryx2PayloadLayout {
    metadata_len: usize,
    payload_offset: usize,
    payload_len: usize,
    payload_alignment: usize,
}

impl Iceoryx2PayloadLayout {
    pub(crate) fn from_validated_parts(
        metadata_len: usize,
        payload_offset: usize,
        payload_len: usize,
        payload_alignment: usize,
        sample_payload_len: usize,
    ) -> Result<Self, FrameContractError> {
        Self::validate(
            metadata_len,
            payload_offset,
            payload_len,
            payload_alignment,
            sample_payload_len,
        )
    }

    pub fn metadata_len(&self) -> usize {
        self.metadata_len
    }

    pub fn payload_len(&self) -> usize {
        self.payload_len
    }

    pub fn payload_alignment(&self) -> usize {
        self.payload_alignment
    }

    pub fn payload_range(&self) -> Range<usize> {
        self.payload_offset..self.payload_offset + self.payload_len
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum FrameContractError {
    FieldTooLarge(&'static str),
    InvalidAlignment(usize),
    LayoutOverflow,
    PayloadOffsetBeforeMetadata {
        metadata_len: usize,
        payload_offset: usize,
    },
    SampleTooSmall {
        sample_payload_len: usize,
        required_len: usize,
    },
    UnsupportedMajorVersion(u8),
}

impl fmt::Display for FrameContractError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FieldTooLarge(field) => write!(f, "frame contract field {field} exceeds usize"),
            Self::InvalidAlignment(alignment) => write!(
                f,
                "payload alignment {alignment} must be a non-zero power of two"
            ),
            Self::LayoutOverflow => f.write_str("iceoryx2 frame layout overflows usize"),
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
            Self::UnsupportedMajorVersion(version) => {
                write!(f, "unsupported uProtocol major version {version}")
            }
        }
    }
}

impl std::error::Error for FrameContractError {}

pub(crate) fn frame_contract_error_to_status(error: impl fmt::Display) -> UStatus {
    UStatus::fail_with_code(UCode::InvalidArgument, error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_offset_length_and_alignment_are_explicit() {
        let layout = Iceoryx2PayloadLayout::from_validated_parts(3, 8, 7, 8, 15).unwrap();
        let mut header = UProtocolHeader::default();
        header.write_payload_layout(layout).unwrap();

        assert_eq!(header.metadata_len, 3);
        assert_eq!(header.payload_offset, 8);
        assert_eq!(header.payload_len, 7);
        assert_eq!(header.payload_alignment, 8);
        assert_eq!(header.payload_layout(15).unwrap().payload_range(), 8..15);
    }
}
