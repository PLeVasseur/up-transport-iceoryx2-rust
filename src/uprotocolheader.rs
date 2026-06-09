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
use std::{borrow::Cow, fmt, ops::Range};
use up_rust::{
    PayloadEncoding, UAttributes, UCode, UFrameMetadata, UMessageType, UPayloadFormat, UPriority,
    UStatus, UUID, UUri,
};

pub(crate) const FRAME_METADATA_MAGIC: [u8; 4] = *b"UFM1";

/// Also see [uAttributes Mapping to iceoryx2 user header](https://github.com/eclipse-uprotocol/up-spec/blob/0cc43c8afb7d7cbd3169ffe093be761c57308cef/up-l1/iceoryx2.adoc#411-uattributes-mapping-to-iceoryx2-user-header)
#[repr(C)]
#[derive(ZeroCopySend, Debug, Default)]
pub struct UProtocolHeader {
    pub(crate) uprotocol_major_version: u8,
    pub(crate) id: [u8; 16],
    pub(crate) message_type: u8,
    pub(crate) priority_present: u8,
    pub(crate) priority: u8,
    pub(crate) ttl_present: u8,
    pub(crate) ttl: u32,
    pub(crate) request_id_present: u8,
    pub(crate) request_id: [u8; 16],
    pub(crate) permission_level_present: u8,
    pub(crate) permission_level: u32,
    pub(crate) commstatus_present: u8,
    pub(crate) commstatus: i32,
    pub(crate) payload_format_present: u8,
    pub(crate) payload_format: i32,
    pub(crate) source_ue_id: u32,
    pub(crate) source_ue_version_major: u8,
    pub(crate) source_resource_id: u16,
    pub(crate) sink_present: u8,
    pub(crate) sink_ue_id: u32,
    pub(crate) sink_ue_version_major: u8,
    pub(crate) sink_resource_id: u16,
    pub(crate) metadata_len: u64,
    pub(crate) payload_offset: u64,
    pub(crate) payload_len: u64,
    pub(crate) payload_alignment: u64,
}

impl UProtocolHeader {
    pub(crate) fn write_attributes(&mut self, attributes: &UAttributes) -> Result<(), UStatus> {
        write_uuid(&mut self.id, attributes.id());
        self.message_type = message_type_to_byte(attributes.type_());

        if let Some(priority) = attributes.priority() {
            self.priority_present = 1;
            self.priority = priority_to_byte(priority);
        } else {
            self.priority_present = 0;
            self.priority = 0;
        }

        if let Some(ttl) = attributes.ttl() {
            self.ttl_present = 1;
            self.ttl = ttl;
        } else {
            self.ttl_present = 0;
            self.ttl = 0;
        }

        if let Some(request_id) = attributes.request_id() {
            self.request_id_present = 1;
            write_uuid(&mut self.request_id, request_id);
        } else {
            self.request_id_present = 0;
            self.request_id = [0; 16];
        }

        if let Some(permission_level) = attributes.permission_level() {
            self.permission_level_present = 1;
            self.permission_level = permission_level;
        } else {
            self.permission_level_present = 0;
            self.permission_level = 0;
        }

        if let Some(commstatus) = attributes.commstatus() {
            self.commstatus_present = 1;
            self.commstatus = commstatus.value();
        } else {
            self.commstatus_present = 0;
            self.commstatus = 0;
        }

        if let Some(payload_format) = attributes.payload_format() {
            self.payload_format_present = 1;
            self.payload_format = payload_format.as_i32();
        } else {
            self.payload_format_present = 0;
            self.payload_format = 0;
        }

        write_uri_fields(
            attributes.source(),
            &mut self.source_ue_id,
            &mut self.source_ue_version_major,
            &mut self.source_resource_id,
        );
        if let Some(sink) = attributes.sink() {
            self.sink_present = 1;
            write_uri_fields(
                sink,
                &mut self.sink_ue_id,
                &mut self.sink_ue_version_major,
                &mut self.sink_resource_id,
            );
        } else {
            self.sink_present = 0;
            self.sink_ue_id = 0;
            self.sink_ue_version_major = 0;
            self.sink_resource_id = 0;
        }

        Ok(())
    }

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

    pub(crate) fn write_payload_layout_at_offset(
        &mut self,
        metadata_len: usize,
        payload_offset: usize,
        payload_len: usize,
        payload_alignment: usize,
        sample_payload_len: usize,
    ) -> Result<Iceoryx2PayloadLayout, FrameContractError> {
        let layout = Iceoryx2PayloadLayout::validate(
            metadata_len,
            payload_offset,
            payload_len,
            payload_alignment,
            sample_payload_len,
        )?;

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

    pub(crate) fn frame_metadata(&self, sample_payload: &[u8]) -> Result<UFrameMetadata, UStatus> {
        if self.uprotocol_major_version != crate::UPROTOCOL_MAJOR_VERSION {
            return Err(UStatus::fail_with_code(
                UCode::InvalidArgument,
                "unsupported uProtocol major version",
            ));
        }

        let layout = self
            .payload_layout(sample_payload.len())
            .map_err(|error| UStatus::fail_with_code(UCode::InvalidArgument, error.to_string()))?;
        let metadata_prefix = layout
            .metadata_prefix(sample_payload)
            .map_err(|error| UStatus::fail_with_code(UCode::InvalidArgument, error.to_string()))?;
        let decoded_metadata = decode_frame_metadata(metadata_prefix)?;
        if decoded_metadata.payload_encoding.is_none() && layout.payload_len() != 0 {
            return Err(UStatus::fail_with_code(
                UCode::InvalidArgument,
                "sample payload is present but payload encoding is absent",
            ));
        }

        let id = read_uuid(&self.id)?;
        let source = read_uri_fields(
            decoded_metadata.source_authority,
            self.source_ue_id,
            self.source_ue_version_major,
            self.source_resource_id,
        )?;
        let sink = if self.sink_present == 0 {
            if decoded_metadata.sink_authority.is_some() {
                return Err(UStatus::fail_with_code(
                    UCode::InvalidArgument,
                    "sink authority metadata present without sink fields",
                ));
            }
            None
        } else {
            let sink_authority = decoded_metadata.sink_authority.ok_or_else(|| {
                UStatus::fail_with_code(UCode::InvalidArgument, "sink authority metadata missing")
            })?;
            Some(read_uri_fields(
                sink_authority,
                self.sink_ue_id,
                self.sink_ue_version_major,
                self.sink_resource_id,
            )?)
        };
        let mut attributes =
            UAttributes::new_unchecked(id, source, sink, byte_to_message_type(self.message_type)?);
        if self.priority_present != 0 {
            attributes.set_priority(byte_to_priority(self.priority)?);
        }
        if self.ttl_present != 0 {
            attributes.set_ttl(self.ttl);
        }
        if self.request_id_present != 0 {
            attributes.set_request_id(read_uuid(&self.request_id)?);
        }
        if self.permission_level_present != 0 {
            attributes.set_permission_level(self.permission_level);
        }
        if self.commstatus_present != 0 {
            let commstatus = UCode::from_i32(self.commstatus).ok_or_else(|| {
                UStatus::fail_with_code(UCode::InvalidArgument, "invalid commstatus")
            })?;
            attributes.set_comm_status(commstatus);
        }
        if self.payload_format_present != 0 {
            let payload_format =
                UPayloadFormat::from_i32(self.payload_format).ok_or_else(|| {
                    UStatus::fail_with_code(UCode::InvalidArgument, "invalid payload format")
                })?;
            attributes.set_payload_format(payload_format);
        }
        if let Some(traceparent) = decoded_metadata.traceparent {
            attributes.set_traceparent(traceparent);
        }
        if let Some(token) = decoded_metadata.token {
            attributes.set_token(token);
        }

        UFrameMetadata::new(attributes, decoded_metadata.payload_encoding).map_err(|error| {
            UStatus::fail_with_code(
                UCode::InvalidArgument,
                format!("invalid frame metadata from iceoryx2 sample: {error}"),
            )
        })
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

    pub(crate) fn payload_len(&self) -> usize {
        self.payload_len
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

pub(crate) fn encode_frame_metadata(
    header: &UFrameMetadata,
) -> Result<Cow<'static, [u8]>, UStatus> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&FRAME_METADATA_MAGIC);
    write_string(&mut bytes, header.attributes().source().authority_name())?;
    write_optional_string(
        &mut bytes,
        header.attributes().sink().map(UUri::authority_name),
    )?;
    write_optional_encoding(&mut bytes, header.payload_encoding())?;
    write_optional_string(&mut bytes, header.attributes().traceparent())?;
    write_optional_string(&mut bytes, header.attributes().token())?;
    Ok(Cow::Owned(bytes))
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

struct DecodedFrameMetadata {
    source_authority: String,
    sink_authority: Option<String>,
    payload_encoding: Option<PayloadEncoding>,
    traceparent: Option<String>,
    token: Option<String>,
}

fn decode_frame_metadata(metadata: &[u8]) -> Result<DecodedFrameMetadata, UStatus> {
    validate_metadata_prefix(metadata)
        .map_err(|error| UStatus::fail_with_code(UCode::InvalidArgument, error.to_string()))?;
    let mut src = metadata
        .get(FRAME_METADATA_MAGIC.len()..)
        .ok_or_else(|| UStatus::fail_with_code(UCode::InvalidArgument, "invalid metadata"))?;
    let source_authority = read_string(&mut src)?;
    let sink_authority = read_optional_string(&mut src)?;
    let payload_encoding = read_optional_encoding(&mut src)?;
    let traceparent = read_optional_string(&mut src)?;
    let token = read_optional_string(&mut src)?;
    if !src.is_empty() {
        return Err(UStatus::fail_with_code(
            UCode::InvalidArgument,
            "trailing frame metadata bytes",
        ));
    }
    Ok(DecodedFrameMetadata {
        source_authority,
        sink_authority,
        payload_encoding,
        traceparent,
        token,
    })
}

fn write_optional_encoding(
    dst: &mut Vec<u8>,
    value: Option<&PayloadEncoding>,
) -> Result<(), UStatus> {
    match value {
        Some(PayloadEncoding::Standard(format)) => {
            dst.push(1);
            dst.push(0);
            dst.extend_from_slice(&format.as_i32().to_le_bytes());
        }
        Some(PayloadEncoding::Custom { id, content_type }) => {
            dst.push(1);
            dst.push(1);
            write_string(dst, id)?;
            write_string(dst, content_type)?;
        }
        None => dst.push(0),
    }
    Ok(())
}

fn write_string(dst: &mut Vec<u8>, value: &str) -> Result<(), UStatus> {
    let len = u32::try_from(value.len()).map_err(|_| {
        UStatus::fail_with_code(UCode::InvalidArgument, "metadata field is too large")
    })?;
    dst.extend_from_slice(&len.to_le_bytes());
    dst.extend_from_slice(value.as_bytes());
    Ok(())
}

fn write_optional_string(dst: &mut Vec<u8>, value: Option<&str>) -> Result<(), UStatus> {
    match value {
        Some(value) => {
            dst.push(1);
            write_string(dst, value)?;
        }
        None => dst.push(0),
    }
    Ok(())
}

fn read_optional_encoding(src: &mut &[u8]) -> Result<Option<PayloadEncoding>, UStatus> {
    match read_u8(src)? {
        0 => Ok(None),
        1 => read_encoding(src).map(Some),
        _ => Err(UStatus::fail_with_code(
            UCode::InvalidArgument,
            "invalid optional metadata field",
        )),
    }
}

fn read_encoding(src: &mut &[u8]) -> Result<PayloadEncoding, UStatus> {
    match read_u8(src)? {
        0 => {
            let value = read_i32(src)?;
            let format = UPayloadFormat::from_i32(value).ok_or_else(|| {
                UStatus::fail_with_code(
                    UCode::InvalidArgument,
                    format!("invalid standard payload format {value}"),
                )
            })?;
            Ok(PayloadEncoding::Standard(format))
        }
        1 => PayloadEncoding::custom(read_string(src)?, read_string(src)?).map_err(|error| {
            UStatus::fail_with_code(
                UCode::InvalidArgument,
                format!("invalid custom payload encoding metadata: {error}"),
            )
        }),
        _ => Err(UStatus::fail_with_code(
            UCode::InvalidArgument,
            "invalid payload encoding kind",
        )),
    }
}

fn read_string(src: &mut &[u8]) -> Result<String, UStatus> {
    let len = usize::try_from(read_u32(src)?).map_err(|_| {
        UStatus::fail_with_code(UCode::InvalidArgument, "metadata length too large")
    })?;
    let bytes = read_bytes(src, len)?;
    String::from_utf8(bytes.to_vec()).map_err(|error| {
        UStatus::fail_with_code(
            UCode::InvalidArgument,
            format!("metadata field is not valid UTF-8: {error}"),
        )
    })
}

fn read_optional_string(src: &mut &[u8]) -> Result<Option<String>, UStatus> {
    match read_u8(src)? {
        0 => Ok(None),
        1 => Ok(Some(read_string(src)?)),
        _ => Err(UStatus::fail_with_code(
            UCode::InvalidArgument,
            "invalid optional metadata field",
        )),
    }
}

fn write_uuid(dst: &mut [u8; 16], uuid: &UUID) {
    let bytes = Vec::<u8>::from(uuid);
    dst.copy_from_slice(&bytes);
}

fn read_uuid(bytes: &[u8; 16]) -> Result<UUID, UStatus> {
    UUID::from_bytes(bytes)
        .map_err(|error| UStatus::fail_with_code(UCode::InvalidArgument, error.to_string()))
}

fn write_uri_fields(uri: &UUri, ue_id: &mut u32, ue_version_major: &mut u8, resource_id: &mut u16) {
    *ue_id = (u32::from(uri.uentity_instance_id()) << 16) | u32::from(uri.uentity_type_id());
    *ue_version_major = uri.uentity_major_version();
    *resource_id = uri.resource_id();
}

fn read_uri_fields(
    authority_name: String,
    ue_id: u32,
    ue_version_major: u8,
    resource_id: u16,
) -> Result<UUri, UStatus> {
    UUri::try_from_parts(&authority_name, ue_id, ue_version_major, resource_id)
        .map_err(|error| UStatus::fail_with_code(UCode::InvalidArgument, error.to_string()))
}

fn message_type_to_byte(message_type: UMessageType) -> u8 {
    match message_type {
        UMessageType::Publish => 1,
        UMessageType::Notification => 2,
        UMessageType::Request => 3,
        UMessageType::Response => 4,
    }
}

fn byte_to_message_type(value: u8) -> Result<UMessageType, UStatus> {
    match value {
        1 => Ok(UMessageType::Publish),
        2 => Ok(UMessageType::Notification),
        3 => Ok(UMessageType::Request),
        4 => Ok(UMessageType::Response),
        _ => Err(UStatus::fail_with_code(
            UCode::InvalidArgument,
            "invalid message type",
        )),
    }
}

fn priority_to_byte(priority: UPriority) -> u8 {
    match priority {
        UPriority::CS0 => 0,
        UPriority::CS1 => 1,
        UPriority::CS2 => 2,
        UPriority::CS3 => 3,
        UPriority::CS4 => 4,
        UPriority::CS5 => 5,
        UPriority::CS6 => 6,
    }
}

fn byte_to_priority(value: u8) -> Result<UPriority, UStatus> {
    match value {
        0 => Ok(UPriority::CS0),
        1 => Ok(UPriority::CS1),
        2 => Ok(UPriority::CS2),
        3 => Ok(UPriority::CS3),
        4 => Ok(UPriority::CS4),
        5 => Ok(UPriority::CS5),
        6 => Ok(UPriority::CS6),
        _ => Err(UStatus::fail_with_code(
            UCode::InvalidArgument,
            "invalid priority",
        )),
    }
}

fn read_i32(src: &mut &[u8]) -> Result<i32, UStatus> {
    let bytes = read_bytes(src, 4)?;
    Ok(i32::from_le_bytes(bytes.try_into().map_err(|_| {
        UStatus::fail_with_code(UCode::InvalidArgument, "invalid metadata integer")
    })?))
}

fn read_u32(src: &mut &[u8]) -> Result<u32, UStatus> {
    let bytes = read_bytes(src, 4)?;
    Ok(u32::from_le_bytes(bytes.try_into().map_err(|_| {
        UStatus::fail_with_code(UCode::InvalidArgument, "invalid metadata length")
    })?))
}

fn read_u8(src: &mut &[u8]) -> Result<u8, UStatus> {
    let (value, remaining) = src
        .split_first()
        .ok_or_else(|| UStatus::fail_with_code(UCode::InvalidArgument, "invalid metadata"))?;
    *src = remaining;
    Ok(*value)
}

fn read_bytes<'a>(src: &mut &'a [u8], len: usize) -> Result<&'a [u8], UStatus> {
    let value = src
        .get(..len)
        .ok_or_else(|| UStatus::fail_with_code(UCode::InvalidArgument, "invalid metadata"))?;
    *src = src
        .get(len..)
        .ok_or_else(|| UStatus::fail_with_code(UCode::InvalidArgument, "invalid metadata"))?;
    Ok(value)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum FrameContractError {
    FieldTooLarge(&'static str),
    InvalidAlignment(usize),
    InvalidMetadataPrefix,
    LayoutOverflow,
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
