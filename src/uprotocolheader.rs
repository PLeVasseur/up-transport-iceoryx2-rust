// ################################################################################
// Copyright (c) 2025 Contributors to the Eclipse Foundation
//
// See the NOTICE file(s) distributed with this work for additional
// information regarding copyright ownership.
//
// This program and the accompanying materials are made available under the
// terms of the Apache License Version 2.0 which is available at
// https://www.apache.org/licenses/LICENSE-2.0
//
// SPDX-License-Identifier: Apache-2.0
// ################################################################################

use iceoryx2::prelude::ZeroCopySend;
use up_rust::{
    UAttributes, UCode, UEncoding, UFrameMetadata, UMessageType, UPriority, UStatus, UUID, UUri,
};

const FRAME_METADATA_MAGIC: &[u8; 4] = b"UFM1";

#[repr(C)]
#[derive(ZeroCopySend, Debug, Default)]
pub struct UProtocolHeader {
    pub(crate) uprotocol_major_version: u8,
    pub(crate) id_msb: u64,
    pub(crate) id_lsb: u64,
    pub(crate) message_type: u8,
    pub(crate) priority: u8,
    pub(crate) ttl_present: u8,
    pub(crate) ttl: u32,
    pub(crate) request_id_present: u8,
    pub(crate) request_id_msb: u64,
    pub(crate) request_id_lsb: u64,
    pub(crate) permission_level_present: u8,
    pub(crate) permission_level: u32,
    pub(crate) commstatus_present: u8,
    pub(crate) commstatus: u8,
    pub(crate) source_ue_id: u32,
    pub(crate) source_ue_version_major: u32,
    pub(crate) source_resource_id: u32,
    pub(crate) sink_present: u8,
    pub(crate) sink_ue_id: u32,
    pub(crate) sink_ue_version_major: u32,
    pub(crate) sink_resource_id: u32,
    pub(crate) metadata_len: u64,
    pub(crate) payload_len: u64,
    pub(crate) payload_alignment: u64,
}

impl UProtocolHeader {
    pub(crate) fn write_frame_metadata(
        &mut self,
        header: &UFrameMetadata,
        metadata_len: usize,
        payload_len: usize,
        payload_alignment: usize,
    ) -> Result<(), UStatus> {
        self.uprotocol_major_version = crate::UPROTOCOL_MAJOR_VERSION;
        self.id_msb = header.attributes().id().msb;
        self.id_lsb = header.attributes().id().lsb;
        self.message_type = message_type_to_byte(header.attributes().message_type());
        self.priority = priority_to_byte(header.attributes().priority());
        if let Some(ttl) = header.attributes().ttl() {
            self.ttl_present = 1;
            self.ttl = ttl;
        } else {
            self.ttl_present = 0;
            self.ttl = 0;
        }
        if let Some(request_id) = header.attributes().request_id() {
            self.request_id_present = 1;
            self.request_id_msb = request_id.msb;
            self.request_id_lsb = request_id.lsb;
        } else {
            self.request_id_present = 0;
            self.request_id_msb = 0;
            self.request_id_lsb = 0;
        }
        if let Some(permission_level) = header.attributes().permission_level() {
            self.permission_level_present = 1;
            self.permission_level = permission_level;
        } else {
            self.permission_level_present = 0;
            self.permission_level = 0;
        }
        if let Some(commstatus) = header.attributes().commstatus() {
            self.commstatus_present = 1;
            self.commstatus = commstatus.as_u8();
        } else {
            self.commstatus_present = 0;
            self.commstatus = 0;
        }
        self.metadata_len = u64::try_from(metadata_len).map_err(|_| {
            UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "metadata length exceeds u64")
        })?;
        self.payload_len = u64::try_from(payload_len).map_err(|_| {
            UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "payload length exceeds u64")
        })?;
        self.payload_alignment = u64::try_from(payload_alignment).map_err(|_| {
            UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "payload alignment exceeds u64")
        })?;
        self.source_ue_id = header.attributes().source().ue_id;
        self.source_ue_version_major = header.attributes().source().ue_version_major;
        self.source_resource_id = header.attributes().source().resource_id;
        if let Some(sink) = header.attributes().sink() {
            self.sink_present = 1;
            self.sink_ue_id = sink.ue_id;
            self.sink_ue_version_major = sink.ue_version_major;
            self.sink_resource_id = sink.resource_id;
        } else {
            self.sink_present = 0;
            self.sink_ue_id = 0;
            self.sink_ue_version_major = 0;
            self.sink_resource_id = 0;
        }
        Ok(())
    }

    pub(crate) fn frame_metadata(&self, sample_payload: &[u8]) -> Result<UFrameMetadata, UStatus> {
        let id = UUID::from_u64_pair(self.id_msb, self.id_lsb).map_err(|e| {
            UStatus::fail_with_code(UCode::INVALID_ARGUMENT, format!("invalid UUID: {e}"))
        })?;
        let metadata = self.metadata(sample_payload)?;
        let metadata = FrameMetadata::decode(metadata)?;
        let source = read_uri_fields(
            metadata.source_authority,
            self.source_ue_id,
            self.source_ue_version_major,
            self.source_resource_id,
        )?;
        let sink = if self.sink_present == 0 {
            if metadata.sink_authority.is_some() {
                return Err(UStatus::fail_with_code(
                    UCode::INVALID_ARGUMENT,
                    "sink authority metadata present without sink fields",
                ));
            }
            None
        } else {
            let sink_authority = metadata.sink_authority.ok_or_else(|| {
                UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "sink authority metadata missing")
            })?;
            Some(read_uri_fields(
                sink_authority,
                self.sink_ue_id,
                self.sink_ue_version_major,
                self.sink_resource_id,
            )?)
        };
        let mut attributes =
            UAttributes::new(id, source, sink, byte_to_message_type(self.message_type)?)
                .with_priority(byte_to_priority(self.priority)?);
        if self.ttl_present != 0 {
            attributes = attributes.with_ttl(self.ttl);
        }
        if self.request_id_present != 0 {
            let request_id = UUID::from_u64_pair(self.request_id_msb, self.request_id_lsb)
                .map_err(|e| {
                    UStatus::fail_with_code(UCode::INVALID_ARGUMENT, format!("invalid UUID: {e}"))
                })?;
            attributes = attributes.with_request_id(request_id);
        }
        if self.permission_level_present != 0 {
            attributes = attributes.with_permission_level(self.permission_level);
        }
        if self.commstatus_present != 0 {
            let commstatus = UCode::from_u8(self.commstatus).ok_or_else(|| {
                UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "invalid commstatus")
            })?;
            attributes = attributes.with_commstatus(commstatus);
        }
        if let Some(traceparent) = metadata.traceparent {
            attributes = attributes.with_traceparent(traceparent);
        }
        if let Some(token) = metadata.token {
            attributes = attributes.with_token(token);
        }
        Ok(UFrameMetadata::new(attributes, metadata.encoding))
    }

    pub(crate) fn payload_layout(
        &self,
        sample_payload_len: usize,
    ) -> Result<(usize, usize), UStatus> {
        let metadata_len = usize::try_from(self.metadata_len).map_err(|_| {
            UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "metadata length exceeds usize")
        })?;
        let payload_len = usize::try_from(self.payload_len).map_err(|_| {
            UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "payload length exceeds usize")
        })?;
        let total_len = metadata_len.checked_add(payload_len).ok_or_else(|| {
            UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "sample payload length overflow")
        })?;
        if total_len > sample_payload_len {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                format!(
                    "sample payload too small for metadata and payload: {sample_payload_len} < {total_len}"
                ),
            ));
        }
        Ok((metadata_len, payload_len))
    }

    fn metadata<'a>(&self, sample_payload: &'a [u8]) -> Result<&'a [u8], UStatus> {
        let (metadata_len, _) = self.payload_layout(sample_payload.len())?;
        sample_payload.get(..metadata_len).ok_or_else(|| {
            UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "sample metadata is missing")
        })
    }
}

pub(crate) fn encode_frame_metadata(header: &UFrameMetadata) -> Result<Vec<u8>, UStatus> {
    FrameMetadata {
        source_authority: header.attributes().source().authority_name.clone(),
        sink_authority: header
            .attributes()
            .sink()
            .map(|sink| sink.authority_name.clone()),
        encoding: header.encoding().clone(),
        traceparent: header.attributes().traceparent().map(str::to_owned),
        token: header.attributes().token().map(str::to_owned),
    }
    .encode()
}

struct FrameMetadata {
    source_authority: String,
    sink_authority: Option<String>,
    encoding: UEncoding,
    traceparent: Option<String>,
    token: Option<String>,
}

impl FrameMetadata {
    fn encode(&self) -> Result<Vec<u8>, UStatus> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(FRAME_METADATA_MAGIC);
        write_string(&mut bytes, &self.source_authority)?;
        write_optional_string(&mut bytes, self.sink_authority.as_deref())?;
        write_string(&mut bytes, self.encoding.format_id())?;
        write_string(&mut bytes, self.encoding.content_type())?;
        write_optional_string(&mut bytes, self.encoding.schema_ref())?;
        write_optional_string(&mut bytes, self.traceparent.as_deref())?;
        write_optional_string(&mut bytes, self.token.as_deref())?;
        Ok(bytes)
    }

    fn decode(mut bytes: &[u8]) -> Result<Self, UStatus> {
        let magic = read_bytes(&mut bytes, FRAME_METADATA_MAGIC.len())?;
        if magic != FRAME_METADATA_MAGIC {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "invalid frame metadata",
            ));
        }
        let source_authority = read_string(&mut bytes)?;
        let sink_authority = read_optional_string(&mut bytes)?;
        let format_id = read_string(&mut bytes)?;
        let content_type = read_string(&mut bytes)?;
        let schema_ref = read_optional_string(&mut bytes)?;
        let traceparent = read_optional_string(&mut bytes)?;
        let token = read_optional_string(&mut bytes)?;
        if !bytes.is_empty() {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "trailing frame metadata bytes",
            ));
        }
        Ok(Self {
            source_authority,
            sink_authority,
            encoding: UEncoding::new(format_id, content_type, schema_ref),
            traceparent,
            token,
        })
    }
}

fn read_uri_fields(
    authority_name: String,
    ue_id: u32,
    ue_version_major: u32,
    resource_id: u32,
) -> Result<UUri, UStatus> {
    let uri = UUri {
        authority_name,
        ue_id,
        ue_version_major,
        resource_id,
    };
    uri.check_validity()
        .map_err(|e| UStatus::fail_with_code(UCode::INVALID_ARGUMENT, e.to_string()))?;
    Ok(uri)
}

fn write_string(dst: &mut Vec<u8>, value: &str) -> Result<(), UStatus> {
    let len = u32::try_from(value.len()).map_err(|_| {
        UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "metadata field is too large")
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

fn read_string(src: &mut &[u8]) -> Result<String, UStatus> {
    let len_bytes = read_bytes(src, 4)?;
    let len = usize::try_from(u32::from_le_bytes(len_bytes.try_into().map_err(|_| {
        UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "invalid metadata string length")
    })?))
    .map_err(|_| UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "metadata length too large"))?;
    let bytes = read_bytes(src, len)?;
    String::from_utf8(bytes.to_vec()).map_err(|e| {
        UStatus::fail_with_code(
            UCode::INVALID_ARGUMENT,
            format!("metadata field is not valid UTF-8: {e}"),
        )
    })
}

fn read_optional_string(src: &mut &[u8]) -> Result<Option<String>, UStatus> {
    match read_u8(src)? {
        0 => Ok(None),
        1 => Ok(Some(read_string(src)?)),
        _ => Err(UStatus::fail_with_code(
            UCode::INVALID_ARGUMENT,
            "invalid optional metadata field",
        )),
    }
}

fn read_u8(src: &mut &[u8]) -> Result<u8, UStatus> {
    let (value, remaining) = src
        .split_first()
        .ok_or_else(|| UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "invalid metadata"))?;
    *src = remaining;
    Ok(*value)
}

fn read_bytes<'a>(src: &mut &'a [u8], len: usize) -> Result<&'a [u8], UStatus> {
    let value = src
        .get(..len)
        .ok_or_else(|| UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "invalid metadata"))?;
    *src = src
        .get(len..)
        .ok_or_else(|| UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "invalid metadata"))?;
    Ok(value)
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
            UCode::INVALID_ARGUMENT,
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
            UCode::INVALID_ARGUMENT,
            "invalid priority",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_metadata_round_trips_authorities_via_prefix() {
        let source = UUri::try_from_parts(&"a".repeat(128), 0x4210, 1, 0x8001).unwrap();
        let sink = UUri::try_from_parts(&"b".repeat(128), 0x4210, 1, 0).unwrap();
        let attributes = UAttributes::new(
            UUID::build(),
            source.clone(),
            Some(sink.clone()),
            UMessageType::Notification,
        )
        .with_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00")
        .with_token("test-token");
        let metadata = UFrameMetadata::new(
            attributes,
            UEncoding::new("json", "application/json", Some("schema://reading")),
        );
        let prefix = encode_frame_metadata(&metadata).unwrap();
        let mut user_header = UProtocolHeader::default();

        user_header
            .write_frame_metadata(&metadata, prefix.len(), 0, 1)
            .unwrap();

        let decoded = user_header.frame_metadata(&prefix).unwrap();
        assert_eq!(decoded.attributes().source(), &source);
        assert_eq!(decoded.attributes().sink(), Some(&sink));
        assert_eq!(
            decoded.attributes().traceparent(),
            Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00")
        );
        assert_eq!(decoded.attributes().token(), Some("test-token"));
        assert_eq!(decoded.encoding(), metadata.encoding());
    }
}
