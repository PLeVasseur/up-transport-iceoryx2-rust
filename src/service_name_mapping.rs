// ################################################################################
// Copyright (c) 2026 Contributors to the Eclipse Foundation
//
// SPDX-License-Identifier: Apache-2.0
// ################################################################################

use iceoryx2::prelude::{MessagingPattern, ServiceName};
use up_rust::{UCode, UMessageType, UStatus, UUri};

fn encode_uuri_segments(uuri: &UUri) -> Vec<String> {
    vec![
        get_authority_name(uuri),
        encode_hex(uuri.uentity_type_id() as u32),
        encode_hex(uuri.uentity_instance_id() as u32),
        encode_hex(uuri.uentity_major_version() as u32),
        encode_hex(uuri.resource_id() as u32),
    ]
}

pub(crate) fn encode_hex(value: u32) -> String {
    format!("{value:X}")
}

pub(crate) fn get_authority_name(source_uuri: &UUri) -> String {
    if source_uuri.authority_name().is_empty() {
        hostname::get()
            .ok()
            .and_then(|hostname| hostname.into_string().ok())
            .unwrap_or_else(|| "unknown".to_string())
    } else {
        source_uuri.authority_name().to_string()
    }
}

fn determine_message_type(
    source: &UUri,
    _sink: Option<&UUri>,
    messaging_pattern: MessagingPattern,
) -> Result<UMessageType, UStatus> {
    if !source.authority_name().is_empty()
        && messaging_pattern == MessagingPattern::PublishSubscribe
    {
        return Ok(UMessageType::Publish);
    }

    Err(UStatus::fail_with_code(
        UCode::InvalidArgument,
        "could not determine a valid UMessageType from the provided UUri(s)",
    ))
}

pub(crate) fn compute_service_name(
    source: &UUri,
    sink: Option<&UUri>,
    messaging_pattern: MessagingPattern,
) -> Result<ServiceName, UStatus> {
    let join_segments = |segments: Vec<String>| segments.join("/");
    let message_type = determine_message_type(source, sink, messaging_pattern)?;
    let service_name = match message_type {
        UMessageType::Request => {
            let Some(sink) = sink else {
                return Err(UStatus::fail_with_code(
                    UCode::InvalidArgument,
                    "sink required for request service name",
                ));
            };
            format!("up/{}", join_segments(encode_uuri_segments(sink)))
        }
        UMessageType::Response | UMessageType::Notification => {
            let Some(sink) = sink else {
                return Err(UStatus::fail_with_code(
                    UCode::InvalidArgument,
                    "sink required for response or notification service name",
                ));
            };
            format!(
                "up/{}/{}",
                join_segments(encode_uuri_segments(source)),
                join_segments(encode_uuri_segments(sink))
            )
        }
        UMessageType::Publish => format!("up/{}", join_segments(encode_uuri_segments(source))),
    };
    ServiceName::new(service_name.as_str()).map_err(|error| {
        UStatus::fail_with_code(
            UCode::InvalidArgument,
            format!("invalid iceoryx2 service name {service_name}: {error}"),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_uri(authority: &str, instance: u16, typ: u16, version: u8, resource: u16) -> UUri {
        let entity_id = ((instance as u32) << 16) | (typ as u32);
        UUri::try_from_parts(authority, entity_id, version, resource).unwrap()
    }

    #[test]
    fn publish_service_name_uses_source_uri() {
        let source = test_uri("device1", 0, 0x10ab, 3, 0x7fff);
        let name = compute_service_name(&source, None, MessagingPattern::PublishSubscribe).unwrap();
        assert_eq!(name.as_str(), "up/device1/10AB/0/3/7FFF");
    }
}
