// ################################################################################
// Copyright (c) 2026 Contributors to the Eclipse Foundation
//
// SPDX-License-Identifier: Apache-2.0
// ################################################################################

use iceoryx2::prelude::{MessagingPattern, ServiceName};
use up_rust::{ExactUUri, UCode, UMessageType, UStatus, UUri};

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
    sink: Option<&UUri>,
    messaging_pattern: MessagingPattern,
) -> Result<UMessageType, UStatus> {
    if messaging_pattern == MessagingPattern::PublishSubscribe {
        return match sink {
            Some(sink) if source.is_rpc_response() && sink.is_rpc_method() => {
                Ok(UMessageType::Request)
            }
            Some(sink) if source.is_rpc_method() && sink.is_rpc_response() => {
                Ok(UMessageType::Response)
            }
            Some(sink) if source.is_event() && sink.is_notification_destination() => {
                Ok(UMessageType::Notification)
            }
            None if !source.authority_name().is_empty() => Ok(UMessageType::Publish),
            _ => Err(UStatus::fail_with_code(
                UCode::InvalidArgument,
                "could not determine a valid UMessageType from the provided UUri(s)",
            )),
        };
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

pub(crate) fn compute_exact_source_publish_subscribe_service_name(
    source: &ExactUUri,
) -> Result<ServiceName, UStatus> {
    compute_service_name(source.as_uuri(), None, MessagingPattern::PublishSubscribe)
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

    #[test]
    fn request_service_name_uses_sink_method_uri() {
        let reply_to = test_uri("client", 0, 0x10ab, 3, 0x0000);
        let method = test_uri("service", 0, 0x20bc, 1, 0x1000);
        let name =
            compute_service_name(&reply_to, Some(&method), MessagingPattern::PublishSubscribe)
                .unwrap();
        assert_eq!(name.as_str(), "up/service/20BC/0/1/1000");
    }

    #[test]
    fn response_service_name_uses_source_and_sink_uri() {
        let method = test_uri("service", 0, 0x20bc, 1, 0x1000);
        let reply_to = test_uri("client", 0, 0x10ab, 3, 0x0000);
        let name =
            compute_service_name(&method, Some(&reply_to), MessagingPattern::PublishSubscribe)
                .unwrap();
        assert_eq!(name.as_str(), "up/service/20BC/0/1/1000/client/10AB/0/3/0");
    }

    #[test]
    fn notification_service_name_uses_source_and_sink_uri() {
        let source = test_uri("device1", 0, 0x10ab, 3, 0x8000);
        let sink = test_uri("client", 0, 0x20bc, 1, 0x0000);
        let name =
            compute_service_name(&source, Some(&sink), MessagingPattern::PublishSubscribe).unwrap();
        assert_eq!(name.as_str(), "up/device1/10AB/0/3/8000/client/20BC/0/1/0");
    }

    #[test]
    fn exact_publish_service_name_requires_exact_source_proof() {
        let source = ExactUUri::try_from(test_uri("device1", 0, 0x10ab, 3, 0x7fff)).unwrap();
        let name = compute_exact_source_publish_subscribe_service_name(&source).unwrap();
        assert_eq!(name.as_str(), "up/device1/10AB/0/3/7FFF");
    }

    #[test]
    fn exact_publish_service_name_rejects_wildcard_source_before_mapping() {
        let wildcard = UUri::try_from_parts("device1", 0x10ab, 3, 0xffff).unwrap();
        assert!(ExactUUri::try_from(wildcard).is_err());
    }
}
