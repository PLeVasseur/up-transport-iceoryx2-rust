// ################################################################################
// Copyright (c) 2026 Contributors to the Eclipse Foundation
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

use iceoryx2::prelude::{AttributeVerifier, SemanticString};
use iceoryx2::service::attribute::{AttributeKey, AttributeSet, AttributeValue};
use iceoryx2_bb_container::string::String as IoxString;
use up_rust::{UCode, UStatus, UUri};

use crate::service_name_mapping::{encode_hex, get_authority_name};

const ATTR_TRANSPORT: &str = "up.transport";
const ATTR_SOURCE_AUTHORITY: &str = "up.source.authority";
const ATTR_SOURCE_TYPE: &str = "up.source.type";
const ATTR_SOURCE_INSTANCE: &str = "up.source.instance";
const ATTR_SOURCE_VERSION: &str = "up.source.version";
const ATTR_SOURCE_RESOURCE: &str = "up.source.resource";

const TRANSPORT_VALUE: &str = "uprotocol";

pub(crate) fn source_attribute_verifier(source: &UUri) -> Result<AttributeVerifier, UStatus> {
    let mut verifier = AttributeVerifier::new();
    for (key, value) in source_attribute_pairs(source)? {
        verifier = verifier.require(&key, &value).map_err(|error| {
            UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                format!("invalid iceoryx2 service attribute verifier requirement: {error}"),
            )
        })?;
    }
    Ok(verifier)
}

pub(crate) fn attributes_match_source_filter(attributes: &AttributeSet, filter: &UUri) -> bool {
    let Some(source) = source_from_attributes(attributes) else {
        return false;
    };
    normalized_filter(filter).is_some_and(|filter| filter.matches(&source))
}

fn source_attribute_pairs(source: &UUri) -> Result<Vec<(AttributeKey, AttributeValue)>, UStatus> {
    let authority = get_authority_name(source)?;
    Ok(vec![
        attribute(ATTR_TRANSPORT, TRANSPORT_VALUE)?,
        attribute(ATTR_SOURCE_AUTHORITY, &authority)?,
        attribute(
            ATTR_SOURCE_TYPE,
            &encode_hex(source.uentity_type_id() as u32),
        )?,
        attribute(
            ATTR_SOURCE_INSTANCE,
            &encode_hex(source.uentity_instance_id() as u32),
        )?,
        attribute(
            ATTR_SOURCE_VERSION,
            &encode_hex(source.uentity_major_version() as u32),
        )?,
        attribute(
            ATTR_SOURCE_RESOURCE,
            &encode_hex(source.resource_id() as u32),
        )?,
    ])
}

fn attribute(key: &str, value: &str) -> Result<(AttributeKey, AttributeValue), UStatus> {
    let key = key.try_into().map_err(|error| {
        UStatus::fail_with_code(
            UCode::INVALID_ARGUMENT,
            format!("invalid iceoryx2 service attribute key {key}: {error}"),
        )
    })?;
    let value = value.try_into().map_err(|error| {
        UStatus::fail_with_code(
            UCode::INVALID_ARGUMENT,
            format!("invalid iceoryx2 service attribute value {value}: {error}"),
        )
    })?;
    Ok((key, value))
}

fn source_from_attributes(attributes: &AttributeSet) -> Option<UUri> {
    if attribute_value(attributes, ATTR_TRANSPORT)? != TRANSPORT_VALUE {
        return None;
    }

    let authority = attribute_value(attributes, ATTR_SOURCE_AUTHORITY)?;
    let entity_type = parse_hex_u16(attribute_value(attributes, ATTR_SOURCE_TYPE)?)?;
    let entity_instance = parse_hex_u16(attribute_value(attributes, ATTR_SOURCE_INSTANCE)?)?;
    let version = parse_hex_u8(attribute_value(attributes, ATTR_SOURCE_VERSION)?)?;
    let resource = parse_hex_u16(attribute_value(attributes, ATTR_SOURCE_RESOURCE)?)?;
    let entity_id = ((entity_instance as u32) << 16) | entity_type as u32;
    UUri::try_from_parts(authority, entity_id, version, resource).ok()
}

fn normalized_filter(filter: &UUri) -> Option<UUri> {
    if filter.has_empty_authority() {
        Some(UUri::from_parts_unchecked(
            get_authority_name(filter).ok()?,
            filter.ue_id(),
            filter.uentity_major_version() as u32,
            filter.resource_id() as u32,
        ))
    } else {
        Some(filter.clone())
    }
}

fn attribute_value<'a>(attributes: &'a AttributeSet, key: &str) -> Option<&'a str> {
    let key: AttributeKey = key.try_into().ok()?;
    Some(attributes.key_value(&key, 0)?.as_string().as_str())
}

fn parse_hex_u16(value: &str) -> Option<u16> {
    u16::from_str_radix(value, 16).ok()
}

fn parse_hex_u8(value: &str) -> Option<u8> {
    u8::from_str_radix(value, 16).ok()
}
