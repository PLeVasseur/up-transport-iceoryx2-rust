// Copyright (c) 2026 Contributors to the Eclipse Foundation
// SPDX-License-Identifier: Apache-2.0

use iceoryx2::{
    prelude::AttributeVerifier,
    service::attribute::{AttributeKey, AttributeValue},
};
use up_rust_core::{transport::BindingRefusal, uri::UUri};

fn verifier(pairs: &[(&str, String)]) -> Result<AttributeVerifier, BindingRefusal> {
    let mut verifier = AttributeVerifier::new();
    for (key, value) in pairs {
        let key: AttributeKey = (*key)
            .try_into()
            .map_err(|_| BindingRefusal("invalid attribute key"))?;
        let value: AttributeValue = value
            .as_str()
            .try_into()
            .map_err(|_| BindingRefusal("invalid attribute value"))?;
        verifier = verifier
            .require(&key, &value)
            .map_err(|_| BindingRefusal("invalid attribute requirement"))?;
    }
    Ok(verifier)
}

pub(crate) fn shared_request_attribute_verifier() -> Result<AttributeVerifier, BindingRefusal> {
    verifier(&[
        ("up.transport", "uprotocol".into()),
        ("up.request.shared", "1".into()),
    ])
}

pub(crate) fn source_attribute_verifier(
    source: &UUri,
) -> Result<AttributeVerifier, BindingRefusal> {
    if source.is_rpc_response() {
        return shared_request_attribute_verifier();
    }
    verifier(&[
        ("up.transport", "uprotocol".into()),
        (
            "up.source.authority",
            crate::service_name_mapping::get_authority_name(source),
        ),
        ("up.source.type", format!("{:X}", source.ue_type_id())),
        (
            "up.source.instance",
            format!("{:X}", source.ue_instance_id()),
        ),
        (
            "up.source.version",
            format!("{:X}", source.uentity_major_version()),
        ),
        ("up.source.resource", format!("{:X}", source.resource_id())),
    ])
}
