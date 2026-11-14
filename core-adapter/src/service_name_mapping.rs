// Copyright (c) 2026 Contributors to the Eclipse Foundation
// SPDX-License-Identifier: Apache-2.0

use iceoryx2::prelude::{MessagingPattern, ServiceName};
use up_rust_core::{transport::BindingRefusal, uri::UUri};

pub(crate) fn get_authority_name(uri: &UUri) -> String {
    if uri.has_empty_authority() {
        hostname::get()
            .ok()
            .and_then(|name| name.into_string().ok())
            .unwrap_or_else(|| "unknown".into())
    } else {
        uri.authority_name().into()
    }
}

fn segments(uri: &UUri) -> String {
    format!(
        "{}/{:X}/{:X}/{:X}/{:X}",
        get_authority_name(uri),
        uri.ue_type_id(),
        uri.ue_instance_id(),
        uri.uentity_major_version(),
        uri.resource_id()
    )
}

pub(crate) fn request_service_name(method: &UUri) -> Result<ServiceName, BindingRefusal> {
    ServiceName::new(&format!("up/{}", segments(method)))
        .map_err(|_| BindingRefusal("invalid native service name"))
}

pub(crate) fn compute_service_name(
    source: &UUri,
    sink: Option<&UUri>,
    pattern: MessagingPattern,
) -> Result<ServiceName, BindingRefusal> {
    if pattern != MessagingPattern::PublishSubscribe {
        return Err(BindingRefusal(
            "only publish-subscribe native services are supported",
        ));
    }
    let name = match sink {
        Some(sink) if source.is_rpc_response() && sink.is_rpc_method() => {
            return request_service_name(sink);
        }
        Some(sink)
            if (source.is_rpc_method() && sink.is_rpc_response())
                || (source.is_event() && sink.is_notification_destination()) =>
        {
            format!("up/{}/{}", segments(source), segments(sink))
        }
        None if !source.has_empty_authority() => format!("up/{}", segments(source)),
        _ => return Err(BindingRefusal("invalid source and sink for native service")),
    };
    ServiceName::new(&name).map_err(|_| BindingRefusal("invalid native service name"))
}
