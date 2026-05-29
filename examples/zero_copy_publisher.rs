// ################################################################################
// Copyright (c) 2026 Contributors to the Eclipse Foundation
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

use std::str::FromStr;

use up_rust::{UFrameMetadata, UUri, payload::RawBytes, zero_copy::UZeroCopyTransportExt};
use up_transport_iceoryx2_rust::{MessagingPattern, transport::UTransportIceoryx2};

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let topic = UUri::from_str("//my-vehicle/4210/1/9000")?;
    let transport = UTransportIceoryx2::build(MessagingPattern::PublishSubscribe)?;
    let payload = b"shared-memory payload";

    for _ in 0..50 {
        transport
            .send_serialized_zero_copy::<RawBytes, _>(
                UFrameMetadata::try_publish(topic.clone())?,
                &&payload[..],
            )
            .await?;
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    Ok(())
}
