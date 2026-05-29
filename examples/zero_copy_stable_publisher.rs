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

use std::str::FromStr;

use up_rust::{
    UFrameMetadata, UUri, payload::StableContainerPayload, zero_copy::UZeroCopyUninitTransportExt,
};
use up_transport_iceoryx2_rust::{MessagingPattern, transport::UTransportIceoryx2};

#[repr(C)]
#[derive(
    Clone, Copy, Debug, Eq, PartialEq, up_rust::StablePayload, up_rust::ByteBackedStablePayload,
)]
#[stable_payload(type_name = "example.vehicle.VehiclePose")]
struct VehiclePose {
    x: u64,
    y: u64,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let topic = UUri::from_str("//my-vehicle/4210/1/9000")?;
    let transport = UTransportIceoryx2::build(MessagingPattern::PublishSubscribe)?;

    for count in 1_u64..=50 {
        let pose = VehiclePose {
            x: count,
            y: count * 10,
        };
        transport
            .send_uninit_loaned_payload_as::<StableContainerPayload<VehiclePose>, VehiclePose>(
                UFrameMetadata::try_publish(topic.clone())?,
                |slot| Ok(slot.write(pose)),
            )
            .await?;
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    Ok(())
}
