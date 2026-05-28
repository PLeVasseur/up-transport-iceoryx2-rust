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
    UCode, UUri,
    zero_copy::{ULoanedContiguousZeroCopyRxFrame, UZeroCopyRxFrame, UZeroCopyTransport},
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

    loop {
        match transport.receive_zero_copy(&topic, None).await {
            Ok(rx) => {
                let pose = rx.borrow_stable_payload::<VehiclePose>()?;
                println!(
                    "received stable shared-memory pose [source: {}, payload provenance: {:?}, pose: {:?}]",
                    rx.metadata().source().to_uri(false),
                    rx.payload_loan_provenance()?,
                    pose
                );
            }
            Err(status) if status.get_code() == UCode::NOT_FOUND => {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            Err(status) => return Err(status.into()),
        }
    }
}
