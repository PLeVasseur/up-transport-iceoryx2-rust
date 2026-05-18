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

use up_rust::{
    UCode, UUri,
    payload::RawBytes,
    zero_copy::{UContiguousZeroCopyRxFrame, UZeroCopyTransport},
};
use up_transport_iceoryx2_rust::{MessagingPattern, transport::UTransportIceoryx2};

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let topic = UUri::from_str("//my-vehicle/4210/1/9000")?;
    let transport = UTransportIceoryx2::build(MessagingPattern::PublishSubscribe)?;

    for _ in 0..100 {
        match transport.receive_zero_copy(&topic, None).await {
            Ok(rx) => {
                let payload: &[u8] = rx.deserialize_borrowed::<RawBytes, _>()?;
                println!(
                    "received {} borrowed bytes: {}",
                    payload.len(),
                    String::from_utf8_lossy(payload)
                );
                return Ok(());
            }
            Err(status) if status.get_code() == UCode::NOT_FOUND => {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            Err(status) => return Err(status.into()),
        }
    }

    Err("timed out waiting for a zero-copy sample".into())
}
