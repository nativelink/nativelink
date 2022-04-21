// Copyright 2022 Nathan (Blaise) Bruer.  All rights reserved.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Cursor};
use std::process::Stdio;
use std::str::from_utf8;

use futures::future::try_join_all;
use tokio::process;

use common::log;
use config::cas_server::WrokerProperty;
use error::{make_err, make_input_err, Error, ResultExt};
use proto::build::bazel::remote::execution::v2::platform::Property;
use proto::com::github::allada::turbo_cache::remote_execution::SupportedProperties;

pub async fn make_supported_properties(
    worker_properties: &HashMap<String, WrokerProperty>,
) -> Result<SupportedProperties, Error> {
    let mut futures = vec![];
    for (property_name, worker_property) in worker_properties {
        futures.push(async move {
            match worker_property {
                WrokerProperty::values(values) => {
                    let mut props = Vec::with_capacity(values.len());
                    for value in values {
                        props.push(Property {
                            name: property_name.clone(),
                            value: value.clone(),
                        });
                    }
                    return Ok(props);
                }
                WrokerProperty::query_cmd(cmd) => {
                    let maybe_split_cmd = shlex::split(&cmd);
                    let (command, args) = match &maybe_split_cmd {
                        Some(split_cmd) => (&split_cmd[0], &split_cmd[1..]),
                        None => {
                            return Err(make_input_err!(
                                "Could not parse the value of worker property: {}: '{}'",
                                property_name,
                                cmd
                            ))
                        }
                    };
                    let mut process = process::Command::new(command);
                    process.env_clear();
                    process.args(args);
                    process.stdin(Stdio::null());
                    let err_fn = || format!("Error executing property_name {} command", property_name);
                    log::info!("Spawning process for cmd: '{}' for property: '{}'", cmd, property_name);
                    let process_output = process.output().await.err_tip(err_fn)?;
                    if !process_output.status.success() {
                        return Err(make_err!(process_output.status.code().unwrap().into(), "{}", err_fn()));
                    }
                    if !process_output.stderr.is_empty() {
                        eprintln!(
                            "{}",
                            from_utf8(&process_output.stderr)
                                .map_err(|e| make_input_err!("Failed to decode stderr to utf8 : {:?}", e))?
                        );
                    }
                    let reader = BufReader::new(Cursor::new(process_output.stdout));

                    let mut props = vec![];
                    for value in reader.lines() {
                        props.push(Property {
                            name: property_name.clone(),
                            value: value.err_tip(|| "Could split input by lines")?.clone(),
                        });
                    }
                    return Ok(props);
                }
            }
        });
    }

    Ok(SupportedProperties {
        properties: try_join_all(futures).await?.into_iter().flatten().collect(),
    })
}
