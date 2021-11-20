// Copyright 2021 Nathan (Blaise) Bruer.  All rights reserved.

use std::collections::HashMap;

use config::cas_server::PropertyType;
use error::{make_input_err, Code, Error, ResultExt};

pub enum PlatformPropertyValue {
    Minimum(u64),
    Exact(String),
}

/// Helps manage known properties and conversion into PlatformPropertyValue.
pub struct PlatformPropertyManager {
    known_properties: HashMap<String, PropertyType>,
}

impl PlatformPropertyManager {
    pub fn new(known_properties: HashMap<String, PropertyType>) -> Self {
        Self { known_properties }
    }

    pub fn make_prop_value(&self, key: &str, value: &str) -> Result<PlatformPropertyValue, Error> {
        if let Some(prop_type) = self.known_properties.get(key) {
            return match prop_type {
                PropertyType::Minimum => Ok(PlatformPropertyValue::Minimum(
                    u64::from_str_radix(value, 10).err_tip_with_code(|e| {
                        (
                            Code::InvalidArgument,
                            format!("Cannot convert to platform property to u64: {} - {}", value, e),
                        )
                    })?,
                )),
                PropertyType::Exact => Ok(PlatformPropertyValue::Exact(value.to_string())),
            };
        }
        Err(make_input_err!("Unknown platform property '{}'", key))
    }
}
