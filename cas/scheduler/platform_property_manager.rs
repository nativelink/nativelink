// Copyright 2021 Nathan (Blaise) Bruer.  All rights reserved.

use std::collections::HashMap;

use config::cas_server::PropertyType;
use error::{make_input_err, Code, Error, ResultExt};

/// `PlatformProperties` helps manage the configuration of platform properties to
/// keys and types. The scheduler uses these properties to decide what jobs
/// can be assigned to different workers. For example, if a job states it needs
/// a specific key, it will never be run on a worker that does not have at least
/// all the platform property keys configured on the worker.
///
/// Additional rules may be applied based on `PlatfromPropertyValue`.
#[derive(Eq, PartialEq, Clone, Debug, Default)]
pub struct PlatformProperties {
    pub properties: HashMap<String, PlatformPropertyValue>,
}

impl PlatformProperties {
    pub fn new(map: HashMap<String, PlatformPropertyValue>) -> Self {
        Self { properties: map }
    }

    /// Determines if the worker's `PlatformProperties` is satisfied by this struct.
    pub fn is_satisfied_by(&self, worker_properties: &PlatformProperties) -> bool {
        for (property, check_value) in &self.properties {
            if let Some(worker_value) = worker_properties.properties.get(property) {
                if !check_value.is_satisfied_by(&worker_value) {
                    return false;
                }
            } else {
                return false;
            }
        }
        return true;
    }
}

/// Holds the associated value of the key and type.
///
/// Exact    - Means the worker must have this exact value.
/// Minimum  - Means that workers must have at least this number available. When
///            a worker executes a task that has this value, the worker will have
///            this value subtracted from the available resources of the worker.
/// Metadata - Means the worker is given this information, but does not restrict
///            what workers can take this value. However, the worker must have the
///            associated key present to be matched.
#[derive(Eq, PartialEq, Hash, Clone, Ord, PartialOrd, Debug)]
pub enum PlatformPropertyValue {
    Exact(String),
    Minimum(u64),
    Metadata(String),
}

impl PlatformPropertyValue {
    /// Same as `PlatformProperties::is_satisfied_by`, but on an individual value.
    pub fn is_satisfied_by(&self, worker_value: &PlatformPropertyValue) -> bool {
        if self == worker_value {
            return true;
        }
        match self {
            PlatformPropertyValue::Minimum(v) => {
                if let PlatformPropertyValue::Minimum(worker_v) = worker_value {
                    return worker_v >= v;
                }
                false
            }
            // Metadata is used to pass info to the worker and not restrict which
            // workers can be selected.
            PlatformPropertyValue::Metadata(_) => true,
            // Success exact case is handled above.
            PlatformPropertyValue::Exact(_) => false,
        }
    }
}

/// Helps manage known properties and conversion into PlatformPropertyValue.
pub struct PlatformPropertyManager {
    known_properties: HashMap<String, PropertyType>,
}

impl PlatformPropertyManager {
    pub fn new(known_properties: HashMap<String, PropertyType>) -> Self {
        Self { known_properties }
    }

    /// Returns the `known_properties` map.
    pub fn get_known_properties(&self) -> &HashMap<String, PropertyType> {
        &self.known_properties
    }

    /// Given a specific key and value, returns the translated `PlatformPropertyValue`. This will
    /// automatically convert any strings to the type-value pairs of `PlatformPropertyValue` based
    /// on the configuration passed into the `PlatformPropertyManager` constructor.
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
                PropertyType::Metadata => Ok(PlatformPropertyValue::Metadata(value.to_string())),
            };
        }
        Err(make_input_err!("Unknown platform property '{}'", key))
    }
}
