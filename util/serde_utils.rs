// Copyright 2022 Nathan (Blaise) Bruer.  All rights reserved.
use std::marker::PhantomData;
use std::str::FromStr;
use std::fmt;

use serde::{de, Deserializer, Deserialize};
use shellexpand;

/// Helper for serde macro so you can use shellexpand variables in the json configuration
/// files when the number is a numeric type.
pub fn convert_numeric_with_shellexpand<'de, D, T, E>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    E: fmt::Display,
    T: TryFrom<i64> + FromStr<Err=E>,
    <T as TryFrom<i64>>::Error: fmt::Display
{
    // define a visitor that deserializes
    // `ActualData` encoded as json within a string
    struct USizeVisitor<T: TryFrom<i64>>(PhantomData::<T>);

    impl<'de, T, FromStrErr> de::Visitor<'de> for USizeVisitor<T>
    where
        FromStrErr: fmt::Display,
        T: TryFrom<i64> + FromStr<Err=FromStrErr>,
        <T as TryFrom<i64>>::Error: fmt::Display,
     {
        type Value = T;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a string containing json data")
        }

        fn visit_i64<E: de::Error>(self, v: i64) -> Result<Self::Value, E> {
            v.try_into().map_err(de::Error::custom)
        }

        fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
            (*shellexpand::env(v).map_err(de::Error::custom)?)
                .parse::<T>()
                .map_err(de::Error::custom)
        }
    }

    deserializer.deserialize_any(USizeVisitor::<T>(PhantomData::<T>{}))
}

/// Helper for serde macro so you can use shellexpand variables in the json configuration
/// files when the number is a numeric type.
pub fn convert_string_with_shellexpand<'de, D: Deserializer<'de>>(deserializer: D) -> Result<String, D::Error>
{
    let value = String::deserialize(deserializer)?;
    Ok((*(shellexpand::env(&value).map_err(de::Error::custom)?)).to_string())
}
