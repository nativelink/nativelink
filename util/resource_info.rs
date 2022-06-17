// Copyright 2022 Nathan (Blaise) Bruer.  All rights reserved.

use error::{error_if, Error, ResultExt};

// Useful utility struct for converting bazel's (uri-like path) into it's parts.
pub struct ResourceInfo<'a> {
    pub instance_name: &'a str,
    pub uuid: Option<&'a str>,
    pub hash: &'a str,
    pub expected_size: usize,
}

impl<'a> ResourceInfo<'a> {
    pub fn new(resource_name: &'a str) -> Result<ResourceInfo<'a>, Error> {
        let mut parts = resource_name.splitn(6, '/');
        const ERROR_MSG: &str = concat!(
            "Expected resource_name to be of pattern ",
            "'{instance_name}/uploads/{uuid}/blobs/{hash}/{size}' or ",
            "'{instance_name}/blobs/{hash}/{size}'",
        );
        let instance_name = &parts.next().err_tip(|| ERROR_MSG)?;
        let mut blobs_or_uploads: &str = parts.next().err_tip(|| ERROR_MSG)?;
        let mut uuid = None;
        if &blobs_or_uploads == &"uploads" {
            uuid = Some(parts.next().err_tip(|| ERROR_MSG)?);
            blobs_or_uploads = parts.next().err_tip(|| ERROR_MSG)?;
        }

        error_if!(
            &blobs_or_uploads != &"blobs",
            "Element 2 or 4 of resource_name should have been 'blobs'. Got: {}",
            blobs_or_uploads
        );
        let hash = &parts.next().err_tip(|| ERROR_MSG)?;
        let raw_digest_size = parts.next().err_tip(|| ERROR_MSG)?;
        let expected_size = raw_digest_size.parse::<usize>().err_tip(|| {
            format!(
                "Digest size_bytes was not convertible to usize. Got: {}",
                raw_digest_size
            )
        })?;
        Ok(ResourceInfo {
            instance_name: instance_name,
            uuid,
            hash,
            expected_size,
        })
    }
}
