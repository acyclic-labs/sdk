#![allow(missing_docs)]

use acyclic_fs::cache::ObjectCacheOptions;
use acyclic_fs::model::VolumeLimits;
use acyclic_fs::{
    DEFAULT_HOSTED_MAXIMUM_PAGE_ITEMS, DEFAULT_HOSTED_MAXIMUM_RESPONSE_BYTES,
    FILESYSTEM_PROTOCOL_VERSION, MAX_BYTE_RESPONSE_ENVELOPE_BYTES,
    MINIMUM_HANDSHAKE_RESPONSE_BYTES,
};
use serde::Serialize;

#[derive(Serialize)]
struct FilesystemDefaults {
    volume_limits: VolumeLimits,
    object_cache_options: ObjectCacheDefaults,
    hosted: HostedDefaults,
}

#[derive(Serialize)]
struct ObjectCacheDefaults {
    maximum_entries: u32,
    maximum_bytes: u64,
    maximum_in_flight: u32,
    maximum_waiters_per_object: u32,
}

impl From<ObjectCacheOptions> for ObjectCacheDefaults {
    fn from(value: ObjectCacheOptions) -> Self {
        Self {
            maximum_entries: value.maximum_entries,
            maximum_bytes: value.maximum_bytes,
            maximum_in_flight: value.maximum_in_flight,
            maximum_waiters_per_object: value.maximum_waiters_per_object,
        }
    }
}

#[derive(Serialize)]
struct HostedDefaults {
    protocol_version: &'static str,
    maximum_response_bytes: u64,
    maximum_page_items: u32,
    maximum_byte_response_envelope_bytes: u64,
    minimum_handshake_response_bytes: u64,
}

fn main() -> Result<(), serde_json::Error> {
    let defaults = FilesystemDefaults {
        volume_limits: VolumeLimits::default(),
        object_cache_options: ObjectCacheOptions::default().into(),
        hosted: HostedDefaults {
            protocol_version: FILESYSTEM_PROTOCOL_VERSION,
            maximum_response_bytes: DEFAULT_HOSTED_MAXIMUM_RESPONSE_BYTES,
            maximum_page_items: DEFAULT_HOSTED_MAXIMUM_PAGE_ITEMS,
            maximum_byte_response_envelope_bytes: MAX_BYTE_RESPONSE_ENVELOPE_BYTES,
            minimum_handshake_response_bytes: MINIMUM_HANDSHAKE_RESPONSE_BYTES as u64,
        },
    };
    println!("{}", serde_json::to_string(&defaults)?);
    Ok(())
}
