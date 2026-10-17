use super::signed::NOW;
use acyclic_auth::{authority, contract::*};

/// Reads `fs:/` resources as the signed fixture's account at its clock.
pub fn request<'a>(
    resource: &'a str,
    attributes: &'a RecordAttributes,
    usage: &'a Limits,
) -> authority::Request<'a> {
    authority::Request {
        target: authority::Target::Resource {
            action: "fs/read",
            resource,
        },
        audience: "workers:/test",
        holder: Some("account"),
        now_unix_seconds: NOW,
        keys: &[],
        attributes,
        usage,
        cpu_millis: 0,
        memory_bytes: 0,
    }
}
