use super::*;

pub(crate) fn read_response(
    bytes: &[u8],
    path: &str,
    volume_role: &str,
    work: &NativeMaterializationWork,
) -> Result<(serde_json::Value, NativeAllocation)> {
    use base64::Engine as _;
    use sha2::{Digest as _, Sha256};
    let size = bytes
        .len()
        .checked_add(2)
        .and_then(|size| size.checked_div(3))
        .and_then(|size| size.checked_mul(4))
        .ok_or_else(|| invalid("builtin response size overflow"))?;
    let _encoded = work.claim(size as u64 + 64, 0, acyclic_fs::WorkCounters::UNCHARGED)?;
    work.charge(WorkCounters {
        bytes_hashed: bytes.len() as u64,
        bytes_encoded: bytes.len() as u64,
        ..WorkCounters::default()
    })?;
    let content_base64 = base64::engine::general_purpose::STANDARD.encode(bytes);
    let sha256 = hex::encode(Sha256::digest(bytes));
    #[derive(Serialize)]
    struct Response<'a> {
        content_base64: &'a str,
        sha256: &'a str,
        path: &'a str,
        volume_role: &'a str,
    }
    crate::pi_root_provider::guarded_pi_value(
        &Response {
            content_base64: &content_base64,
            sha256: &sha256,
            path,
            volume_role,
        },
        MAXIMUM_BUILTIN_BYTES * 2,
        work,
    )
}
