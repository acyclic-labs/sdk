//! The document and its directory index must describe the same occurrence.
use crate::agent::IntentId;

pub(super) fn matches(path: &str, directory: &str, due: Option<u64>, id: &IntentId) -> bool {
    let Some(name) = path.strip_prefix(directory) else {
        return false;
    };
    let name = if let Some(due) = due {
        let Some((time, name)) = name.split_once('-') else {
            return false;
        };
        if time.len() != 20
            || !time.bytes().all(|byte| byte.is_ascii_digit())
            || time.parse::<u64>().ok() != Some(due)
        {
            return false;
        }
        name
    } else {
        name
    };
    let digest = blake3::hash(id.as_str().as_bytes()).to_hex();
    name.strip_suffix(".json") == Some(digest.as_str())
}
