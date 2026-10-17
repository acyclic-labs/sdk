use super::*;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Header {
    #[serde(rename = "type")]
    pub kind: String,
    pub version: u32,
    pub id: String,
    pub timestamp: String,
    pub cwd: String,
    #[serde(
        default,
        rename = "parentSession",
        skip_serializing_if = "Option::is_none"
    )]
    pub parent_session: Option<String>,
}

impl Header {
    pub fn validate(&self) -> Result<()> {
        if self.kind != "session"
            || self.version != 3
            || uuid::Uuid::parse_str(&self.id).is_err()
            || !text(&self.timestamp)
            || !text(&self.cwd)
            || self
                .parent_session
                .as_deref()
                .is_some_and(|value| !text(value))
        {
            return Err(invalid());
        }
        Ok(())
    }
}

pub(crate) fn fresh(bytes: &[u8], session: &str) -> Result<()> {
    let header: Header = decode(bytes, acyclic_stream::MAX_RECORD_BYTES as u64)?;
    header.validate()?;
    if header.id != session || header.parent_session.is_some() {
        return Err(invalid());
    }
    Ok(())
}

pub(crate) fn seed(bytes: &[u8], root: &std::path::Path) -> Result<Vec<u8>> {
    let mut header: Header = decode(bytes, acyclic_stream::MAX_RECORD_BYTES as u64)?;
    header.validate()?;
    header.cwd = root.to_str().ok_or_else(invalid)?.into();
    let mut bytes = crate::external_executor::volume::encode(&header).map_err(|_| invalid())?;
    bytes.push(b'\n');
    Ok(bytes)
}

pub(crate) fn rebase(bytes: &[u8], session: &str, root: &std::path::Path) -> Result<Vec<u8>> {
    let file = SessionFile::decode(bytes, bytes.len() as u64)?;
    if file.header.id != session {
        return Err(invalid());
    }
    let mut output = seed(file.header_bytes, root)?;
    for row in &file.rows {
        output.extend_from_slice(row.bytes);
        output.push(b'\n');
    }
    Ok(output)
}
