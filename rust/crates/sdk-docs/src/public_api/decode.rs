pub(crate) fn from_slice(bytes: &[u8]) -> Result<rustdoc_types::Crate, serde_json::Error> {
    serde_json::from_slice(bytes)
}
