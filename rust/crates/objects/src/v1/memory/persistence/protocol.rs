#[derive(Clone, PartialEq, prost::Message)]
pub(super) struct Header {
    #[prost(uint64, optional, tag = "1")]
    pub(super) object_bytes: Option<u64>,
    #[prost(uint64, optional, tag = "2")]
    pub(super) total_bytes: Option<u64>,
    #[prost(uint64, optional, tag = "3")]
    pub(super) operations: Option<u64>,
    #[prost(uint64, optional, tag = "4")]
    pub(super) journal_bytes: Option<u64>,
    #[prost(bytes = "vec", tag = "5")]
    pub(super) cursor_key: Vec<u8>,
}
