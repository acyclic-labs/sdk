#[derive(Clone, PartialEq, prost::Message)]
pub(super) struct Header {
    #[prost(uint64, tag = "1")]
    pub(super) object_bytes: u64,
    #[prost(uint64, tag = "2")]
    pub(super) total_bytes: u64,
    #[prost(uint64, tag = "3")]
    pub(super) operations: u64,
    #[prost(uint64, tag = "4")]
    pub(super) journal_bytes: u64,
    #[prost(bytes = "vec", tag = "5")]
    pub(super) cursor_key: Vec<u8>,
}
