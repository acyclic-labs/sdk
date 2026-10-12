use super::*;

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

#[derive(Clone, PartialEq, prost::Message)]
pub(super) struct Delta {
    #[prost(uint64, tag = "1")]
    pub(super) sequence: u64,
    #[prost(uint64, tag = "2")]
    pub(super) ordinal: u64,
    #[prost(message, repeated, tag = "3")]
    pub(super) buckets: Vec<BucketChange>,
    #[prost(message, repeated, tag = "4")]
    pub(super) objects: Vec<ObjectChange>,
    #[prost(message, repeated, tag = "5")]
    pub(super) uploads: Vec<UploadChange>,
    #[prost(message, repeated, tag = "6")]
    pub(super) parts: Vec<PartChange>,
    #[prost(message, repeated, tag = "7")]
    pub(super) receipts: Vec<ReceiptChange>,
    #[prost(bytes = "vec", tag = "8")]
    pub(super) inline_data: Vec<u8>,
}
#[derive(Clone, PartialEq, prost::Message)]
pub(super) struct BucketChange {
    #[prost(string, tag = "1")]
    pub(super) name: String,
    #[prost(message, optional, tag = "2")]
    pub(super) info: Option<wire::Bucket>,
}
#[derive(Clone, PartialEq, prost::Message)]
pub(super) struct ObjectChange {
    #[prost(string, tag = "1")]
    pub(super) bucket: String,
    #[prost(string, tag = "2")]
    pub(super) key: String,
    #[prost(message, optional, tag = "3")]
    pub(super) info: Option<wire::ObjectInfo>,
    #[prost(message, repeated, tag = "4")]
    pub(super) bodies: Vec<BodyRecord>,
}
#[derive(Clone, PartialEq, prost::Message)]
pub(super) struct UploadChange {
    #[prost(string, tag = "1")]
    pub(super) id: String,
    #[prost(bool, tag = "2")]
    pub(super) deleted: bool,
    #[prost(string, tag = "3")]
    pub(super) bucket: String,
    #[prost(string, tag = "4")]
    pub(super) key: String,
    #[prost(message, optional, tag = "5")]
    pub(super) metadata: Option<wire::ObjectMetadata>,
}
#[derive(Clone, PartialEq, prost::Message)]
pub(super) struct PartChange {
    #[prost(string, tag = "1")]
    pub(super) upload: String,
    #[prost(uint32, tag = "2")]
    pub(super) number: u32,
    #[prost(message, optional, tag = "3")]
    pub(super) receipt: Option<wire::UploadedPart>,
    #[prost(message, repeated, tag = "4")]
    pub(super) bodies: Vec<BodyRecord>,
}
#[derive(Clone, PartialEq, prost::Message)]
pub(super) struct ReceiptChange {
    #[prost(string, tag = "1")]
    pub(super) key: String,
    #[prost(bytes = "vec", tag = "2")]
    pub(super) digest: Vec<u8>,
    #[prost(bytes = "vec", tag = "3")]
    pub(super) response: Vec<u8>,
    #[prost(uint32, tag = "4")]
    pub(super) kind: u32,
}
#[derive(Clone, PartialEq, prost::Message)]
pub(super) struct BodyRecord {
    #[prost(bytes = "vec", tag = "1")]
    pub(super) segment: Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    pub(super) digest: Vec<u8>,
    #[prost(uint64, tag = "3")]
    pub(super) length: u64,
    #[prost(fixed64, tag = "4")]
    pub(super) offset: u64,
    #[prost(bool, tag = "5")]
    pub(super) journal: bool,
}
