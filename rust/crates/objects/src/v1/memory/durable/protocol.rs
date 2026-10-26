use super::*;

#[derive(Clone, PartialEq, prost::Message)]
pub(in crate::v1::memory) struct Delta {
    #[prost(uint64, tag = "1")]
    pub(in crate::v1::memory) sequence: u64,
    #[prost(uint64, tag = "2")]
    pub(in crate::v1::memory) ordinal: u64,
    #[prost(message, repeated, tag = "3")]
    pub(in crate::v1::memory) buckets: Vec<BucketChange>,
    #[prost(message, repeated, tag = "4")]
    pub(in crate::v1::memory) objects: Vec<ObjectChange>,
    #[prost(message, repeated, tag = "5")]
    pub(in crate::v1::memory) uploads: Vec<UploadChange>,
    #[prost(message, repeated, tag = "6")]
    pub(in crate::v1::memory) parts: Vec<PartChange>,
    #[prost(message, repeated, tag = "7")]
    pub(in crate::v1::memory) receipts: Vec<ReceiptChange>,
    #[prost(bytes = "vec", tag = "8")]
    pub(in crate::v1::memory) inline_data: Vec<u8>,
}
#[derive(Clone, PartialEq, prost::Message)]
pub(in crate::v1::memory) struct BucketChange {
    #[prost(string, tag = "1")]
    pub(in crate::v1::memory) name: String,
    #[prost(message, optional, tag = "2")]
    pub(in crate::v1::memory) info: Option<wire::Bucket>,
}
#[derive(Clone, PartialEq, prost::Message)]
pub(in crate::v1::memory) struct ObjectChange {
    #[prost(string, tag = "1")]
    pub(in crate::v1::memory) bucket: String,
    #[prost(string, tag = "2")]
    pub(in crate::v1::memory) key: String,
    #[prost(message, optional, tag = "3")]
    pub(in crate::v1::memory) info: Option<wire::ObjectInfo>,
    #[prost(message, repeated, tag = "4")]
    pub(in crate::v1::memory) bodies: Vec<BodyRecord>,
}
#[derive(Clone, PartialEq, prost::Message)]
pub(in crate::v1::memory) struct UploadChange {
    #[prost(string, tag = "1")]
    pub(in crate::v1::memory) id: String,
    #[prost(bool, tag = "2")]
    pub(in crate::v1::memory) deleted: bool,
    #[prost(string, tag = "3")]
    pub(in crate::v1::memory) bucket: String,
    #[prost(string, tag = "4")]
    pub(in crate::v1::memory) key: String,
    #[prost(message, optional, tag = "5")]
    pub(in crate::v1::memory) metadata: Option<wire::ObjectMetadata>,
}
#[derive(Clone, PartialEq, prost::Message)]
pub(in crate::v1::memory) struct PartChange {
    #[prost(string, tag = "1")]
    pub(in crate::v1::memory) upload: String,
    #[prost(uint32, tag = "2")]
    pub(in crate::v1::memory) number: u32,
    #[prost(message, optional, tag = "3")]
    pub(in crate::v1::memory) receipt: Option<wire::UploadedPart>,
    #[prost(message, repeated, tag = "4")]
    pub(in crate::v1::memory) bodies: Vec<BodyRecord>,
}
#[derive(Clone, PartialEq, prost::Message)]
pub(in crate::v1::memory) struct ReceiptChange {
    #[prost(string, tag = "1")]
    pub(in crate::v1::memory) key: String,
    #[prost(bytes = "vec", tag = "2")]
    pub(in crate::v1::memory) digest: Vec<u8>,
    #[prost(bytes = "vec", tag = "3")]
    pub(in crate::v1::memory) response: Vec<u8>,
    #[prost(uint32, tag = "4")]
    pub(in crate::v1::memory) kind: u32,
}
#[derive(Clone, PartialEq, prost::Message)]
pub(in crate::v1::memory) struct BodyRecord {
    #[prost(bytes = "vec", tag = "1")]
    pub(in crate::v1::memory) segment: Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    pub(in crate::v1::memory) digest: Vec<u8>,
    #[prost(uint64, tag = "3")]
    pub(in crate::v1::memory) length: u64,
    #[prost(fixed64, tag = "4")]
    pub(in crate::v1::memory) offset: u64,
    #[prost(bool, tag = "5")]
    pub(in crate::v1::memory) journal: bool,
}
