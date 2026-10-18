use super::*;
use prost::encoding::{WireType, encode_key, encode_varint, encoded_len_varint, key_len};

// Write the generated schema's field numbers from borrowed domain fields. Nested
// messages write into the same exactly sized buffer; none owns temporary fields.
fn number_len(tag: u32, value: u64) -> usize {
    if value == 0 {
        0
    } else {
        key_len(tag) + encoded_len_varint(value)
    }
}
fn number(tag: u32, value: u64, out: &mut Vec<u8>) {
    if value != 0 {
        encode_key(tag, WireType::Varint, out);
        encode_varint(value, out);
    }
}
fn message_len(tag: u32, len: usize) -> usize {
    key_len(tag) + encoded_len_varint(len as u64) + len
}
fn header(tag: u32, len: usize, out: &mut Vec<u8>) {
    encode_key(tag, WireType::LengthDelimited, out);
    encode_varint(len as u64, out);
}
fn blob(tag: u32, value: &[u8], out: &mut Vec<u8>) {
    header(tag, value.len(), out);
    out.extend_from_slice(value);
}
fn workspace_len(name: &WorkspaceName) -> usize {
    message_len(1, 16) + message_len(2, name.as_str().len())
}
fn workspace(volume: VolumeId, name: &WorkspaceName, out: &mut Vec<u8>) {
    blob(1, &volume.into_bytes(), out);
    blob(2, name.as_str().as_bytes(), out);
}
fn subscription_len(sub: &Subscription) -> usize {
    number_len(1, 1)
        + message_len(2, 16)
        + message_len(3, sub.path_prefix.as_str().len())
        + number_len(4, sub.delivery as u64)
        + number_len(5, sub.start_next)
        + message_len(6, workspace_len(&sub.workspace_name))
}
fn subscription_fields(sub: &Subscription, out: &mut Vec<u8>) {
    number(1, 1, out);
    blob(2, &sub.volume_id.into_bytes(), out);
    blob(3, sub.path_prefix.as_str().as_bytes(), out);
    number(4, sub.delivery as u64, out);
    number(5, sub.start_next, out);
    header(6, workspace_len(&sub.workspace_name), out);
    workspace(sub.volume_id, &sub.workspace_name, out);
}
pub(super) fn subscription(sub: &Subscription) -> Bytes {
    let mut out = Vec::with_capacity(subscription_len(sub));
    subscription_fields(sub, &mut out);
    Bytes::from(out)
}
pub(super) fn publication(value: &Publication) -> Bytes {
    let ws_len = workspace_len(&value.workspace_name);
    let generation_len = message_len(1, ws_len) + message_len(2, 32);
    let len = number_len(1, 1)
        + message_len(2, 16)
        + message_len(3, generation_len)
        + number_len(4, value.sequence.get())
        + message_len(5, 32);
    let mut out = Vec::with_capacity(len);
    number(1, 1, &mut out);
    blob(2, &value.generation.volume_id.into_bytes(), &mut out);
    header(3, generation_len, &mut out);
    header(1, ws_len, &mut out);
    workspace(value.generation.volume_id, &value.workspace_name, &mut out);
    blob(
        2,
        value.generation.generation_root.digest.as_bytes(),
        &mut out,
    );
    number(4, value.sequence.get(), &mut out);
    blob(5, value.commit_id.as_bytes(), &mut out);
    Bytes::from(out)
}
pub(super) fn cursor(sub: &Subscription, next: u64, anchor: CommitId) -> Bytes {
    let sub_len = subscription_len(sub);
    let len = number_len(1, 1) + message_len(2, sub_len) + number_len(3, next) + message_len(4, 32);
    let mut out = Vec::with_capacity(len);
    number(1, 1, &mut out);
    header(2, sub_len, &mut out);
    subscription_fields(sub, &mut out);
    number(3, next, &mut out);
    blob(4, anchor.as_bytes(), &mut out);
    Bytes::from(out)
}
