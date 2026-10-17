use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage, Value};
use std::io;

type Error = Box<dyn std::error::Error>;

fn invalid() -> io::Error {
    io::Error::other("invalid protobuf descriptor shape")
}

fn decode(bytes: &[u8]) -> Result<DynamicMessage, Error> {
    let descriptor = DescriptorPool::global()
        .get_message_by_name("google.protobuf.FileDescriptorSet")
        .ok_or_else(invalid)?;
    Ok(DynamicMessage::decode(descriptor, bytes)?)
}

fn name(file: &Value) -> Option<String> {
    Some(
        file.as_message()?
            .get_field_by_name("name")?
            .as_str()?
            .to_owned(),
    )
}

fn list<'a>(message: &'a mut DynamicMessage, field: &str) -> Result<&'a mut Vec<Value>, io::Error> {
    message
        .get_field_by_name_mut(field)
        .and_then(Value::as_list_mut)
        .ok_or_else(invalid)
}

fn location_key(value: &Value) -> Option<(bool, [i32; 4], Vec<i32>)> {
    let location = value.as_message()?;
    let numbers = |field| {
        location
            .get_field_by_name(field)?
            .as_list()?
            .iter()
            .map(Value::as_i32)
            .collect::<Option<Vec<_>>>()
    };
    let path = numbers("path")?;
    let span = numbers("span")?;
    let coordinates = match span.as_slice() {
        [line, start, end] => [*line, *start, *line, *end],
        [line, start, end_line, end] => [*line, *start, *end_line, *end],
        _ => return None,
    };
    Some((!path.is_empty(), coordinates, path))
}

/// Preserve the descriptor identity emitted by the original Buf compiler.
///
/// Project files come from protoc, including source info and retained options.
/// Google imports are compiler inputs pinned by the existing wire baselines;
/// they must not change when the vendored compiler updates its built-in schema.
/// Unknown options remain intact through reflection-based decoding/encoding.
///
/// # Errors
/// Returns malformed protobuf descriptor errors.
pub fn canonicalize(bytes: &[u8], roots: &[&str]) -> Result<Vec<u8>, Error> {
    let mut set = decode(bytes)?;
    let mut pinned = decode(include_bytes!("../descriptors/objects-v1.bin"))?;
    let pinned = list(&mut pinned, "file")?;
    for file in list(&mut set, "file")? {
        let file_name = name(file).ok_or_else(invalid)?;
        if file_name.starts_with("google/protobuf/") {
            let baseline = pinned
                .iter()
                .find(|candidate| name(candidate).as_deref() == Some(file_name.as_str()))
                .ok_or_else(|| io::Error::other("unpinned Google protocol import"))?;
            file.clone_from(baseline);
            continue;
        }
        let file = file.as_message_mut().ok_or_else(invalid)?;
        let info = file
            .get_field_by_name_mut("source_code_info")
            .and_then(Value::as_message_mut)
            .ok_or_else(invalid)?;
        let locations = list(info, "location")?;
        locations.sort_by_cached_key(location_key);
        for location in locations {
            let location = location.as_message_mut().ok_or_else(invalid)?;
            if location.has_field_by_name("trailing_comments") {
                let comment = location
                    .get_field_by_name_mut("trailing_comments")
                    .and_then(Value::as_string_mut)
                    .ok_or_else(invalid)?;
                comment.truncate(comment.trim_end_matches('\n').len());
            }
        }
        // Buf's ImageFile extension (8042) records explicit import and syntax
        // flags, including false values. Preserve every other unknown option.
        let mut unknown = Vec::new();
        for field in file.take_unknown_fields() {
            if field.number() != 8042 {
                field.encode(&mut unknown);
            }
        }
        file.merge(unknown.as_slice())?;
        let is_import = !roots
            .iter()
            .any(|root| root.strip_prefix("proto/").unwrap_or(root) == file_name);
        file.merge([0xd2, 0xf6, 0x03, 4, 8, u8::from(is_import), 24, 0].as_slice())?;
    }
    Ok(set.encode_to_vec())
}
