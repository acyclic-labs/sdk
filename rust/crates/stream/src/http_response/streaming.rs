//! Incremental projection of the canonical compressed stream frames.
use super::*;

/// Response validation state is SDK-owned, shared across HTTP framing choices.
/// It retains only the next sequence/last child and a finite remaining count.
pub struct StreamProjection {
    kind: Kind,
    maximum: usize,
}
enum Kind {
    Records {
        next: u64,
        remaining: Option<u32>,
    },
    Children {
        last: Option<String>,
        remaining: u32,
    },
}
impl StreamProjection {
    /// Initialize from the same generated request sent to `StreamService`.
    pub fn new(route: &str, request: &[u8], maximum: usize) -> Result<Self> {
        if maximum == 0 || request.len() > crate::MAX_COMMAND_BYTES {
            return Err("limit_exceeded");
        }
        let kind = match route {
            "read" => {
                let request = wire::ReadRequest::decode(request).map_err(|_| "invalid_argument")?;
                Kind::Records {
                    next: request.from,
                    remaining: Some(request.limit),
                }
            }
            "follow" => {
                let request =
                    wire::FollowRequest::decode(request).map_err(|_| "invalid_argument")?;
                Kind::Records {
                    next: request.from,
                    remaining: None,
                }
            }
            "children" => {
                let request =
                    wire::ChildrenRequest::decode(request).map_err(|_| "invalid_argument")?;
                Kind::Children {
                    last: None,
                    remaining: request.limit,
                }
            }
            _ => return Err("invalid_argument"),
        };
        Ok(Self { kind, maximum })
    }
    /// Decode a whole authenticated protobuf frame before advancing state.
    /// Record batches are decompressed with canonical bounds and emitted one
    /// JSON element per logical record, not one frame per compressed batch.
    pub fn encode_frame(&mut self, input: &[u8]) -> Result<Vec<Vec<u8>>> {
        if input.len() > self.maximum {
            return Err("limit_exceeded");
        }
        match &mut self.kind {
            Kind::Records { next, remaining } => {
                let frame = wire::ReadResponse::decode(input).map_err(|_| "invalid_response")?;
                let records = crate::wire_codec::read_response_records(frame)
                    .map_err(|_| "invalid_response")?;
                if records.len() > crate::MAX_ITEMS
                    || remaining.is_some_and(|remaining| records.len() > remaining as usize)
                {
                    return Err("invalid_response");
                }
                let mut cursor = *next;
                let mut output = Vec::with_capacity(records.len());
                let mut size = 0usize;
                for record in records {
                    if record.sequence != cursor || record.value.len() > crate::MAX_RECORD_BYTES {
                        return Err("invalid_response");
                    }
                    crate::wire_codec::commit_id(&record.commit_id)
                        .map_err(|_| "invalid_response")?;
                    cursor = cursor.checked_add(1).ok_or("invalid_response")?;
                    let encoded = serde_json::to_vec(&record_json(&record))
                        .map_err(|_| "invalid_response")?;
                    size = size
                        .checked_add(encoded.len())
                        .filter(|size| *size <= self.maximum)
                        .ok_or("limit_exceeded")?;
                    output.push(encoded);
                }
                if let Some(remaining) = remaining {
                    *remaining -= u32::try_from(output.len()).map_err(|_| "invalid_response")?;
                }
                *next = cursor;
                Ok(output)
            }
            Kind::Children { last, remaining } => {
                let child = wire::ChildrenResponse::decode(input)
                    .map_err(|_| "invalid_response")?
                    .child
                    .ok_or("invalid_response")?;
                if *remaining == 0 || last.as_ref().is_some_and(|last| last >= &child.path) {
                    return Err("invalid_response");
                }
                crate::StreamPath::new(&child.path).map_err(|_| "invalid_response")?;
                let encoded =
                    serde_json::to_vec(&json_object(vec![("path", json_string(&child.path))]))
                        .map_err(|_| "invalid_response")?;
                if encoded.len() > self.maximum {
                    return Err("limit_exceeded");
                }
                *last = Some(child.path);
                *remaining -= 1;
                Ok(vec![encoded])
            }
        }
    }
}

/// Bounded collection for finite hosted Read/Children calls. It assembles the
/// exact SDK array contract without reparsing or copying retained JSON values.
pub struct Collection {
    bytes: Vec<u8>,
    maximum: usize,
    empty: bool,
}
impl Collection {
    /// Starts a finite JSON array bounded by `maximum` bytes, including brackets.
    pub fn new(maximum: usize) -> Result<Self> {
        if maximum < 2 {
            return Err("limit_exceeded");
        }
        Ok(Self {
            bytes: vec![b'['],
            maximum,
            empty: true,
        })
    }
    /// `element` must be an element produced by `StreamProjection`.
    pub fn push(&mut self, element: &[u8]) -> Result<()> {
        let size = self
            .bytes
            .len()
            .checked_add(element.len())
            .and_then(|size| size.checked_add(usize::from(!self.empty) + 1))
            .filter(|size| *size <= self.maximum)
            .ok_or("limit_exceeded")?;
        self.bytes.reserve(size - self.bytes.len());
        if !self.empty {
            self.bytes.push(b',');
        }
        self.bytes.extend_from_slice(element);
        self.empty = false;
        Ok(())
    }
    /// Closes and returns the complete canonical response array.
    pub fn finish(mut self) -> Vec<u8> {
        self.bytes.push(b']');
        self.bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn batches_preserve_cursor_and_finite_collection() {
        let request = wire::ReadRequest {
            path: "events".into(),
            from: 5,
            limit: 2,
        }
        .encode_to_vec();
        let mut projection = StreamProjection::new("read", &request, 4096).unwrap();
        let record = |sequence| wire::Record {
            sequence,
            value: bytes::Bytes::from_static(b"x"),
            commit_id: vec![1; 32].into(),
            committed_at_micros: 1,
        };
        let frame =
            crate::wire_codec::read_response_wire(vec![record(5), record(6)]).encode_to_vec();
        let elements = projection.encode_frame(&frame).unwrap();
        let mut collection = Collection::new(4096).unwrap();
        for element in elements {
            collection.push(&element).unwrap();
        }
        let value: Value = serde_json::from_slice(&collection.finish()).unwrap();
        assert_eq!(value.as_array().unwrap().len(), 2);
        assert_eq!(value[0]["sequence"], "5");
        assert!(
            projection
                .encode_frame(
                    &crate::wire_codec::read_response_wire(vec![record(7)]).encode_to_vec()
                )
                .is_err()
        );
    }
    #[test]
    fn bad_frame_does_not_advance_cursor() {
        let request = wire::FollowRequest {
            path: "events".into(),
            from: 0,
        }
        .encode_to_vec();
        let mut projection = StreamProjection::new("follow", &request, 4096).unwrap();
        let record = |sequence| wire::Record {
            sequence,
            value: bytes::Bytes::new(),
            commit_id: vec![1; 32].into(),
            committed_at_micros: 0,
        };
        assert!(
            projection
                .encode_frame(
                    &crate::wire_codec::read_response_wire(vec![record(1)]).encode_to_vec()
                )
                .is_err()
        );
        assert!(
            projection
                .encode_frame(
                    &crate::wire_codec::read_response_wire(vec![record(0)]).encode_to_vec()
                )
                .is_ok()
        );
        let mut collection = Collection::new(3).unwrap();
        assert!(collection.push(b"12").is_err());
        assert_eq!(collection.finish(), b"[]");
    }
}
