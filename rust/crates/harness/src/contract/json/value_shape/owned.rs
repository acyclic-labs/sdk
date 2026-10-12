use super::*;

impl ValueInspector {
    fn owned_key(&mut self, key: &str) {
        let mut matched = Match::new();
        for &byte in key.as_bytes() {
            matched.push(byte);
        }
        let frame = &mut self.frames[self.frame_len - 1];
        frame.key = matched.key();
        frame.raw = frame.first && frame.key == RAW_KEY;
        frame.phase = Phase::Value;
    }

    fn owned_complete(&mut self) {
        self.complete(0);
        if self.frame_len != 0 {
            let frame = &mut self.frames[self.frame_len - 1];
            frame.first = false;
            frame.raw = false;
            frame.phase = if frame.object {
                Phase::Key
            } else {
                Phase::Value
            };
        }
    }

    fn owned_enter<'a, F: FnMut(JsonCodecCharge) -> Result<()>>(
        &mut self,
        value: &'a serde_json::Value,
        charge: &mut F,
    ) -> std::result::Result<Option<OwnedFrame<'a>>, CodecError> {
        let selection = self.select(0);
        let scope = Self::value_scope(selection);
        self.item(0, scope);
        let tag = self.frame_len != 0
            && !Self::tag_key(self.frames[self.frame_len - 1].selection.node).is_empty()
            && self.frames[self.frame_len - 1].key
                == Self::tag_key(self.frames[self.frame_len - 1].selection.node);
        if tag {
            let mut matched = Match::new();
            if let serde_json::Value::String(string) = value {
                for &byte in string.as_bytes() {
                    matched.push(byte);
                }
            }
            self.set_tag(if value.is_string() {
                matched.key()
            } else {
                b""
            });
        }
        match value {
            serde_json::Value::Object(values) => {
                let node = if selection.node == Node::Schema(JsonValueSchema::ModelContent) {
                    Node::Schema(JsonValueSchema::ModelContentPart)
                } else {
                    selection.node
                };
                self.frames[self.frame_len] = Frame {
                    object: true,
                    phase: Phase::Key,
                    selection: Selection { node, ..selection },
                    ..Frame::EMPTY
                };
                self.frame_len += 1;
                Ok(Some(OwnedFrame::Object(values.iter())))
            }
            serde_json::Value::Array(values) => {
                let node = if selection.node == Node::Schema(JsonValueSchema::ModelContent) {
                    Node::Parts
                } else {
                    selection.node
                };
                self.frames[self.frame_len] = Frame {
                    selection: Selection { node, ..selection },
                    ..Frame::EMPTY
                };
                self.frame_len += 1;
                Ok(Some(OwnedFrame::Array(values.iter())))
            }
            serde_json::Value::String(string) => {
                let raw = self.frame_len != 0 && self.frames[self.frame_len - 1].raw;
                if raw && scope != Scope::Ignore {
                    if scope == Scope::Unknown {
                        self.potential_raw = true;
                    } else {
                        if let Some(stats) = self.stats(scope) {
                            Stats::add(&mut stats.raw_frames, &mut stats.overflow);
                        }
                        self.layers[1] = Layer {
                            base: self.frame_len,
                            root: Selection {
                                node: Node::Schema(JsonValueSchema::Root),
                                scope,
                            },
                            ..Layer::EMPTY
                        };
                        for &byte in string.as_bytes() {
                            self.push_layer(1, byte, charge)?;
                        }
                        self.close_raw(0, scope);
                    }
                }
                Ok(None)
            }
            _ => Ok(None),
        }
    }
}

enum OwnedFrame<'a> {
    Array(std::slice::Iter<'a, serde_json::Value>),
    Object(serde_json::map::Iter<'a>),
}

struct OwnedMeter {
    bounds: JsonBounds,
    items: u64,
    bytes: u64,
}
impl OwnedMeter {
    fn visit<F: FnMut(JsonCodecCharge) -> Result<()>>(
        &mut self,
        depth: usize,
        payload: usize,
        charge: &mut F,
    ) -> std::result::Result<(), CodecError> {
        if depth > self.bounds.maximum_depth.min(256) as usize {
            return Err(CodecError::Bound(DEPTH_LIMIT));
        }
        if self.items >= self.bounds.maximum_items {
            return Err(CodecError::Bound(ITEM_LIMIT));
        }
        let bytes = self
            .bytes
            .checked_add(1)
            .and_then(|bytes| bytes.checked_add(payload as u64))
            .ok_or(CodecError::Bound("JSON byte limit exceeded"))?;
        if bytes > self.bounds.maximum_bytes {
            return Err(CodecError::Bound("JSON byte limit exceeded"));
        }
        charge(JsonCodecCharge::Inspect { items: 1 })?;
        self.items += 1;
        self.bytes = bytes;
        Ok(())
    }
}

/// Borrowed resident-tree witness immediately before a genuine from_value conversion.
pub(in crate::contract::json) fn inspect_owned<F: FnMut(JsonCodecCharge) -> Result<()>>(
    value: &serde_json::Value,
    bounds: JsonBounds,
    schema: JsonValueSchema,
    charge: &mut F,
) -> std::result::Result<JsonValueShape, CodecError> {
    let mut inspector = ValueInspector::new(Some(schema), bounds);
    let mut meter = OwnedMeter {
        bounds,
        items: 0,
        bytes: 0,
    };
    let mut stack: [Option<OwnedFrame<'_>>; 257] = std::array::from_fn(|_| None);
    let mut length = 0;
    let mut next = Some(value);
    loop {
        if let Some(value) = next.take() {
            let payload = match value {
                serde_json::Value::String(string) => string.len(),
                _ => 0,
            };
            meter.visit(length, payload, charge)?;
            if let Some(frame) = inspector.owned_enter(value, charge)? {
                stack[length] = Some(frame);
                length += 1;
            } else {
                inspector.owned_complete();
            }
        }
        if length == 0 {
            break;
        }
        let entry = match stack[length - 1]
            .as_mut()
            .expect("live fixed resident iterator")
        {
            OwnedFrame::Array(values) => values.next().map(|value| (None, value)),
            OwnedFrame::Object(values) => values.next().map(|(key, value)| (Some(key), value)),
        };
        if let Some((key, value)) = entry {
            if let Some(key) = key {
                meter.visit(length, key.len(), charge)?;
                inspector.owned_key(key);
            }
            next = Some(value);
        } else {
            stack[length - 1] = None;
            length -= 1;
            inspector.close_frame();
            inspector.owned_complete();
        }
    }
    let mut shape = inspector.finish(meter.bytes, meter.items)?;
    shape.origin = ShapeOrigin::OwnedConversion;
    Ok(shape)
}
