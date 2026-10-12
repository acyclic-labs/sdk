use super::*;
use serde_json::Value;
struct OwnedMeter {
    bounds: JsonBounds,
    items: u64,
    bytes: u64,
}
impl OwnedMeter {
    fn visit(
        &mut self,
        depth: usize,
        payload: usize,
        charge: &mut dyn FnMut(JsonCodecCharge) -> Result<()>,
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
impl ValueInspector {
    fn owned(
        &mut self,
        value: &Value,
        selection: Selection,
        meter: &mut OwnedMeter,
        charge: &mut dyn FnMut(JsonCodecCharge) -> Result<()>,
    ) -> std::result::Result<(), CodecError> {
        let scope = Self::value_scope(selection);
        meter.visit(self.frame_len, value.as_str().map_or(0, str::len), charge)?;
        self.item(scope, false);
        match value {
            Value::Array(values) => {
                if !self.enter(selection, false) {
                    return Err(CodecError::Bound(DEPTH_LIMIT));
                }
                for value in values {
                    let child = self.select(0);
                    self.owned(value, child, meter, charge)?;
                    self.frames[self.frame_len - 1].array_index += 1;
                }
                self.close();
            }
            Value::Object(values) => {
                if !self.enter(selection, true) {
                    return Err(CodecError::Bound(DEPTH_LIMIT));
                }
                let frame = self.frame_len - 1;
                for (index, (key, value)) in values.iter().enumerate() {
                    meter.visit(self.frame_len, key.len(), charge)?;
                    self.item(Self::value_scope(self.frames[frame].selection), false);
                    self.frames[frame].key = Token::from_str(key);
                    let child = self.select(0);
                    if !Self::tag_key(self.frames[frame].selection.node).is_empty()
                        && key.as_bytes() == Self::tag_key(self.frames[frame].selection.node)
                    {
                        self.tag(value.as_str().map_or(Token::EMPTY, Token::from_str));
                    }
                    if index == 0
                        && key.as_bytes() == RAW_KEY
                        && Self::value_scope(child) != Scope::Ignore
                        && value.is_string()
                    {
                        meter.visit(self.frame_len, value.as_str().unwrap().len(), charge)?;
                        self.item(Self::value_scope(child), false);
                        let mut source = Slice::new(value.as_str().unwrap().as_bytes(), None);
                        self.raw(&mut source, Self::value_scope(child), charge)?;
                    } else {
                        self.owned(value, child, meter, charge)?;
                    }
                }
                self.close();
            }
            _ => {}
        }
        Ok(())
    }
}
pub(in crate::contract::json) fn inspect_owned<F: FnMut(JsonCodecCharge) -> Result<()>>(
    value: &Value,
    bounds: JsonBounds,
    schema: JsonValueSchema,
    charge: &mut F,
) -> std::result::Result<JsonValueShape, CodecError> {
    let mut inspector = ValueInspector::new(bounds);
    let mut meter = OwnedMeter {
        bounds,
        items: 0,
        bytes: 0,
    };
    inspector.owned(
        value,
        Selection {
            node: Node::Schema(schema),
            scope: Scope::Value,
        },
        &mut meter,
        charge,
    )?;
    let mut shape = inspector.finish(meter.bytes, meter.items)?;
    shape.origin = ShapeOrigin::OwnedConversion;
    Ok(shape)
}
