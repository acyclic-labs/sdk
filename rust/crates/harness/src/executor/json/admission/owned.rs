use super::*;

struct Resident {
    bytes: u64,
    nodes: u64,
    bounds: JsonBounds,
}
impl Resident {
    fn visit(&mut self, depth: u32, payload: usize, charge: &Charge<'_>) -> Result<()> {
        let code = Some(crate::wire::ErrorCode::Invalid);
        if depth > self.bounds.maximum_depth.min(256) {
            return Err(boundary("JSON nesting limit exceeded", code));
        }
        if self.nodes >= self.bounds.maximum_items {
            return Err(boundary("JSON item limit exceeded", code));
        }
        let bytes = self
            .bytes
            .checked_add(1)
            .and_then(|bytes| bytes.checked_add(payload as u64))
            .ok_or_else(|| boundary("JSON byte limit exceeded", code))?;
        if bytes > self.bounds.maximum_bytes {
            return Err(boundary("JSON byte limit exceeded", code));
        }
        charge.borrow_mut()(JsonCodecCharge::Inspect { items: 1 })?;
        self.nodes += 1;
        self.bytes = bytes;
        Ok(())
    }
}
fn tree(
    value: &Value,
    schema: &'static Schema,
    depth: u32,
    resident: &mut Resident,
    charge: &Charge<'_>,
) -> Result<Branches> {
    resident.visit(depth, value.as_str().map_or(0, str::len), charge)?;
    let mut counts = Branches::default();
    match value {
        Value::Array(values) => {
            for (index, value) in values.iter().enumerate() {
                let (child, gate) = schema.child(b"", Some(index));
                counts.add(tree(value, child, depth + 1, resident, charge)?, gate)?;
            }
        }
        Value::Object(values) => {
            let mut tag = None;
            for (index, (key, field)) in values.iter().enumerate() {
                resident.visit(depth + 1, key.len(), charge)?;
                let (child, gate) = schema.child(key.as_bytes(), None);
                if !schema.tag.is_empty() && key == schema.tag {
                    tag = Some(schema.mask(field.as_str().map_or(b"", str::as_bytes)));
                }
                let selected = if index == 0
                    && key.as_bytes() == RAW
                    && std::ptr::eq(child, &ROOT)
                    && field.is_string()
                {
                    let text = field.as_str().unwrap();
                    resident.visit(depth + 1, text.len(), charge)?;
                    let mut source = text.bytes().map(|byte| {
                        charge.borrow_mut()(JsonCodecCharge::Inspect { items: 1 })?;
                        Ok(byte)
                    });
                    let mut reader: &mut Input<'_> = &mut source;
                    let mut reader = reader.by_ref().peekable();
                    let (mut selected, valid) =
                        super::value(&mut reader, &ROOT, depth + 1, true, charge)?;
                    let ended = space(&mut reader)?.is_none();
                    while next(&mut reader)?.is_some() {}
                    selected.always.unresolved |= !valid || !ended;
                    selected.always.bytes = checked(
                        selected.always.bytes.checked_add(text.len() as u64),
                        "JSON byte count overflow",
                    )?;
                    selected.always.frames = checked(
                        selected.always.frames.checked_add(1),
                        "JSON RawValue count overflow",
                    )?;
                    selected
                } else {
                    tree(field, child, depth + 1, resident, charge)?
                };
                counts.add(selected, gate)?;
            }
            if !schema.tag.is_empty() {
                counts.resolve(tag.unwrap_or(0))?;
            }
        }
        _ => {}
    }
    Ok(counts)
}
pub(super) fn inspect(
    value: &Value,
    bounds: JsonBounds,
    schema: &'static Schema,
    charge: &mut dyn FnMut(JsonCodecCharge) -> Result<()>,
) -> Result<JsonValueShape> {
    let charge = RefCell::new(charge);
    let mut resident = Resident {
        bytes: 0,
        nodes: 0,
        bounds,
    };
    let counts = tree(value, schema, 0, &mut resident, &charge)?.always;
    finish(
        resident.bytes,
        resident.nodes,
        counts,
        bounds,
        Origin::Owned,
        Some(crate::wire::ErrorCode::Invalid),
    )
}
