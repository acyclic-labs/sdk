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
struct Frame<'a> {
    schema: &'static JsonValueSchema,
    array: std::iter::Enumerate<std::slice::Iter<'a, Value>>,
    object: Option<std::iter::Enumerate<serde_json::map::Iter<'a>>>,
    counts: Branches,
    tag: Option<u8>,
    gate: u8,
}
impl<'a> Frame<'a> {
    fn new(value: &'a Value, schema: &'static JsonValueSchema, gate: u8) -> Self {
        Self {
            schema,
            array: value
                .as_array()
                .map_or(&[][..], Vec::as_slice)
                .iter()
                .enumerate(),
            object: value.as_object().map(|values| values.iter().enumerate()),
            counts: Branches::default(),
            tag: None,
            gate,
        }
    }
}
fn tree(
    value: &Value,
    schema: &'static JsonValueSchema,
    resident: &mut Resident,
    charge: &Charge<'_>,
) -> Result<Branches> {
    resident.visit(0, value.as_str().map_or(0, str::len), charge)?;
    let mut stack: [Option<Frame<'_>>; 257] = [const { None }; 257];
    let invalid_frame = || {
        boundary(
            "JSON nesting limit exceeded",
            Some(crate::wire::ErrorCode::Invalid),
        )
    };
    *stack.first_mut().ok_or_else(invalid_frame)? = Some(Frame::new(value, schema, 0));
    let mut depth = 0;
    loop {
        let frame = stack
            .get_mut(depth)
            .and_then(Option::as_mut)
            .ok_or_else(invalid_frame)?;
        let next = if let Some(object) = &mut frame.object {
            object
                .next()
                .map(|(index, (key, value))| (index, key.as_str(), value))
        } else {
            frame.array.next().map(|(index, value)| (index, "", value))
        };
        let Some((index, key, field)) = next else {
            let mut frame = stack
                .get_mut(depth)
                .and_then(Option::take)
                .ok_or_else(invalid_frame)?;
            if frame.object.is_some() && !frame.schema.tag.is_empty() {
                frame.counts.resolve(frame.tag.unwrap_or(0))?;
            }
            if depth == 0 {
                return Ok(frame.counts);
            }
            depth -= 1;
            stack
                .get_mut(depth)
                .and_then(Option::as_mut)
                .ok_or_else(invalid_frame)?
                .counts
                .add(frame.counts, frame.gate)?;
            continue;
        };
        let child_depth = u32::try_from(depth).map_err(|_| invalid_frame())? + 1;
        if frame.object.is_some() {
            resident.visit(child_depth, key.len(), charge)?;
        }
        let (child, gate) = frame
            .schema
            .child(key.as_bytes(), frame.object.is_none().then_some(index));
        if frame.object.is_some() && !frame.schema.tag.is_empty() && key == frame.schema.tag {
            frame.tag = Some(frame.schema.mask(field.as_str().map_or(b"", str::as_bytes)));
        }
        resident.visit(child_depth, field.as_str().map_or(0, str::len), charge)?;
        if index == 0
            && key.as_bytes() == RAW
            && std::ptr::eq(child, &ROOT)
            && let Some(text) = field.as_str()
        {
            charge.borrow_mut()(JsonCodecCharge::Inspect {
                items: text.len() as u64,
            })?;
            let mut selected = super::document(
                text.as_bytes(),
                &ROOT,
                child_depth,
                true,
                charge,
                &RefCell::new(None),
            )?;
            selected.always.bytes = checked(
                selected.always.bytes.checked_add(text.len() as u64),
                "JSON byte count overflow",
            )?;
            selected.always.frames = checked(
                selected.always.frames.checked_add(1),
                "JSON RawValue count overflow",
            )?;
            frame.counts.add(selected, gate)?;
        } else {
            depth += 1;
            *stack.get_mut(depth).ok_or_else(invalid_frame)? = Some(Frame::new(field, child, gate));
        }
    }
}
pub(super) fn inspect(
    value: &Value,
    bounds: JsonBounds,
    schema: &'static JsonValueSchema,
    charge: &mut dyn FnMut(JsonCodecCharge) -> Result<()>,
) -> Result<JsonValueShape> {
    let charge = RefCell::new(charge);
    let mut resident = Resident {
        bytes: 0,
        nodes: 0,
        bounds,
    };
    let counts = tree(value, schema, &mut resident, &charge)?.always;
    finish(
        resident.bytes,
        resident.nodes,
        0,
        counts,
        bounds,
        Origin::Owned,
        Some(crate::wire::ErrorCode::Invalid),
    )
}
