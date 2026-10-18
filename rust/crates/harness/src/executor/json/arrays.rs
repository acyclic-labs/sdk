use super::{Result, boundary, checked};

const WORD: usize = std::mem::size_of::<usize>();
const ENTRY: usize = 3 * WORD;

pub(super) fn bytes(count: usize) -> Result<usize> {
    checked(count.checked_mul(ENTRY), "JSON array index overflow")
}

// Parent links replace a separate stack; entries and lengths are in decoder
// preorder, whether separate or prefixed to a RawValue buffer.
#[derive(Default)]
pub(super) struct Index {
    next: usize,
    current: Option<usize>,
}
impl Index {
    pub(super) fn byte(
        &mut self,
        table: &mut [u8],
        byte: u8,
        depth: u32,
        string: bool,
        item: bool,
    ) -> Result<()> {
        if let Some(current) = self.current
            && item
            && get(table, current, 1)? == depth as usize
        {
            let count = checked(
                get(table, current, 2)?.checked_add(1),
                "JSON array length overflow",
            )?;
            put(table, current, 2, count)?;
        }
        if !string && byte == b'[' {
            put(table, self.next, 0, self.current.unwrap_or(usize::MAX))?;
            put(table, self.next, 1, depth as usize + 1)?;
            self.current = Some(self.next);
            self.next += 1;
        } else if !string && byte == b']' {
            let current = self.current.ok_or_else(invalid)?;
            let parent = get(table, current, 0)?;
            self.current = (parent != usize::MAX).then_some(parent);
        }
        Ok(())
    }
    pub(super) fn finish(&self, table: &[u8]) -> Result<()> {
        if self.current.is_some() || bytes(self.next)? != table.len() {
            return Err(invalid());
        }
        Ok(())
    }
}
fn invalid() -> super::Error {
    boundary("JSON array index differs from admitted document", None)
}
fn range(entry: usize, field: usize) -> Result<std::ops::Range<usize>> {
    let start = checked(
        entry
            .checked_mul(ENTRY)
            .and_then(|n| n.checked_add(field * WORD)),
        "JSON array index overflow",
    )?;
    Ok(start..checked(start.checked_add(WORD), "JSON array index overflow")?)
}
fn get(table: &[u8], entry: usize, field: usize) -> Result<usize> {
    let bytes = table.get(range(entry, field)?).ok_or_else(invalid)?;
    Ok(usize::from_ne_bytes(
        bytes.try_into().map_err(|_| invalid())?,
    ))
}
fn put(table: &mut [u8], entry: usize, field: usize, value: usize) -> Result<()> {
    table
        .get_mut(range(entry, field)?)
        .ok_or_else(invalid)?
        .copy_from_slice(&value.to_ne_bytes());
    Ok(())
}
pub(super) fn count(table: &[u8], entry: usize) -> Result<usize> {
    get(table, entry, 2)
}

// The original serializer already validated this document. Reuse its bytes for
// decoder array lengths instead of invoking user Serialize a second time.
pub(super) fn build(
    document: &[u8],
    count: usize,
    items: u64,
    bounds: super::JsonBounds,
    code: Option<crate::wire::ErrorCode>,
    charge: &mut dyn FnMut(super::JsonCodecCharge) -> Result<()>,
) -> Result<Vec<u8>> {
    use super::JsonCodecCharge;
    let length = bytes(count)?;
    if length == 0 {
        return Ok(Vec::new());
    }
    charge(JsonCodecCharge::Retain {
        bytes: length as u64,
    })?;
    charge(JsonCodecCharge::CopyAdmission {
        bytes: length as u64,
    })?;
    charge(JsonCodecCharge::AllocationOperation {
        bytes: length as u64,
    })?;
    let mut table = Vec::new();
    table
        .try_reserve_exact(length)
        .map_err(super::Error::Allocation)?;
    charge(JsonCodecCharge::Retain {
        bytes: (table.capacity() - length) as u64,
    })?;
    charge(JsonCodecCharge::Copy {
        bytes: length as u64,
    })?;
    table.resize(length, 0);
    // The scan re-examines exactly the serializer's lexical items. Fund them
    // before entering it; the scan may not examine one item more or fewer.
    charge(JsonCodecCharge::Inspect { items })?;
    let mut remaining = items;
    let mut prepaid = |event| match event {
        JsonCodecCharge::Work { items, .. } if items <= remaining => {
            remaining -= items;
            Ok(())
        }
        _ => Err(boundary("JSON array index scan exceeds its admission", code)),
    };
    let prepaid =
        std::cell::RefCell::new(&mut prepaid as &mut dyn FnMut(JsonCodecCharge) -> Result<()>);
    let mut lexical = super::admission::Lexical::default();
    let mut index = Index::default();
    for byte in document {
        let depth = lexical.depth;
        let string = lexical.string;
        let item = lexical.byte(*byte, bounds, code, &prepaid)?;
        index.byte(&mut table, *byte, depth, string, item)?;
    }
    if lexical.items != items {
        return Err(boundary("JSON array index scan differs from its admission", code));
    }
    index.finish(&table)?;
    Ok(table)
}
