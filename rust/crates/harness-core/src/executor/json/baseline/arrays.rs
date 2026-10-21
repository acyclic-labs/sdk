use super::{Result, boundary, checked};

const WORD: usize = std::mem::size_of::<usize>();
const ENTRY: usize = 3 * WORD;

pub(super) fn bytes(count: usize) -> Result<usize> {
    checked(count.checked_mul(ENTRY), "JSON array index overflow")
}

// The index shares the encoded buffer's one allocation. Its parent links
// replace a separate stack; entries and lengths are in decoder preorder.
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
