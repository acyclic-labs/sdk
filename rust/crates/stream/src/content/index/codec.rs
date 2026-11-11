use super::*;

fn field_length(field: &Field) -> usize {
    match field {
        Field::Inline(bytes) => 9 + bytes.len(),
        Field::Content(_) => 42,
    }
}
pub(super) fn entry_length(entry: &Entry) -> usize {
    32 + field_length(&entry.key.field) + field_length(&entry.value)
}
pub(super) fn child_length(child: &Child<Key, Root>) -> usize {
    32 + field_length(&child.first.field) + 41
}
pub(super) fn root_bytes(bytes: &mut Vec<u8>, root: Root) {
    bytes.extend_from_slice(&root.part.digest);
    bytes.extend_from_slice(&root.part.length.to_le_bytes());
    bytes.push(root.height);
}
pub(super) async fn field_bytes<S: Storage>(
    writer: &mut tree::Writer<'_, S>,
    bytes: &mut Vec<u8>,
    field: &Field,
) -> Result<(), StreamError> {
    match field {
        Field::Inline(value) => {
            bytes.push(0);
            bytes.extend_from_slice(&(value.len() as u64).to_le_bytes());
            bytes.extend_from_slice(value.as_slice());
        }
        Field::Content(root) => {
            writer.reference(*root).await?;
            bytes.push(1);
            root_bytes(bytes, *root);
        }
    }
    Ok(())
}

pub(super) async fn write(
    storage: &impl Storage,
    page: PageRef<'_, Format>,
    height: u8,
) -> Result<Child<Key, Root>, StreamError> {
    let first = match &page {
        PageRef::Leaf(entries) => entries.first().map(|entry| &entry.key),
        PageRef::Internal(children) => children.first().map(|child| &child.first),
    }
    .ok_or(StreamError::InvalidArgument)?
    .clone();
    let page = write_root(storage, page, height).await?;
    Ok(Child { first, page })
}

pub(super) async fn write_root(
    storage: &impl Storage,
    page: PageRef<'_, Format>,
    height: u8,
) -> Result<Root, StreamError> {
    let (count, size, first) = match &page {
        PageRef::Leaf(entries) => (
            entries.len(),
            entries.iter().map(entry_length).sum::<usize>(),
            entries.first().map(|entry| &entry.key),
        ),
        PageRef::Internal(children) => (
            children.len(),
            children.iter().map(child_length).sum::<usize>(),
            children.first().map(|child| &child.first),
        ),
    };
    first.ok_or(StreamError::InvalidArgument)?;
    let size = size
        .checked_add(HEADER)
        .filter(|size| *size <= tree::LEAF_BYTES)
        .ok_or(StreamError::InvalidArgument)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(size)
        .map_err(|_| StreamError::Capacity)?;
    bytes.extend_from_slice(MAGIC);
    bytes.push(height);
    bytes.extend_from_slice(
        &u32::try_from(count)
            .map_err(|_| StreamError::Capacity)?
            .to_le_bytes(),
    );
    let mut writer = tree::Writer::new(storage)?;
    match page {
        PageRef::Leaf(entries) => {
            for entry in entries {
                bytes.extend_from_slice(&entry.key.prefix);
                field_bytes(&mut writer, &mut bytes, &entry.key.field).await?;
                field_bytes(&mut writer, &mut bytes, &entry.value).await?;
            }
        }
        PageRef::Internal(children) => {
            for child in children {
                bytes.extend_from_slice(&child.first.prefix);
                field_bytes(&mut writer, &mut bytes, &child.first.field).await?;
                writer.reference(child.page).await?;
                root_bytes(&mut bytes, child.page);
            }
        }
    }
    writer.write(&bytes).await?;
    let page = writer.finish().await?;
    crate::runtime::yield_now().await?;
    Ok(page)
}

pub(super) struct Decode<'a> {
    pub(super) bytes: &'a [u8],
}
impl Decode<'_> {
    fn take(&mut self, count: usize) -> Result<&[u8], StreamError> {
        let (head, tail) = self
            .bytes
            .split_at_checked(count)
            .ok_or(StreamError::InvalidResponse)?;
        self.bytes = tail;
        Ok(head)
    }
    pub(super) fn array<const N: usize>(&mut self) -> Result<[u8; N], StreamError> {
        self.take(N)?
            .try_into()
            .map_err(|_| StreamError::InvalidResponse)
    }
    pub(super) fn root(&mut self) -> Result<Root, StreamError> {
        let digest = self.array()?;
        let length = u64::from_le_bytes(self.array()?);
        let height = self.array::<1>()?[0];
        if height > MAX_HEIGHT {
            return Err(StreamError::InvalidResponse);
        }
        Ok(Root {
            part: crate::content::Part { digest, length },
            height,
        })
    }
    fn field(&mut self, maximum: usize) -> Result<Field, StreamError> {
        match self.array::<1>()?[0] {
            0 => {
                let count = usize::try_from(u64::from_le_bytes(self.array()?))
                    .map_err(|_| StreamError::InvalidResponse)?;
                if count > maximum {
                    return Err(StreamError::InvalidResponse);
                }
                Ok(Field::Inline(OwnedBytes::copy_from_slice(
                    self.take(count)?,
                )))
            }
            1 => Ok(Field::Content(self.root()?)),
            _ => Err(StreamError::InvalidResponse),
        }
    }
    fn key(&mut self) -> Result<Key, StreamError> {
        let prefix = self.array()?;
        let field = self.field(key::INLINE_KEY)?;
        Key::decoded(prefix, field)
    }
}

pub(super) async fn body(
    storage: &impl Storage,
    root: Root,
    minimum: usize,
    maximum: usize,
) -> Result<Vec<u8>, StreamError> {
    if root.part.length < minimum as u64 || root.part.length > maximum as u64 {
        return Err(StreamError::InvalidResponse);
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(root.part.length as usize)
        .map_err(|_| StreamError::Capacity)?;
    let mut reader = tree::Reader::new(storage, root)?;
    while let Some(page) = reader.next().await? {
        if page.len() > maximum - bytes.len() {
            return Err(StreamError::InvalidResponse);
        }
        bytes.extend_from_slice(page.as_slice());
    }
    Ok(bytes)
}

pub(super) async fn read(
    storage: &impl Storage,
    root: Root,
    height: u8,
) -> Result<Page<Format>, StreamError> {
    if height > MAX_HEIGHT {
        return Err(StreamError::InvalidResponse);
    }
    let bytes = body(storage, root, HEADER, tree::LEAF_BYTES).await?;
    let mut decode = Decode {
        bytes: crate::persistence::header(&bytes, b"acyclic-ordered-index-v", b"1\0")?,
    };
    if decode.array::<1>()?[0] != height {
        return Err(StreamError::InvalidResponse);
    }
    let count = u32::from_le_bytes(decode.array()?) as usize;
    let minimum = if height == 0 { 50 } else { 82 };
    if count == 0 || count > decode.bytes.len() / minimum {
        return Err(StreamError::InvalidResponse);
    }
    let page = if height == 0 {
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(count)
            .map_err(|_| StreamError::Capacity)?;
        for _ in 0..count {
            entries.push(Entry {
                key: decode.key()?,
                value: decode.field(INLINE_FIELD_BYTES)?,
            });
        }
        Page::Leaf(entries)
    } else {
        let mut children = Vec::new();
        children
            .try_reserve_exact(count)
            .map_err(|_| StreamError::Capacity)?;
        for _ in 0..count {
            children.push(Child {
                first: decode.key()?,
                page: decode.root()?,
            });
        }
        Page::Internal(children)
    };
    if !decode.bytes.is_empty() {
        return Err(StreamError::InvalidResponse);
    }
    Ok(page)
}
