use super::*;
use std::cmp::Ordering;

const PREFIX: usize = 32;
pub(super) const INLINE_KEY: usize = 64;

pub(super) fn length(field: &Field) -> u64 {
    match field {
        Field::Inline(bytes) => bytes.len() as u64,
        Field::Content(root) => root.part.length,
    }
}

pub(super) async fn store(
    storage: &impl Storage,
    field: Field,
    inline: usize,
) -> Result<Field, StreamError> {
    match field {
        Field::Inline(bytes) if bytes.len() > inline => {
            let mut writer = tree::Writer::new(storage)?;
            writer.write(bytes.as_slice()).await?;
            Ok(Field::Content(writer.finish().await?))
        }
        Field::Inline(bytes) => {
            let (bytes, _) = bytes.into_backed(inline);
            Ok(Field::Inline(bytes))
        }
        other => Ok(other),
    }
}

pub(super) enum Bytes<'a, S> {
    Inline { bytes: OwnedBytes, offset: usize },
    Content(tree::Reader<'a, S>),
}

impl<'a, S: Storage> Bytes<'a, S> {
    fn at(storage: &'a S, field: &Field, offset: u64) -> Result<Self, StreamError> {
        Ok(match field {
            Field::Inline(bytes) => Self::Inline {
                bytes: bytes.clone(),
                offset: usize::try_from(offset).map_err(|_| StreamError::Capacity)?,
            },
            Field::Content(root) => Self::Content(tree::Reader::range(
                storage,
                *root,
                offset,
                root.part
                    .length
                    .checked_sub(offset)
                    .ok_or(StreamError::InvalidArgument)?,
            )?),
        })
    }
    async fn next(&mut self) -> Result<Option<OwnedBytes>, StreamError> {
        match self {
            Self::Content(reader) => reader.next().await,
            Self::Inline { bytes, offset } => {
                if *offset >= bytes.len() {
                    return Ok(None);
                }
                let end = bytes.len().min(offset.saturating_add(tree::PAGE_BYTES));
                let page = bytes.slice(*offset..end);
                *offset = end;
                if page.len() == tree::PAGE_BYTES {
                    crate::runtime::yield_now().await?;
                }
                Ok(Some(page))
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct Key {
    pub(super) field: Field,
    pub(super) prefix: [u8; PREFIX],
}

impl Key {
    pub(super) async fn stored(storage: &impl Storage, field: Field) -> Result<Self, StreamError> {
        let mut key = Self::new(storage, field).await?;
        key.field = store(storage, key.field, INLINE_KEY).await?;
        Ok(key)
    }
    pub(super) async fn new(storage: &impl Storage, field: Field) -> Result<Self, StreamError> {
        let mut prefix = [0; PREFIX];
        let count = length(&field).min(PREFIX as u64) as usize;
        if let Field::Inline(bytes) = &field {
            prefix[..count].copy_from_slice(&bytes.as_slice()[..count]);
            return Ok(Self { field, prefix });
        }
        let mut bytes = Bytes::at(storage, &field, 0)?;
        let mut copied = 0;
        while copied < count {
            let page = bytes.next().await?.ok_or(StreamError::InvalidResponse)?;
            let take = (count - copied).min(page.len());
            if take == 0 {
                return Err(StreamError::InvalidResponse);
            }
            prefix[copied..copied + take].copy_from_slice(&page.as_slice()[..take]);
            copied += take;
        }
        Ok(Self { field, prefix })
    }
    pub(super) fn hash(&self) -> u64 {
        acyclic_collections::page::stable_hash(&self.prefix)
    }
    pub(super) async fn compare(
        &self,
        storage: &impl Storage,
        other: &Self,
    ) -> Result<Ordering, StreamError> {
        if let (Field::Content(left), Field::Content(right)) = (&self.field, &other.field)
            && left == right
        {
            return Ok(Ordering::Equal);
        }
        let left_len = length(&self.field);
        let right_len = length(&other.field);
        let prefix = left_len.min(right_len).min(PREFIX as u64) as usize;
        let ordering = self.prefix[..prefix].cmp(&other.prefix[..prefix]);
        if ordering != Ordering::Equal {
            return Ok(ordering);
        }
        if left_len.min(right_len) <= PREFIX as u64 {
            return Ok(left_len.cmp(&right_len));
        }
        let mut left = Bytes::at(storage, &self.field, PREFIX as u64)?;
        let mut right = Bytes::at(storage, &other.field, PREFIX as u64)?;
        let (mut a, mut b) = (OwnedBytes::new(), OwnedBytes::new());
        let (mut ai, mut bi) = (0, 0);
        loop {
            if ai == a.len() {
                a = left.next().await?.unwrap_or_default();
                ai = 0;
            }
            if bi == b.len() {
                b = right.next().await?.unwrap_or_default();
                bi = 0;
            }
            if a.is_empty() || b.is_empty() {
                return Ok(left_len.cmp(&right_len));
            }
            let count = (a.len() - ai).min(b.len() - bi);
            let ordering = crate::owned_bytes::compare_bytes(
                &a.as_slice()[ai..ai + count],
                &b.as_slice()[bi..bi + count],
            );
            if ordering != Ordering::Equal {
                return Ok(ordering);
            }
            ai += count;
            bi += count;
        }
    }
}
