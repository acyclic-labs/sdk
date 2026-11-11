use super::{Root, Storage};
use crate::content::{Field, INLINE_FIELD_BYTES as INLINE_BYTES};
use crate::{OwnedBytes, StreamError};

pub(crate) struct Cursor<'a, S> {
    content: super::Reader<'a, S>,
    page: OwnedBytes,
    offset: usize,
    remaining: u64,
    position: u64,
}

impl<'a, S: Storage> Cursor<'a, S> {
    pub(crate) fn position(&self) -> u64 {
        self.position
    }

    pub(crate) fn new(storage: &'a S, root: Root) -> Result<Self, StreamError> {
        Self::at(storage, root, 0)
    }
    pub(crate) fn at(storage: &'a S, root: Root, position: u64) -> Result<Self, StreamError> {
        let remaining = root
            .part
            .length
            .checked_sub(position)
            .ok_or(StreamError::InvalidArgument)?;
        Ok(Self {
            content: super::Reader::range(storage, root, position, remaining)?,
            page: OwnedBytes::new(),
            offset: 0,
            remaining,
            position,
        })
    }
    pub(crate) async fn copy(&mut self, output: &mut [u8]) -> Result<(), StreamError> {
        let length = u64::try_from(output.len()).map_err(|_| StreamError::Capacity)?;
        if length > self.remaining {
            return Err(StreamError::InvalidResponse);
        }
        let mut written = 0;
        while written < output.len() {
            if self.offset == self.page.len() {
                self.page = self
                    .content
                    .next()
                    .await?
                    .ok_or(StreamError::InvalidResponse)?;
                self.offset = 0;
                if self.page.is_empty() {
                    return Err(StreamError::InvalidResponse);
                }
            }
            let count = (output.len() - written).min(self.page.len() - self.offset);
            output[written..written + count]
                .copy_from_slice(&self.page.as_slice()[self.offset..self.offset + count]);
            self.offset += count;
            written += count;
            self.remaining -= count as u64;
            self.position += count as u64;
        }
        Ok(())
    }
    pub(crate) async fn exact<const N: usize>(&mut self) -> Result<[u8; N], StreamError> {
        let mut bytes = [0; N];
        self.copy(&mut bytes).await?;
        Ok(bytes)
    }
    pub(crate) async fn byte(&mut self) -> Result<u8, StreamError> {
        Ok(self.exact::<1>().await?[0])
    }
    pub(crate) async fn boolean(&mut self) -> Result<bool, StreamError> {
        match self.byte().await? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(StreamError::InvalidResponse),
        }
    }
    pub(crate) async fn number(&mut self) -> Result<u64, StreamError> {
        Ok(u64::from_le_bytes(self.exact().await?))
    }
    pub(crate) async fn optional(&mut self) -> Result<Option<u64>, StreamError> {
        if self.boolean().await? {
            Ok(Some(self.number().await?))
        } else {
            Ok(None)
        }
    }
    pub(crate) async fn small(&mut self, count: usize) -> Result<OwnedBytes, StreamError> {
        if count > INLINE_BYTES {
            return Err(StreamError::InvalidResponse);
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(count)
            .map_err(|_| StreamError::Capacity)?;
        bytes.resize(count, 0);
        self.copy(&mut bytes).await?;
        Ok(bytes.into())
    }
    pub(crate) async fn field(&mut self) -> Result<Field, StreamError> {
        match self.byte().await? {
            0 => {
                let count = usize::try_from(self.number().await?)
                    .map_err(|_| StreamError::InvalidResponse)?;
                Ok(Field::Inline(self.small(count).await?))
            }
            1 => {
                let digest = self.exact().await?;
                let length = self.number().await?;
                let height = self.byte().await?;
                if height > u64::BITS as u8 {
                    return Err(StreamError::InvalidResponse);
                }
                Ok(Field::Content(Root {
                    part: crate::content::Part { digest, length },
                    height,
                }))
            }
            _ => Err(StreamError::InvalidResponse),
        }
    }
    pub(crate) async fn end(&mut self) -> Result<(), StreamError> {
        if self.remaining != 0
            || self.offset != self.page.len()
            || self.content.next().await?.is_some()
        {
            return Err(StreamError::InvalidResponse);
        }
        Ok(())
    }
}
