use super::*;

struct Cursor<'a, S> {
    content: tree::Reader<'a, S>,
    page: OwnedBytes,
    offset: usize,
    remaining: u64,
    position: u64,
}

impl<'a, S: Storage> Cursor<'a, S> {
    fn new(storage: &'a S, root: Root) -> Result<Self, StreamError> {
        Self::at(storage, root, 0)
    }
    fn at(storage: &'a S, root: Root, position: u64) -> Result<Self, StreamError> {
        let remaining = root
            .part
            .length
            .checked_sub(position)
            .ok_or(StreamError::InvalidArgument)?;
        Ok(Self {
            content: tree::Reader::range(storage, root, position, remaining)?,
            page: OwnedBytes::new(),
            offset: 0,
            remaining,
            position,
        })
    }
    async fn copy(&mut self, output: &mut [u8]) -> Result<(), StreamError> {
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
    async fn exact<const N: usize>(&mut self) -> Result<[u8; N], StreamError> {
        let mut bytes = [0; N];
        self.copy(&mut bytes).await?;
        Ok(bytes)
    }
    async fn byte(&mut self) -> Result<u8, StreamError> {
        Ok(self.exact::<1>().await?[0])
    }
    async fn boolean(&mut self) -> Result<bool, StreamError> {
        match self.byte().await? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(StreamError::InvalidResponse),
        }
    }
    async fn number(&mut self) -> Result<u64, StreamError> {
        Ok(u64::from_le_bytes(self.exact().await?))
    }
    async fn optional(&mut self) -> Result<Option<u64>, StreamError> {
        if self.boolean().await? {
            Ok(Some(self.number().await?))
        } else {
            Ok(None)
        }
    }
    async fn small(&mut self, count: usize) -> Result<OwnedBytes, StreamError> {
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
    async fn field(&mut self) -> Result<Field, StreamError> {
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
    async fn end(&mut self) -> Result<(), StreamError> {
        if self.remaining != 0
            || self.offset != self.page.len()
            || self.content.next().await?.is_some()
        {
            return Err(StreamError::InvalidResponse);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Phase {
    Effect,
    Condition,
    Mutation,
    Exclusion,
    Record,
    Key,
    Attribute,
    End,
}

#[derive(Clone, Default)]
pub(super) struct Shape {
    family: Option<Family>,
    effects: u64,
    conditions: u64,
    mutations: u64,
    exclusions: u64,
    records: u64,
    keys: u64,
    attributes: u64,
}

impl Shape {
    pub(super) fn new(family: Family, effects: u64) -> Self {
        Self {
            family: Some(family),
            effects,
            ..Self::default()
        }
    }
    pub(super) fn complete(&self) -> bool {
        self.next() == Phase::End
    }
    fn next(&self) -> Phase {
        if self.keys != 0 {
            Phase::Key
        } else if self.attributes != 0 {
            Phase::Attribute
        } else if self.exclusions != 0 {
            Phase::Exclusion
        } else if self.records != 0 {
            Phase::Record
        } else if self.conditions != 0 {
            Phase::Condition
        } else if self.mutations != 0 {
            Phase::Mutation
        } else if self.effects != 0 {
            Phase::Effect
        } else {
            Phase::End
        }
    }
    pub(super) fn accept<B>(&mut self, token: &Token<B>) -> Result<(), StreamError> {
        match (self.family, self.next(), token) {
            (
                Some(Family::Append | Family::Fork),
                Phase::Effect,
                Token::Effect {
                    soft,
                    conditions,
                    mutations,
                },
            ) => {
                let allowed = if self.family == Some(Family::Append) {
                    1
                } else {
                    0
                };
                if *soft || *conditions > allowed || *mutations != 1 {
                    return Err(StreamError::InvalidResponse);
                }
            }
            (
                Some(Family::Append),
                Phase::Condition,
                Token::Tail {
                    commit_id: None, ..
                },
            ) => {}
            (Some(Family::Append), Phase::Condition, _) => {
                return Err(StreamError::InvalidResponse);
            }
            (Some(Family::Append), Phase::Mutation, Token::Append { .. }) => {}
            (Some(Family::Fork), Phase::Mutation, Token::Fork { records: 0, .. }) => {}
            (Some(Family::Append | Family::Fork), Phase::Mutation, _) => {
                return Err(StreamError::InvalidResponse);
            }
            (Some(Family::Commit), Phase::Mutation, Token::Fork { at_tail: None, .. }) => {
                return Err(StreamError::InvalidResponse);
            }
            _ => {}
        }
        match (self.next(), token) {
            (
                Phase::Effect,
                Token::Effect {
                    conditions,
                    mutations,
                    ..
                },
            ) => {
                self.effects -= 1;
                self.conditions = *conditions;
                self.mutations = *mutations;
            }
            (Phase::Condition, Token::Presence { .. } | Token::Tail { .. }) => {
                self.conditions -= 1;
            }
            (Phase::Mutation, Token::Append { records, .. }) => {
                self.mutations -= 1;
                self.records = *records;
            }
            (
                Phase::Mutation,
                Token::Fork {
                    exclusions,
                    records,
                    ..
                },
            ) => {
                self.mutations -= 1;
                self.exclusions = *exclusions;
                self.records = *records;
            }
            (Phase::Exclusion, Token::Exclusion(_)) => {
                self.exclusions -= 1;
            }
            (
                Phase::Record,
                Token::Record {
                    keys, attributes, ..
                },
            ) => {
                self.records -= 1;
                self.keys = *keys;
                self.attributes = *attributes;
            }
            (Phase::Key, Token::Key(_)) => {
                self.keys -= 1;
            }
            (Phase::Attribute, Token::Attribute { .. }) => {
                self.attributes -= 1;
            }
            _ => return Err(StreamError::InvalidResponse),
        }
        Ok(())
    }
}

/// Lazy token cursor for one sealed submission. Conditions and path references
/// are observations to perform at final apply, never evaluated by this reader.
pub struct Reader<'a, S> {
    cursor: Cursor<'a, S>,
    binding: StagedWrite,
    deadline: Option<u64>,
    shape: Shape,
    interrupted: bool,
}

/// Opaque verified token boundary in one immutable staged submission.
/// Retains constant decoder metadata, not token or record prefixes. The stage
/// owner must keep its content alive; this is not an authorization credential.
#[derive(Clone)]
pub struct Bookmark {
    binding: StagedWrite,
    position: u64,
    deadline: Option<u64>,
    shape: Shape,
}

impl<'a, S: Storage> Reader<'a, S> {
    /// Verifies the immutable stage binding before exposing any request token.
    pub async fn open(storage: &'a S, staged: &StagedWrite) -> Result<Self, StreamError> {
        let mut cursor = Cursor::new(storage, staged.body)?;
        let magic = cursor.small(MAGIC.len()).await?;
        crate::persistence::header(magic.as_slice(), b"acyclic-stream-staged-v", b"1\0")?;
        if cursor.exact::<32>().await? != *staged.commit_id.as_bytes() {
            return Err(StreamError::InvalidResponse);
        }
        let family = match cursor.byte().await? {
            1 => Family::Append,
            2 => Family::Fork,
            3 => Family::Commit,
            _ => return Err(StreamError::InvalidResponse),
        };
        if family != staged.family {
            return Err(StreamError::InvalidResponse);
        }
        let key = cursor.field().await?;
        if !matches_field(storage, &key, staged.key.as_bytes()).await? {
            return Err(StreamError::IdempotencyMismatch);
        }
        let deadline = cursor.optional().await?;
        let effects = cursor.number().await?;
        if family != Family::Commit && (effects != 1 || deadline.is_some()) {
            return Err(StreamError::InvalidResponse);
        }
        Ok(Self {
            cursor,
            binding: staged.clone(),
            deadline,
            shape: Shape::new(family, effects),
            interrupted: false,
        })
    }

    /// Retains the next token boundary, refusing a failed or canceled cursor.
    pub fn bookmark(&self) -> Result<Bookmark, StreamError> {
        if self.interrupted {
            return Err(StreamError::Unavailable);
        }
        Ok(Bookmark {
            binding: self.binding.clone(),
            position: self.cursor.position,
            deadline: self.deadline,
            shape: self.shape.clone(),
        })
    }

    /// Reopens a verified boundary without decoding or retaining its prefix.
    /// Accepted-output projection may change; the input binding may not.
    pub fn resume(
        storage: &'a S,
        staged: &StagedWrite,
        bookmark: &Bookmark,
    ) -> Result<Self, StreamError> {
        let original = &bookmark.binding;
        if original.body != staged.body
            || original.commit_id != staged.commit_id
            || original.key != staged.key
            || original.family != staged.family
        {
            return Err(StreamError::InvalidArgument);
        }
        Ok(Self {
            cursor: Cursor::at(storage, staged.body, bookmark.position)?,
            binding: staged.clone(),
            deadline: bookmark.deadline,
            shape: bookmark.shape.clone(),
            interrupted: false,
        })
    }

    /// Logical byte offset of the next token boundary in the sealed body.
    pub fn position(&self) -> Result<u64, StreamError> {
        if self.interrupted {
            return Err(StreamError::Unavailable);
        }
        Ok(self.cursor.position)
    }

    /// Original deadline, to be checked only by final apply's authority.
    pub const fn deadline_unix_millis(&self) -> Option<u64> {
        self.deadline
    }

    /// Decodes one bounded token. Failure or cancellation retires this cursor;
    /// it cannot silently resume after a partially consumed token.
    pub async fn next(&mut self) -> Result<Option<Token>, StreamError> {
        if self.interrupted {
            return Err(StreamError::Unavailable);
        }
        self.interrupted = true;
        let result = self.next_inner().await;
        if result.is_ok() {
            self.interrupted = false;
        }
        result
    }

    async fn next_inner(&mut self) -> Result<Option<Token>, StreamError> {
        if self.shape.next() == Phase::End {
            self.cursor.end().await?;
            return Ok(None);
        }
        let token = match self.cursor.byte().await? {
            1 => Token::Effect {
                soft: self.cursor.boolean().await?,
                conditions: self.cursor.number().await?,
                mutations: self.cursor.number().await?,
            },
            2 => Token::Presence {
                path: self.cursor.field().await?,
                present: self.cursor.boolean().await?,
            },
            3 => Token::Tail {
                path: self.cursor.field().await?,
                expected: self.cursor.number().await?,
                commit_id: if self.cursor.boolean().await? {
                    Some(CommitId::from_bytes(self.cursor.exact().await?))
                } else {
                    None
                },
            },
            4 => Token::Append {
                path: self.cursor.field().await?,
                records: self.cursor.number().await?,
            },
            5 => Token::Fork {
                source: self.cursor.field().await?,
                destination: self.cursor.field().await?,
                at_tail: self.cursor.optional().await?,
                exclusions: self.cursor.number().await?,
                records: self.cursor.number().await?,
            },
            6 => Token::Exclusion(self.cursor.field().await?),
            7 => Token::Record {
                value: self.cursor.field().await?,
                keys: self.cursor.number().await?,
                attributes: self.cursor.number().await?,
            },
            8 => Token::Key(self.cursor.field().await?),
            9 => Token::Attribute {
                name: self.cursor.field().await?,
                value: self.cursor.field().await?,
            },
            _ => return Err(StreamError::InvalidResponse),
        };
        self.shape.accept(&token)?;
        Ok(Some(token))
    }
}

async fn matches_field(
    storage: &impl Storage,
    field: &Field,
    expected: &[u8],
) -> Result<bool, StreamError> {
    match field {
        Field::Inline(bytes) => Ok(bytes.as_slice() == expected),
        Field::Content(root) => {
            if root.part.length != expected.len() as u64 {
                return Ok(false);
            }
            let mut reader = tree::Reader::new(storage, *root)?;
            let mut offset = 0;
            while let Some(bytes) = reader.next().await? {
                let end = offset + bytes.len();
                if expected.get(offset..end) != Some(bytes.as_slice()) {
                    return Ok(false);
                }
                offset = end;
            }
            Ok(offset == expected.len())
        }
    }
}
