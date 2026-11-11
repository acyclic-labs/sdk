use super::*;
use futures::StreamExt as _;

struct Encoder<'a, S> {
    storage: &'a S,
    output: tree::Writer<'a, S>,
    submission: Submission,
    shape: super::decode::Shape,
}

fn count(value: usize) -> Result<u64, StreamError> {
    u64::try_from(value).map_err(|_| StreamError::Capacity)
}
fn path(value: &crate::StreamPath) -> Field<&[u8]> {
    Field::Inline(value.as_str().as_bytes())
}

impl<'a, S: Storage> Encoder<'a, S> {
    async fn start(storage: &'a S, submission: Submission) -> Result<Self, StreamError> {
        if submission.family != Family::Commit
            && (submission.effects != 1
                || submission.deadline_unix_millis.is_some()
                || submission.accepted)
        {
            return Err(StreamError::InvalidArgument);
        }
        let mut encoder = Self {
            storage,
            output: tree::Writer::new(storage)?,
            shape: super::decode::Shape::new(submission.family, submission.effects),
            submission,
        };
        encoder.output.write(MAGIC).await?;
        encoder
            .output
            .write(encoder.submission.commit_id.as_bytes())
            .await?;
        encoder
            .byte(match encoder.submission.family {
                Family::Append => 1,
                Family::Fork => 2,
                Family::Commit => 3,
            })
            .await?;
        // Retain the original key owner while writing, without copying its bytes.
        let key = encoder.submission.idempotency_key.clone();
        encoder.bytes(key.as_bytes()).await?;
        encoder
            .optional(encoder.submission.deadline_unix_millis)
            .await?;
        encoder.number(encoder.submission.effects).await?;
        Ok(encoder)
    }
    async fn byte(&mut self, value: u8) -> Result<(), StreamError> {
        self.output.write(&[value]).await
    }
    async fn number(&mut self, value: u64) -> Result<(), StreamError> {
        self.output.write(&value.to_le_bytes()).await
    }
    async fn optional(&mut self, value: Option<u64>) -> Result<(), StreamError> {
        self.byte(u8::from(value.is_some())).await?;
        if let Some(value) = value {
            self.number(value).await?;
        }
        Ok(())
    }
    async fn root(&mut self, root: Root) -> Result<(), StreamError> {
        if root.height > u64::BITS as u8 {
            return Err(StreamError::InvalidArgument);
        }
        self.output.reference(root).await?;
        self.byte(1).await?;
        self.output.write(&root.part.digest).await?;
        self.number(root.part.length).await?;
        self.byte(root.height).await
    }
    async fn bytes(&mut self, value: &[u8]) -> Result<(), StreamError> {
        if value.len() <= INLINE_BYTES {
            self.byte(0).await?;
            self.number(count(value.len())?).await?;
            self.output.write(value).await
        } else {
            let mut content = tree::Writer::new(self.storage)?;
            content.write(value).await?;
            self.root(content.finish().await?).await
        }
    }
    async fn field<B: AsRef<[u8]>>(&mut self, field: &Field<B>) -> Result<(), StreamError> {
        match field {
            Field::Inline(bytes) => self.bytes(bytes.as_ref()).await,
            Field::Content(root) => self.root(*root).await,
        }
    }
    async fn token<B: AsRef<[u8]>>(&mut self, token: &Token<B>) -> Result<(), StreamError> {
        self.shape
            .accept(token)
            .map_err(|_| StreamError::InvalidArgument)?;
        match token {
            Token::Effect {
                soft,
                conditions,
                mutations,
            } => {
                self.byte(1).await?;
                self.byte(u8::from(*soft)).await?;
                self.number(*conditions).await?;
                self.number(*mutations).await
            }
            Token::Presence { path, present } => {
                self.byte(2).await?;
                self.field(path).await?;
                self.byte(u8::from(*present)).await
            }
            Token::Tail {
                path,
                expected,
                commit_id,
            } => {
                self.byte(3).await?;
                self.field(path).await?;
                self.number(*expected).await?;
                self.byte(u8::from(commit_id.is_some())).await?;
                if let Some(commit_id) = commit_id {
                    self.output.write(commit_id.as_bytes()).await?;
                }
                Ok(())
            }
            Token::Append { path, records } => {
                self.byte(4).await?;
                self.field(path).await?;
                self.number(*records).await
            }
            Token::Fork {
                source,
                destination,
                at_tail,
                exclusions,
                records,
            } => {
                self.byte(5).await?;
                self.field(source).await?;
                self.field(destination).await?;
                self.optional(*at_tail).await?;
                self.number(*exclusions).await?;
                self.number(*records).await
            }
            Token::Exclusion(path) => {
                self.byte(6).await?;
                self.field(path).await
            }
            Token::Record {
                value,
                keys,
                attributes,
            } => {
                self.byte(7).await?;
                self.field(value).await?;
                self.number(*keys).await?;
                self.number(*attributes).await
            }
            Token::Key(key) => {
                self.byte(8).await?;
                self.field(key).await
            }
            Token::Attribute { name, value } => {
                self.byte(9).await?;
                self.field(name).await?;
                self.field(value).await
            }
        }
    }
    async fn finish(self) -> Result<StagedWrite, StreamError> {
        if !self.shape.complete() {
            return Err(StreamError::InvalidArgument);
        }
        let body = self.output.finish().await?;
        Ok(StagedWrite {
            body,
            commit_id: self.submission.commit_id,
            key: self.submission.idempotency_key,
            family: self.submission.family,
            accepted: self.submission.accepted,
        })
    }
    async fn effect(
        &mut self,
        soft: bool,
        conditions: usize,
        mutations: usize,
    ) -> Result<(), StreamError> {
        self.token::<&[u8]>(&Token::Effect {
            soft,
            conditions: count(conditions)?,
            mutations: count(mutations)?,
        })
        .await
    }
    async fn condition(&mut self, condition: &CommitCondition) -> Result<(), StreamError> {
        let token = match condition {
            CommitCondition::Present { path: value } => Token::Presence {
                path: path(value),
                present: true,
            },
            CommitCondition::Absent { path: value } => Token::Presence {
                path: path(value),
                present: false,
            },
            CommitCondition::Tail {
                path: value,
                expected,
            } => Token::Tail {
                path: path(value),
                expected: *expected,
                commit_id: None,
            },
            CommitCondition::Head {
                path: value,
                expected,
                commit_id,
            } => Token::Tail {
                path: path(value),
                expected: *expected,
                commit_id: Some(*commit_id),
            },
        };
        self.token(&token).await
    }
    async fn records(&mut self, records: &[crate::AppendRecord]) -> Result<(), StreamError> {
        for record in records {
            self.token(&Token::Record {
                value: Field::Inline(record.value.as_slice()),
                keys: count(record.headers.keys().len())?,
                attributes: count(record.headers.attributes().len())?,
            })
            .await?;
            for key in record.headers.keys() {
                self.token(&Token::Key(Field::Inline(key.as_slice())))
                    .await?;
            }
            for attribute in record.headers.attributes() {
                self.token(&Token::Attribute {
                    name: Field::Inline(attribute.name.as_bytes()),
                    value: Field::Inline(attribute.value.as_slice()),
                })
                .await?;
            }
        }
        Ok(())
    }
    async fn append(
        &mut self,
        destination: &crate::StreamPath,
        records: &[crate::AppendRecord],
    ) -> Result<(), StreamError> {
        self.token(&Token::Append {
            path: path(destination),
            records: count(records.len())?,
        })
        .await?;
        self.records(records).await
    }
    async fn fork(
        &mut self,
        source: &crate::StreamPath,
        destination: &crate::StreamPath,
        at_tail: Option<u64>,
        exclude: &[crate::RelativePath],
        records: &[crate::AppendRecord],
    ) -> Result<(), StreamError> {
        self.token(&Token::Fork {
            source: path(source),
            destination: path(destination),
            at_tail,
            exclusions: count(exclude.len())?,
            records: count(records.len())?,
        })
        .await?;
        for value in exclude {
            self.token(&Token::Exclusion(path(value))).await?;
        }
        self.records(records).await
    }
}

pub(super) async fn stage_tokens<B: AsRef<[u8]>>(
    storage: &impl Storage,
    submission: Submission,
    tokens: impl futures::Stream<Item = Result<Token<B>, StreamError>>,
) -> Result<StagedWrite, StreamError> {
    let mut encoder = Encoder::start(storage, submission).await?;
    futures::pin_mut!(tokens);
    while let Some(token) = tokens.next().await {
        encoder.token(&token?).await?;
    }
    encoder.finish().await
}

pub(super) async fn stage(
    storage: &impl Storage,
    commit_id: CommitId,
    request: &WriteRequest,
) -> Result<StagedWrite, StreamError> {
    let (family, key, deadline, effects) = match request {
        WriteRequest::Append(request) => (Family::Append, &request.idempotency_key, None, 1),
        WriteRequest::Fork(request) => (Family::Fork, &request.idempotency_key, None, 1),
        WriteRequest::Commit(request) | WriteRequest::CommitAccepted(request) => (
            Family::Commit,
            &request.idempotency_key,
            request.deadline_unix_millis,
            count(request.effects.len())?,
        ),
    };
    let mut encoder = Encoder::start(
        storage,
        Submission {
            commit_id,
            idempotency_key: key.clone(),
            family,
            deadline_unix_millis: deadline,
            effects,
            accepted: matches!(request, WriteRequest::CommitAccepted(_)),
        },
    )
    .await?;
    match request {
        WriteRequest::Append(request) => {
            encoder
                .effect(false, usize::from(request.if_tail.is_some()), 1)
                .await?;
            if let Some(expected) = request.if_tail {
                encoder
                    .token(&Token::Tail {
                        path: path(&request.path),
                        expected,
                        commit_id: None,
                    })
                    .await?;
            }
            encoder.append(&request.path, &request.records).await?;
        }
        WriteRequest::Fork(request) => {
            encoder.effect(false, 0, 1).await?;
            encoder
                .fork(
                    &request.source,
                    &request.destination,
                    request.at_tail,
                    &request.exclude,
                    &[],
                )
                .await?;
        }
        WriteRequest::Commit(request) | WriteRequest::CommitAccepted(request) => {
            for effect in &request.effects {
                encoder
                    .effect(effect.soft, effect.conditions.len(), effect.mutations.len())
                    .await?;
                for condition in &effect.conditions {
                    encoder.condition(condition).await?;
                }
                for mutation in &effect.mutations {
                    match mutation {
                        CommitMutation::Append { path, records } => {
                            encoder.append(path, records).await?
                        }
                        CommitMutation::Fork {
                            source,
                            destination,
                            at_tail,
                            exclude,
                            records,
                        } => {
                            encoder
                                .fork(source, destination, Some(*at_tail), exclude, records)
                                .await?
                        }
                    }
                }
            }
        }
    }
    encoder.finish().await
}
