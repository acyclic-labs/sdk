use super::*;

struct Encoder<'a, S> {
    storage: &'a S,
    output: tree::Writer<'a, S>,
}

impl<S: Storage> Encoder<'_, S> {
    async fn byte(&mut self, value: u8) -> Result<(), StreamError> {
        self.output.write(&[value]).await
    }
    async fn number(&mut self, value: u64) -> Result<(), StreamError> {
        self.output.write(&value.to_le_bytes()).await
    }
    async fn count(&mut self, value: usize) -> Result<(), StreamError> {
        self.number(u64::try_from(value).map_err(|_| StreamError::Capacity)?)
            .await
    }
    async fn optional(&mut self, value: Option<u64>) -> Result<(), StreamError> {
        self.byte(u8::from(value.is_some())).await?;
        if let Some(value) = value {
            self.number(value).await?;
        }
        Ok(())
    }
    async fn field(&mut self, value: &[u8]) -> Result<(), StreamError> {
        if value.len() <= INLINE_BYTES {
            self.byte(0).await?;
            self.count(value.len()).await?;
            self.output.write(value).await
        } else {
            let mut content = tree::Writer::new(self.storage)?;
            content.write(value).await?;
            let root = content.finish().await?;
            self.byte(1).await?;
            self.output.write(&root.part.digest).await?;
            self.number(root.part.length).await?;
            self.byte(root.height).await
        }
    }
    async fn effect(
        &mut self,
        soft: bool,
        conditions: usize,
        mutations: usize,
    ) -> Result<(), StreamError> {
        self.byte(1).await?;
        self.byte(u8::from(soft)).await?;
        self.count(conditions).await?;
        self.count(mutations).await
    }
    async fn condition(&mut self, condition: &CommitCondition) -> Result<(), StreamError> {
        match condition {
            CommitCondition::Present { path } | CommitCondition::Absent { path } => {
                self.byte(2).await?;
                self.field(path.as_str().as_bytes()).await?;
                self.byte(u8::from(matches!(
                    condition,
                    CommitCondition::Present { .. }
                )))
                .await
            }
            CommitCondition::Tail { path, expected } => self.tail(path, *expected, None).await,
            CommitCondition::Head {
                path,
                expected,
                commit_id,
            } => self.tail(path, *expected, Some(*commit_id)).await,
        }
    }
    async fn tail(
        &mut self,
        path: &crate::StreamPath,
        expected: u64,
        commit: Option<CommitId>,
    ) -> Result<(), StreamError> {
        self.byte(3).await?;
        self.field(path.as_str().as_bytes()).await?;
        self.number(expected).await?;
        self.byte(u8::from(commit.is_some())).await?;
        if let Some(commit) = commit {
            self.output.write(commit.as_bytes()).await?;
        }
        Ok(())
    }
    async fn records(&mut self, records: &[crate::AppendRecord]) -> Result<(), StreamError> {
        for record in records {
            self.byte(7).await?;
            self.field(record.value.as_slice()).await?;
            self.count(record.headers.keys().len()).await?;
            self.count(record.headers.attributes().len()).await?;
            for key in record.headers.keys() {
                self.byte(8).await?;
                self.field(key.as_slice()).await?;
            }
            for attribute in record.headers.attributes() {
                self.byte(9).await?;
                self.field(attribute.name.as_bytes()).await?;
                self.field(attribute.value.as_slice()).await?;
            }
        }
        Ok(())
    }
    async fn append(
        &mut self,
        path: &crate::StreamPath,
        records: &[crate::AppendRecord],
    ) -> Result<(), StreamError> {
        self.byte(4).await?;
        self.field(path.as_str().as_bytes()).await?;
        self.count(records.len()).await?;
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
        self.byte(5).await?;
        self.field(source.as_str().as_bytes()).await?;
        self.field(destination.as_str().as_bytes()).await?;
        self.optional(at_tail).await?;
        self.count(exclude.len()).await?;
        self.count(records.len()).await?;
        for path in exclude {
            self.byte(6).await?;
            self.field(path.as_str().as_bytes()).await?;
        }
        self.records(records).await
    }
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
            request.effects.len(),
        ),
    };
    // Accepted-envelope selection is observation-only, outside the hashed body.
    let accepted = matches!(request, WriteRequest::CommitAccepted(_));
    let mut encoder = Encoder {
        storage,
        output: tree::Writer::new(storage)?,
    };
    encoder.output.write(MAGIC).await?;
    encoder.output.write(commit_id.as_bytes()).await?;
    encoder
        .byte(match family {
            Family::Append => 1,
            Family::Fork => 2,
            Family::Commit => 3,
        })
        .await?;
    encoder.field(key.as_bytes()).await?;
    encoder.optional(deadline).await?;
    encoder.count(effects).await?;
    match request {
        WriteRequest::Append(request) => {
            encoder
                .effect(false, usize::from(request.if_tail.is_some()), 1)
                .await?;
            if let Some(tail) = request.if_tail {
                encoder.tail(&request.path, tail, None).await?;
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
                                .await?;
                        }
                    }
                }
            }
        }
    }
    let body = encoder.output.finish().await?;
    Ok(StagedWrite {
        commit_id,
        key: key.clone(),
        family,
        body,
        accepted,
    })
}
