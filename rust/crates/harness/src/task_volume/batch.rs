use super::*;

impl<P: StreamProvider + ?Sized> TaskVolume<P> {
    /// Atomically publishes caller-owned files at one exact generation.
    /// Immutable requests are created once; editable files share the same fence.
    pub(crate) async fn append_many(
        &self,
        expected: Generation,
        writes: &[FileValue],
    ) -> Result<Generation> {
        self.refresh().await?;
        let index = self.index.lock().await;
        if expected != self.generation(&index) {
            return Err(Error::Conflict(self.generation(&index)));
        }
        let mut keys = BTreeSet::new();
        let mut records = Vec::with_capacity(writes.len());
        for file in writes {
            if file.key.owner() != Owner::Caller || file.requester.is_some() {
                return Err(Error::Unauthorized);
            }
            if !keys.insert(&file.key) {
                return Err(Error::InvalidSegment);
            }
            if !file.key.mutable() && index.files.contains_key(&file.key) {
                return Err(Error::ImmutableConflict(file.key.clone()));
            }
            validate_file(&index, file)?;
            records.push(segment::encode(file, None)?.0);
        }
        if records.is_empty() {
            return Ok(expected);
        }
        let next = expected
            .position
            .checked_add(records.len() as u64)
            .ok_or(Error::LimitExceeded)?;
        let request = CommitRequest {
            conditions: vec![CommitCondition::Head {
                path: self.path.clone(),
                expected: expected.position,
                commit_id: CommitId::from_bytes(expected.commit),
            }],
            mutations: vec![CommitMutation::Append {
                path: self.path.clone(),
                records,
            }],
            idempotency_key: IdempotencyKey::new(Uuid::new_v4().as_bytes().to_vec())?,
        };
        drop(index);
        self.commit_append(expected, request, writes.iter().cloned(), next, None)
            .await
    }
}
