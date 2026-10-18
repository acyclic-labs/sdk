use super::*;

pub(super) const PUBLICATION_ATTEMPTS: usize = 8;

impl<P: StreamProvider + ?Sized> CommandSession<P> {
    pub(super) async fn publish_many<const RETRY: bool>(
        &mut self,
        writes: &[files::FileValue],
    ) -> Result<()> {
        #[cfg(acyclic_harness_loom)]
        let writes =
            if files::model_checking::enabled(files::model_checking::Fault::OmitDraftConsumption) {
                &writes[..writes.len() - 1]
            } else {
                writes
            };
        for file in writes {
            if file.bytes.len() as u64 > self.maximum_bytes
                || files::record_bytes(file)? > acyclic_stream::MAX_RECORD_BYTES
            {
                return Err(Error::Invalid(
                    "command publication exceeds byte bound".into(),
                ));
            }
        }
        let before: Vec<_> = writes
            .iter()
            .map(|f| self.files.get(&f.key).cloned())
            .collect();
        for _ in 0..PUBLICATION_ATTEMPTS {
            self.make_room(
                writes
                    .iter()
                    .filter(|f| !self.files.contains_key(&f.key))
                    .count(),
            )?;
            let expected = self.generation;
            let result = match &self.writer {
                Some(writer) => writer.write_many(expected, writes).await,
                None => self.volume.append_many(expected, writes).await,
            };
            match result {
                Ok(generation) => {
                    while self.generation.position < generation.position {
                        self.advance().await?;
                    }
                    return Ok(());
                }
                Err(files::Error::Conflict(generation)) => {
                    while self.generation.position < generation.position {
                        self.advance().await?;
                    }
                    if !RETRY
                        || self.generation == expected
                        || writes
                            .iter()
                            .zip(&before)
                            .any(|(f, old)| self.files.get(&f.key) != old.as_ref())
                    {
                        return Err(Error::Conflict(
                            "command files changed; retry from the new cut".into(),
                        ));
                    }
                }
                Err(error) => return Err(error.into()),
            }
        }
        Err(Error::Conflict("command publication remained busy".into()))
    }

    pub(super) fn publication_byte_limit(&self, key: files::FileKey) -> Result<usize> {
        let framing = files::record_bytes(&files::FileValue::new(key, Vec::new()))?;
        Ok(usize::try_from(self.maximum_bytes)
            .unwrap_or(usize::MAX)
            .min(acyclic_stream::MAX_RECORD_BYTES.saturating_sub(framing)))
    }

    pub(super) async fn publish(
        &mut self,
        key: files::FileKey,
        bytes: Vec<u8>,
        create_only: bool,
    ) -> Result<()> {
        self.publish_rebased::<false, true>(files::FileValue::new(key, bytes), create_only)
            .await
    }

    /// Persist an editor projection across unrelated owner publications. The
    /// observed target contents are still a precondition; peer edits fail closed.
    /// Non-input resolution and execution claims retain their strict query CAS.
    pub(crate) async fn write_unchanged_state(
        &mut self,
        root: CommandFileRoot,
        name: &str,
        value: &Value,
    ) -> Result<()> {
        self.publish_rebased::<true, true>(
            files::FileValue::new(
                self.state_key(root, name)?,
                serde_json::to_vec(value).map_err(codec)?,
            ),
            false,
        )
        .await
    }

    // Rebase across unrelated publications only. A peer's same-file update
    // remains authoritative; immutable execution claims still have one winner.
    // The const variants are the real-path models' two negative controls.
    pub(super) async fn publish_rebased<const RETRY: bool, const MATCH_KEY: bool>(
        &mut self,
        file: files::FileValue,
        create_only: bool,
    ) -> Result<()> {
        #[cfg(all(test, not(acyclic_harness_loom)))]
        let file = tests::handoff(file);
        if file.bytes.len() as u64 > self.maximum_bytes
            || files::record_bytes(&file)? > acyclic_stream::MAX_RECORD_BYTES
        {
            return Err(Error::Invalid(
                "command publication exceeds byte bound".into(),
            ));
        }
        let before = self.files.get(&file.key).cloned();
        if create_only && before.is_some() {
            if RETRY && before.as_ref() == Some(&file) {
                return Ok(());
            }
            return Err(Error::Conflict("command file already exists".into()));
        }
        for _ in 0..PUBLICATION_ATTEMPTS {
            self.make_room(usize::from(!self.files.contains_key(&file.key)))?;
            let expected = self.generation;
            let published = if let Some(writer) = &self.writer {
                writer.write(expected, &file).await
            } else if create_only {
                self.volume.create(expected, &file).await
            } else {
                self.volume.append(expected, &file).await
            };
            match published {
                Ok(generation) => {
                    while self.generation.position < generation.position {
                        self.advance().await?;
                    }
                    return Ok(());
                }
                Err(files::Error::Conflict(generation)) => {
                    while self.generation.position < generation.position {
                        self.advance().await?;
                    }
                    if RETRY && create_only && self.files.get(&file.key) == Some(&file) {
                        return Ok(());
                    }
                    if !RETRY
                        || self.generation == expected
                        || (MATCH_KEY && self.files.get(&file.key) != before.as_ref())
                    {
                        return Err(Error::Conflict(
                            "command query advanced; retry from the new cut".into(),
                        ));
                    }
                }
                Err(error) => return Err(error.into()),
            }
        }
        Err(Error::Conflict(
            "command query advanced; retry from the new cut".into(),
        ))
    }
}

#[cfg(all(test, not(acyclic_harness_loom)))]
#[path = "session_publish_tests.rs"]
mod tests;
