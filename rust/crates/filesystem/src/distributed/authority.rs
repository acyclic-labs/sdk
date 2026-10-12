use super::*;

impl<P: acyclic_stream::StreamProvider> StreamAuthorityStore<P> {
    /// Lists up to `maximum` exact filesystem authority identities, in order.
    pub async fn authorities(&self, maximum: u32) -> Result<Vec<AuthorityId>, AuthorityStoreError> {
        /// Restarts tolerated when authorities come and go mid-listing.
        const RESTARTS: usize = 16;
        let parent = acyclic_stream::StreamPath::new("fs/authorities").map_err(map_stream_error)?;
        let page = u32::try_from(acyclic_stream::MAX_ITEMS).unwrap_or(u32::MAX);
        'listing: for _ in 0..RESTARTS {
            let mut authorities = Vec::new();
            let mut after = None;
            let mut hierarchy_version = None;
            loop {
                let listed = match self
                    .provider
                    .children_page(acyclic_stream::ChildrenPageRequest {
                        parent: Some(parent.clone()),
                        after: after.take(),
                        hierarchy_version,
                        limit: page,
                    })
                    .await
                {
                    Ok(listed) => listed,
                    Err(acyclic_stream::StreamError::HierarchyChanged) => continue 'listing,
                    Err(error) => return Err(map_stream_error(error)),
                };
                for child in &listed.children {
                    let authority = authority_child(&child.path)?;
                    if !self.is_retired(authority).await? {
                        authorities.push(authority);
                    }
                }
                if authorities.len() > maximum as usize {
                    return Err(AuthorityStoreError::Rejected(
                        "authority listing bound exceeded".to_owned(),
                    ));
                }
                let Some(next) = listed.next_after else {
                    return Ok(authorities);
                };
                after = Some(next);
                hierarchy_version = Some(listed.hierarchy_version);
            }
        }
        Err(AuthorityStoreError::Rejected(
            "authorities kept changing while they were listed".to_owned(),
        ))
    }

    pub(super) async fn is_retired(
        &self,
        authority_id: AuthorityId,
    ) -> Result<bool, AuthorityStoreError> {
        let path = stream_path(authority_id, "/retirement")?;
        match self.provider.tail(path.clone()).await {
            Err(acyclic_stream::StreamError::NotFound) => Ok(false),
            Ok(1) => {
                let marker = read_one(self.provider.as_ref(), path, 0).await?;
                if marker.value.as_ref() != AUTHORITY_RETIRED {
                    return Err(AuthorityStoreError::Corrupt(
                        "filesystem retirement marker is invalid".to_owned(),
                    ));
                }
                Ok(true)
            }
            Ok(_) => Err(AuthorityStoreError::Corrupt(
                "filesystem retirement marker has an invalid tail".to_owned(),
            )),
            Err(error) => Err(map_stream_error(error)),
        }
    }

    pub(super) async fn tail_of(
        &self,
        authority: AuthorityId,
        suffix: &str,
        work: &mut AuthorityWork<'_>,
    ) -> Result<u64, AuthorityStoreError> {
        let path = work.path(authority, suffix, None)?;
        let bytes = path.as_str().len() as u64;
        let result = self.tail(path, work).await;
        work.release(bytes)?;
        result
    }

    pub(super) async fn read_at<T>(
        &self,
        authority: AuthorityId,
        suffix: &str,
        from: u64,
        work: &mut AuthorityWork<'_>,
        decode: impl FnOnce(
            &acyclic_stream::Record,
            &mut AuthorityWork<'_>,
        ) -> Result<T, AuthorityStoreError>,
    ) -> Result<T, AuthorityStoreError> {
        let path = work.path(authority, suffix, None)?;
        let bytes = path.as_str().len() as u64;
        let result = self.record(path, from, work).await;
        work.release(bytes)?;
        let record = result?;
        decode(&record.0, work)
    }

    pub(super) async fn fork_snapshot(
        &self,
        authority: AuthorityId,
        work: &mut AuthorityWork<'_>,
    ) -> Result<AuthoritySnapshot, AuthorityStoreError> {
        let baseline = work.allocations.live_bytes();
        let result = async {
            self.authority_require_live(authority, work).await?;
            self.snapshot(authority, work).await
        }
        .await;
        if result.is_err() && !work.is_native() {
            work.release(
                work.allocations
                    .live_bytes()
                    .checked_sub(baseline)
                    .ok_or(crate::WorkError::Overflow)?,
            )?;
        }
        result
    }

    pub(super) async fn snapshot(
        &self,
        authority: AuthorityId,
        work: &mut AuthorityWork<'_>,
    ) -> Result<AuthoritySnapshot, AuthorityStoreError> {
        let record_tail = self.tail_of(authority, "/records", work).await?;
        let epoch_tail = self.tail_of(authority, "/epochs", work).await?;
        if record_tail == 0 || epoch_tail == 0 {
            return Err(AuthorityStoreError::CorruptStatic(
                "authority genesis paths are empty",
            ));
        }
        let epoch = self
            .read_at(
                authority,
                "/epochs",
                epoch_tail - 1,
                work,
                |record, work| work.decode_epoch(&record.value, EPOCH_DOMAIN),
            )
            .await?;
        let head = self
            .read_at(
                authority,
                "/records",
                record_tail - 1,
                work,
                |record, work| {
                    if record_tail == 1 {
                        work.decode_epoch(&record.value, GENESIS_DOMAIN)?;
                        return Ok(Head::genesis(epoch));
                    }
                    let durable = work.decode_durable(&record.value)?;
                    if work.durable_digest(authority, &durable)? != durable.digest
                        || durable.sequence.get().checked_add(1) != Some(record_tail)
                    {
                        return Err(AuthorityStoreError::CorruptStatic(
                            "Stream authority record or sequence mismatch",
                        ));
                    }
                    Ok(Head {
                        epoch,
                        sequence: durable.sequence,
                        digest: durable.digest,
                    })
                },
            )
            .await?;
        Ok(AuthoritySnapshot { head, epoch_tail })
    }

    pub(super) async fn authority_require_live(
        &self,
        authority: AuthorityId,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), AuthorityStoreError> {
        match self.tail_of(authority, "/retirement", work).await {
            Err(error) if authority_missing(&error) => Ok(()),
            Ok(1) => {
                self.read_at(authority, "/retirement", 0, work, |record, _| {
                    if record.value.as_ref() == AUTHORITY_RETIRED {
                        Ok(())
                    } else {
                        Err(AuthorityStoreError::CorruptStatic(
                            "filesystem retirement marker is invalid",
                        ))
                    }
                })
                .await?;
                Err(AuthorityStoreError::Retired)
            }
            Ok(_) => Err(AuthorityStoreError::CorruptStatic(
                "filesystem retirement marker has an invalid tail",
            )),
            Err(error) => Err(error),
        }
    }

    pub(super) async fn fork_gate(
        &self,
        authority: AuthorityId,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), AuthorityStoreError> {
        let tail = self
            .tail_of(authority, "/publication-gate", work)
            .await
            .map_err(|error| {
                if authority_missing(&error) {
                    authority_corrupt("authority publication gate is missing", work)
                } else {
                    error
                }
            })?;
        let from = tail
            .checked_sub(1)
            .ok_or(AuthorityStoreError::CorruptStatic(
                "authority publication gate is empty",
            ))?;
        self.read_at(authority, "/publication-gate", from, work, |record, _| {
            decode_publication_gate(&record.value).map(|_| ())
        })
        .await
    }

    pub(super) async fn existing_fork_destination(
        &self,
        authority: AuthorityId,
        generation: GenerationId,
        forked_at: u64,
        work: &mut AuthorityWork<'_>,
    ) -> Result<Option<AuthoritySnapshot>, AuthorityStoreError> {
        let snapshot = match self.fork_snapshot(authority, work).await {
            Ok(snapshot) => snapshot,
            Err(error) if authority_missing(&error) => return Ok(None),
            Err(error) => return Err(error),
        };
        let locator = work.path(authority, "/generations/", Some(generation))?;
        let bytes = locator.as_str().len() as u64;
        let record = self.record(locator, 0, work).await?;
        work.release(bytes)?;
        let tail = decode_lineage_tail(&record.0.value)?;
        drop(record);
        let from = tail.checked_sub(1).ok_or_else(|| {
            authority_corrupt(
                "destination generation locator selected an empty lineage",
                work,
            )
        })?;
        let selected = self
            .read_at(authority, "/lineage", from, work, |record, _| {
                decode_lineage_generation(&record.value)
            })
            .await?;
        if tail == forked_at && selected == generation {
            Ok(Some(snapshot))
        } else {
            Err(AuthorityStoreError::RejectedStatic(
                "destination generation fork conflicts with existing state",
            ))
        }
    }

    pub(super) async fn resolve_source_fork_point(
        &self,
        authority: AuthorityId,
        generation: GenerationId,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(StreamPath, u64), AuthorityStoreError> {
        let lineage = work.path(authority, "/lineage", None)?;
        let locator = work.path(authority, "/generations/", Some(generation))?;
        let bytes = locator.as_str().len() as u64;
        let record = self.record(locator, 0, work).await.map_err(|error| {
            if authority_missing(&error) && !work.is_native() {
                AuthorityStoreError::RejectedStatic(
                    "source authority has no generation lineage locator",
                )
            } else {
                error
            }
        })?;
        work.release(bytes)?;
        let tail = decode_lineage_tail(&record.0.value)?;
        drop(record);
        let from = tail.checked_sub(1).ok_or_else(|| {
            authority_corrupt("generation locator selected an empty lineage", work)
        })?;
        let record = self.record(lineage.clone(), from, work).await?;
        if decode_lineage_generation(&record.0.value)? != generation {
            return Err(authority_corrupt(
                "generation locator and lineage disagree",
                work,
            ));
        }
        Ok((lineage, tail))
    }
}
#[cfg(test)]
impl<P: acyclic_stream::StreamProvider> StreamAuthorityStore<P> {
    /// Every generation in one authority's lineage, oldest first.
    pub(crate) async fn generation_lineage(
        &self,
        authority: AuthorityId,
    ) -> Result<Vec<GenerationId>, AuthorityStoreError> {
        let path = stream_path(authority, "/lineage")?;
        let tail = self
            .provider
            .tail(path.clone())
            .await
            .map_err(map_stream_error)?;
        let mut lineage = Vec::new();
        for sequence in 0..tail {
            let record = read_one(self.provider.as_ref(), path.clone(), sequence).await?;
            lineage.push(decode_lineage_generation(&record.value)?);
        }
        Ok(lineage)
    }

    /// The lineage length at which one generation is located.
    pub(crate) async fn generation_locator(
        &self,
        authority: AuthorityId,
        generation: GenerationId,
    ) -> Result<u64, AuthorityStoreError> {
        let path = stream_path(
            authority,
            &format!(
                "/generations/{}",
                hex::encode(generation.digest().as_bytes())
            ),
        )?;
        let record = read_one(self.provider.as_ref(), path, 0).await?;
        decode_lineage_tail(&record.value)
    }
}
