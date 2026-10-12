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

    pub(super) async fn historical_head(
        &self,
        authority_id: AuthorityId,
    ) -> Result<Head, AuthorityStoreError> {
        let records = stream_path(authority_id, "/records")?;
        let epochs = stream_path(authority_id, "/epochs")?;
        let record_tail = self
            .provider
            .tail(records.clone())
            .await
            .map_err(map_stream_error)?;
        let epoch_tail = self
            .provider
            .tail(epochs.clone())
            .await
            .map_err(map_stream_error)?;
        if record_tail == 0 || epoch_tail == 0 {
            return Err(AuthorityStoreError::Corrupt(
                "authority genesis paths are empty".to_owned(),
            ));
        }
        let epoch_record = read_one(self.provider.as_ref(), epochs, epoch_tail - 1).await?;
        let epoch = decode_epoch(&epoch_record.value, EPOCH_DOMAIN, false)?;
        Ok(if record_tail == 1 {
            let genesis = read_one(self.provider.as_ref(), records, 0).await?;
            let _genesis_epoch = decode_epoch(&genesis.value, GENESIS_DOMAIN, false)?;
            Head::genesis(epoch)
        } else {
            let record = read_one(self.provider.as_ref(), records, record_tail - 1).await?;
            let durable = decode_record(&record)?;
            validate_record(authority_id, &record, &durable)?;
            if durable.sequence.get().saturating_add(1) != record_tail {
                return Err(AuthorityStoreError::Corrupt(
                    "Stream sequence does not match filesystem authority sequence".to_owned(),
                ));
            }
            Head {
                epoch,
                sequence: durable.sequence,
                digest: durable.digest,
            }
        })
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

    pub(super) async fn fork_snapshot(
        &self,
        authority: AuthorityId,
        meter: &mut AuthorityWork<'_>,
    ) -> Result<AuthoritySnapshot, AuthorityStoreError> {
        let baseline = if meter.is_native() {
            0
        } else {
            meter.allocations.live_bytes()
        };
        let result = async {
            self.authority_require_live(authority, meter).await?;
            let records = meter.path(authority, "/records", None)?;
            let epochs = meter.path(authority, "/epochs", None)?;
            let record_tail = self.tail(records.clone(), meter).await?;
            let epoch_tail = self.tail(epochs.clone(), meter).await?;
            if record_tail == 0 || epoch_tail == 0 {
                return Err(AuthorityStoreError::CorruptStatic(
                    "authority genesis paths are empty",
                ));
            }
            let epoch_record = self.record(epochs.clone(), epoch_tail - 1, meter).await?;
            let epoch = meter.decode_epoch(&epoch_record.0.value, EPOCH_DOMAIN)?;
            drop(epoch_record);
            let epochs_bytes = epochs.as_str().len() as u64;
            drop(epochs);
            meter.release(epochs_bytes)?;
            let record = self.record(records.clone(), record_tail - 1, meter).await?;
            let head = if record_tail == 1 {
                meter.decode_epoch(&record.0.value, GENESIS_DOMAIN)?;
                Head::genesis(epoch)
            } else {
                let durable = meter.decode_durable(&record.0.value)?;
                let digest = meter.durable_digest(authority, &durable)?;
                if digest != durable.digest
                    || durable.sequence.get().checked_add(1) != Some(record_tail)
                {
                    return Err(AuthorityStoreError::CorruptStatic(
                        "Stream authority record or sequence mismatch",
                    ));
                }
                Head {
                    epoch,
                    sequence: durable.sequence,
                    digest: durable.digest,
                }
            };
            drop(record);
            let records_bytes = records.as_str().len() as u64;
            drop(records);
            meter.release(records_bytes)?;
            Ok(AuthoritySnapshot { head, epoch_tail })
        }
        .await;
        if result.is_err() && !meter.is_native() {
            let remaining = meter
                .allocations
                .live_bytes()
                .checked_sub(baseline)
                .ok_or(crate::WorkError::Overflow)?;
            meter.release(remaining)?;
        }
        result
    }

    pub(super) async fn authority_require_live(
        &self,
        authority: AuthorityId,
        meter: &mut AuthorityWork<'_>,
    ) -> Result<(), AuthorityStoreError> {
        let retirement = meter.path(authority, "/retirement", None)?;
        let retired = match self.tail(retirement.clone(), meter).await {
            Err(failure) if matches!(failure, AuthorityStoreError::Missing) => false,
            Ok(1) => {
                let marker = self.record(retirement.clone(), 0, meter).await?;
                if marker.0.value.as_ref() != AUTHORITY_RETIRED {
                    return Err(AuthorityStoreError::CorruptStatic(
                        "filesystem retirement marker is invalid",
                    ));
                }
                true
            }
            Ok(_) => {
                return Err(AuthorityStoreError::CorruptStatic(
                    "filesystem retirement marker has an invalid tail",
                ));
            }
            Err(failure) => return Err(failure),
        };
        let retirement_bytes = retirement.as_str().len() as u64;
        drop(retirement);
        meter.release(retirement_bytes)?;
        if retired {
            return Err(AuthorityStoreError::Retired);
        }
        Ok(())
    }

    pub(super) async fn fork_gate(
        &self,
        authority: AuthorityId,
        meter: &mut AuthorityWork<'_>,
    ) -> Result<(), AuthorityStoreError> {
        let gate = meter.path(authority, "/publication-gate", None)?;
        let tail = self.tail(gate.clone(), meter).await?;
        if tail == 0 {
            return Err(AuthorityStoreError::Corrupt(
                "authority publication gate is empty".to_owned(),
            ));
        }
        let record = self.record(gate.clone(), tail - 1, meter).await?;
        decode_publication_gate(&record.0.value)?;
        drop(record);
        let bytes = gate.as_str().len() as u64;
        drop(gate);
        meter.release(bytes)
    }

    pub(super) async fn existing_fork_destination(
        &self,
        destination_authority: AuthorityId,
        source_generation: GenerationId,
        forked_at: u64,
        meter: &mut AuthorityWork<'_>,
    ) -> Result<Option<AuthoritySnapshot>, AuthorityStoreError> {
        let snapshot = match self.fork_snapshot(destination_authority, meter).await {
            Ok(snapshot) => snapshot,
            Err(failure) if authority_missing(&failure) => return Ok(None),
            Err(failure) => return Err(failure),
        };
        let locator = meter.path(
            destination_authority,
            "/generations/",
            Some(source_generation),
        )?;
        let locator_bytes = locator.as_str().len() as u64;
        let record = self.record(locator, 0, meter).await?;
        meter.release(locator_bytes)?;
        let located_tail = decode_lineage_tail(&record.0.value)?;
        drop(record);
        if located_tail == 0 {
            return Err(authority_corrupt(
                "destination generation locator selected an empty lineage",
                meter,
            ));
        }
        let lineage = meter.path(destination_authority, "/lineage", None)?;
        let lineage_bytes = lineage.as_str().len() as u64;
        let selected = self.record(lineage, located_tail - 1, meter).await?;
        meter.release(lineage_bytes)?;
        let generation = decode_lineage_generation(&selected.0.value)?;
        if located_tail == forked_at && generation == source_generation {
            Ok(Some(snapshot))
        } else {
            Err(AuthorityStoreError::RejectedStatic(
                "destination generation fork conflicts with existing state",
            ))
        }
    }

    /// Resolves the original source identity with bounded paths and exact reads.
    pub(super) async fn resolve_source_fork_point(
        &self,
        source_authority: AuthorityId,
        source_generation: GenerationId,
        meter: &mut AuthorityWork<'_>,
    ) -> Result<(acyclic_stream::StreamPath, u64), AuthorityStoreError> {
        let source_lineage = meter.path(source_authority, "/lineage", None)?;
        let locator = meter.path(source_authority, "/generations/", Some(source_generation))?;
        let locator_bytes = locator.as_str().len() as u64;
        let record = self.record(locator, 0, meter).await.map_err(|failure| {
            if authority_missing(&failure) && !meter.is_native() {
                AuthorityStoreError::RejectedStatic(
                    "source authority has no generation lineage locator",
                )
            } else {
                failure
            }
        })?;
        meter.release(locator_bytes)?;
        let forked_at = decode_lineage_tail(&record.0.value)?;
        drop(record);
        if forked_at == 0 {
            return Err(authority_corrupt(
                "generation locator selected an empty lineage",
                meter,
            ));
        }
        let selected = self
            .record(source_lineage.clone(), forked_at - 1, meter)
            .await?;
        let selected_generation = decode_lineage_generation(&selected.0.value)?;
        if selected_generation != source_generation {
            return Err(authority_corrupt(
                "generation locator and lineage disagree",
                meter,
            ));
        }
        Ok((source_lineage, forked_at))
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
