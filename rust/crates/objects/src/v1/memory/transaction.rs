use super::*;

impl MemoryObjects {
    pub(super) fn mutate<R: Message + Default + 'static>(
        &self,
        digest: [u8; 32],
        identity: &Option<wire::MutationIdentity>,
        action: impl FnOnce(&mut State) -> Result<R, Error>,
    ) -> Result<R, Error> {
        let mut guard = self.lock_state()?;
        self.mutate_locked(&mut guard, digest, identity, action)
    }

    pub(super) fn mutate_locked<R: Message + Default + 'static>(
        &self,
        guard: &mut State,
        digest: [u8; 32],
        identity: &Option<wire::MutationIdentity>,
        action: impl FnOnce(&mut State) -> Result<R, Error>,
    ) -> Result<R, Error> {
        self.mutate_locked_engine(
            guard,
            digest,
            identity,
            |receipt| R::decode(receipt.response.as_slice()).map_err(|_| Unavailable.into()),
            action,
            |response, identity| {
                let encoded: OwnedBytes = response.encode_to_vec().into();
                // Promote ordinary byte controls before native aliasing.
                drop(encoded.clone());
                Ok((
                    identity.idempotency_key.clone().into(),
                    Receipt {
                        digest,
                        response: encoded,
                        put_info: (response as &dyn std::any::Any)
                            .downcast_ref::<wire::ObjectInfo>()
                            .cloned()
                            .map(Into::into),
                        #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
                        kind: persistence::response_kind::<R>()?,
                    },
                ))
            },
            |before, next| {
                #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
                if let Some(journal) = &self.journal {
                    if self.defer_local {
                        journal.validate_bodies(next)?;
                    } else {
                        journal
                            .commit(before, next, None)
                            .map_err(persistence::service_error)?;
                    }
                }
                #[cfg(not(all(feature = "local", not(target_arch = "wasm32"))))]
                let _ = (before, next);
                Ok(())
            },
        )
    }

    pub(super) fn mutate_locked_engine<R, E: From<Error>>(
        &self,
        guard: &mut State,
        digest: [u8; 32],
        identity: &Option<wire::MutationIdentity>,
        replay: impl FnOnce(&Receipt) -> Result<R, E>,
        action: impl FnOnce(&mut State) -> Result<R, E>,
        receipt: impl FnOnce(&R, &wire::MutationIdentity) -> Result<(SharedInfo<String>, Receipt), E>,
        commit: impl FnOnce(&State, &mut State) -> Result<(), E>,
    ) -> Result<R, E> {
        #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
        if let Some(journal) = &self.journal {
            journal.check()?;
        }
        if let Some(identity) = identity
            && let Some(receipt) = guard.receipts.get(identity.idempotency_key.as_str())
        {
            if receipt.digest != digest {
                return Err(Error::from(IdempotencyMismatch).into());
            }
            return replay(receipt);
        }
        let mut next = guard.clone();
        let response = action(&mut next)?;
        if let Some(identity) = identity {
            let (key, receipt) = receipt(&response, identity)?;
            next.receipts.insert(key, receipt);
        }
        let count = next.buckets.len()
            + next.uploads.len()
            + next.receipts.len()
            + next
                .buckets
                .values()
                .map(|bucket| bucket.objects.len())
                .sum::<usize>()
            + next
                .uploads
                .values()
                .map(|upload| upload.parts.len())
                .sum::<usize>();
        let bytes = stored_bytes(guard, &mut next).ok_or(Error::from(QuotaExceeded))?;
        if count > self.options.maximum_entries || bytes > self.options.maximum_bytes {
            return Err(Error::from(QuotaExceeded).into());
        }
        commit(guard, &mut next)?;
        *guard = next;
        Ok(response)
    }

    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    pub(super) fn put_durable_batch_locked(
        &self,
        state: &mut State,
        requests: Vec<(wire::PutObjectHeader, OwnedBytes)>,
    ) -> Vec<Result<wire::ObjectInfo, Error>> {
        let mut staged = self.clone();
        staged.defer_local = true;
        let mut input = requests.into_iter();
        let mut results = Vec::with_capacity(input.len());
        loop {
            let Some(count) = put_group_sizes(input.as_slice()).next() else {
                break;
            };
            let mut next = state.clone();
            let start = results.len();
            for (query, body) in input.by_ref().take(count) {
                results.push(staged.put_locked(&mut next, &query, body));
            }
            if next.sequence != state.sequence {
                match self
                    .journal
                    .as_ref()
                    .ok_or(Error::from(Unavailable))
                    .and_then(|journal| {
                        journal
                            .commit(state, &mut next, None)
                            .map_err(persistence::service_error)
                    }) {
                    Ok(()) => *state = next,
                    Err(error) => {
                        for result in &mut results[start..] {
                            if result.is_ok() {
                                *result = Err(error);
                            }
                        }
                    }
                }
            }
        }
        results
    }

    pub(super) fn put_locked(
        &self,
        state: &mut State,
        query: &wire::PutObjectHeader,
        body: OwnedBytes,
    ) -> Result<wire::ObjectInfo, Error> {
        let digest = request::put_digest(query, &body)?;
        let name = request::bucket(&query.bucket)?;
        self.mutate_locked(state, digest, &query.mutation, |state| {
            publish(
                state,
                name,
                &query.object_key,
                query.metadata.clone(),
                &query.preconditions,
                StoredBody::memory(body),
            )
        })
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl MemoryObjects {
    pub(super) fn put_locked_native(
        &self,
        state: &mut State,
        mut query: wire::PutObjectHeader,
        body: OwnedBytes,
        meter: &Arc<dyn NativeReadMeter>,
    ) -> Result<ObjectsPutReceipt, NativeObjectsError> {
        request::put(&query)?;
        request::upload_length(body.len() as u64)?;
        let digest = put_digest_measured(&mut query, &body, meter)?;
        let name = query
            .bucket
            .as_ref()
            .expect("validated put bucket")
            .name
            .as_str();
        self.mutate_locked_engine(
            state,
            digest,
            &query.mutation,
            |receipt| {
                let stored = receipt.put_info.as_ref().ok_or(Error::from(Unavailable))?;
                let mut allocation = ReadAllocation::empty(meter);
                let info = clone_native_info(stored, meter, &mut allocation)?;
                Ok((
                    ObjectsPutReceipt::new(info, Some(allocation)),
                    stored.clone(),
                ))
            },
            |state| {
                publish_engine(
                    state,
                    name,
                    &query.object_key,
                    &query.preconditions,
                    StoredBody::memory(body),
                    |state, size| {
                        let mut stored_allocation = ReadAllocation::empty(meter);
                        let metadata = query
                            .metadata
                            .as_ref()
                            .map(|metadata| {
                                clone_native_metadata(metadata, meter, &mut stored_allocation)
                            })
                            .transpose()?;
                        let info = wire::ObjectInfo {
                            etag: next_id_measured(state, meter, &mut stored_allocation)?,
                            size,
                            metadata,
                            last_modified: Some(timestamp()?),
                        };
                        let info = SharedInfo::new_measured(info, stored_allocation, meter)?;
                        let mut returned_allocation = ReadAllocation::empty(meter);
                        let returned = clone_native_info(&info, meter, &mut returned_allocation)?;
                        Ok((
                            info.clone(),
                            (
                                ObjectsPutReceipt::new(returned, Some(returned_allocation)),
                                info,
                            ),
                        ))
                    },
                    |key| SharedInfo::<String>::text(key, meter),
                )
            },
            |response, identity| {
                let encoded = encode_measured(response.0.info(), meter)?;
                Ok((
                    SharedInfo::<String>::text(&identity.idempotency_key, meter)?,
                    Receipt {
                        digest,
                        response: encoded,
                        put_info: Some(response.1.clone()),
                        #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
                        kind: persistence::response_kind::<wire::ObjectInfo>()?,
                    },
                ))
            },
            |before, next| {
                meter.check()?;
                #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
                if let Some(journal) = &self.journal {
                    if self.defer_local {
                        journal.validate_bodies(next)?;
                    } else {
                        journal.commit(before, next, Some(Arc::clone(meter)))?;
                    }
                }
                #[cfg(not(all(feature = "local", not(target_arch = "wasm32"))))]
                let _ = (before, next);
                if self.native_table_backend() {
                    meter.admit_write(0)?;
                }
                Ok(())
            },
        )
        .map(|(response, _)| response)
    }

    pub(super) fn native_table_backend(&self) -> bool {
        #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
        {
            self.journal.is_none()
        }
        #[cfg(not(all(feature = "local", not(target_arch = "wasm32"))))]
        {
            true
        }
    }
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub(super) fn put_group_sizes(
    requests: &[(wire::PutObjectHeader, OwnedBytes)],
) -> impl Iterator<Item = usize> + '_ {
    let mut requests = requests.iter().peekable();
    std::iter::from_fn(move || {
        requests.peek()?;
        let mut bytes = 0_usize;
        let mut count = 0_usize;
        while requests
            .next_if(|(query, body)| {
                let total = bytes.saturating_add(persistence::put_record_bytes(query, body));
                let fits = bytes == 0 || total <= persistence::RECORD_LIMIT;
                if fits {
                    bytes = total;
                }
                fits
            })
            .is_some()
        {
            count += 1;
        }
        Some(count)
    })
}
