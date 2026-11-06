use super::*;

/// A single fragment stays an alias. On the second, admit the final output
/// once and release each copied fragment before reading another one.
struct Selection {
    length: usize,
    body: Selected,
}

enum Selected {
    Empty,
    One(OwnedBytes),
    Joined(Vec<u8>),
}

impl Selection {
    fn new(length: u64) -> Result<Self, Error> {
        Ok(Self {
            length: usize::try_from(length).map_err(|_| Error::from(ErrorCode::QuotaExceeded))?,
            body: Selected::Empty,
        })
    }

    fn push(&mut self, bytes: OwnedBytes, physical: &impl Medium) -> Result<(), Error> {
        let current = match &self.body {
            Selected::Empty => 0,
            Selected::One(first) => first.len(),
            Selected::Joined(joined) => joined.len(),
        };
        if current
            .checked_add(bytes.len())
            .is_none_or(|length| length > self.length)
        {
            return Err(corrupt());
        }
        match &mut self.body {
            Selected::Empty => self.body = Selected::One(bytes),
            Selected::One(first) => {
                physical.allocate(self.length as u64).map_err(medium)?;
                let mut joined = Vec::new();
                joined
                    .try_reserve_exact(self.length)
                    .map_err(|_| Error::from(ErrorCode::QuotaExceeded))?;
                joined.extend_from_slice(first);
                joined.extend_from_slice(&bytes);
                self.body = Selected::Joined(joined);
            }
            Selected::Joined(joined) => joined.extend_from_slice(&bytes),
        }
        Ok(())
    }

    fn finish(self, physical: &impl Medium) -> Result<OwnedBytes, Error> {
        match self.body {
            Selected::Empty => Ok(OwnedBytes::new()),
            Selected::One(bytes) => Ok(bytes),
            Selected::Joined(bytes) => physical.finish_allocation(bytes).map_err(medium),
        }
    }
}

impl<M: Medium, P: Deref<Target = M> + ProviderPlatform> Account<M, P> {
    /// Appends the selected logical bytes of `digest` to `out`.
    async fn select(
        &self,
        digest: Digest,
        envelope: &Envelope,
        range: std::ops::Range<u64>,
        out: &mut Selection,
    ) -> Result<(), Error> {
        if range.is_empty() {
            return Ok(());
        }
        match envelope.kind {
            Kind::Blob => {
                let whole = range.start == 0 && range.end == envelope.length;
                // A whole read checks its bytes; a partial one first
                // proves a payload a crash could have torn complete.
                if !whole && self.medium.volatile() && !self.complete(&digest, envelope).await? {
                    return Err(ErrorCode::NotFound.into());
                }
                out.push(
                    self.read_payload(&digest, envelope, (!whole).then_some(range))
                        .await?,
                    &*self.medium,
                )?;
            }
            Kind::Manifest => {
                let manifest = self.manifest(&digest, envelope).await?;
                let mut offset = 0u64;
                for part in manifest.parts {
                    let end = offset.checked_add(part.length).ok_or_else(corrupt)?;
                    let start = range.start.max(offset);
                    let stop = range.end.min(end);
                    if start < stop {
                        let (_, child) = self
                            .stored_envelope(&part.digest)
                            .await?
                            .ok_or_else(corrupt)?;
                        if child.length != part.length {
                            return Err(corrupt());
                        }
                        Box::pin(self.select(
                            part.digest,
                            &child,
                            start - offset..stop - offset,
                            out,
                        ))
                        .await?;
                    }
                    if end >= range.end {
                        break;
                    }
                    offset = end;
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn get_inner(
        &self,
        digest: Digest,
        range: ByteRange,
        maximum_bytes: u64,
    ) -> Result<Object, Error> {
        acyclic_observability::async_operation!(
            "acyclic.objects.get",
            |error: &Error| error.code.as_str_name(),
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
                // Everything committed by `served` is visible to the read after it.
                let served = self.read_revision().await?;
                // Reading the payload proves the blob complete; an empty selection
                // reads none, so it checks the payload instead.
                let (_, envelope) = self
                    .stored_envelope(&digest)
                    .await?
                    .ok_or(Error::from(ErrorCode::NotFound))?;
                let selected = range.resolve(envelope.length)?;
                if selected.is_empty() && !self.complete(&digest, &envelope).await? {
                    return Err(ErrorCode::NotFound.into());
                }
                if selected.end - selected.start > maximum_bytes {
                    return Err(ErrorCode::QuotaExceeded.into());
                }
                let mut parts = Selection::new(selected.end - selected.start)?;
                self.select(digest, &envelope, selected.clone(), &mut parts)
                    .await?;
                let body = parts.finish(&*self.medium)?;
                if body.len() as u64 != selected.end - selected.start {
                    return Err(corrupt());
                }
                self.medium
                    .finish_object(
                        Object::new(Self::head_of(digest, envelope), selected.start, body)
                            .served_at(served),
                    )
                    .map_err(medium)
            }
        )
    }
}

#[cfg(all(test, feature = "local", not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    #[tokio::test]
    async fn real_local_selection_starts_without_a_heap_frame()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let store =
            Store::new(crate::LocalMedium::open(directory.path(), Default::default(), None).await?);
        let account = store.account(b"inline-selection")?;
        let bytes = OwnedBytes::from_static(b"original physical selection");
        let leaf = account.put(bytes.clone(), Vec::new()).await?;
        let leaf = leaf.verified().ok_or("leaf publication is known")?;
        let manifest = account
            .put_manifest(Manifest {
                parts: vec![crate::Part {
                    digest: leaf.digest,
                    length: leaf.length,
                }],
                edges: Vec::new(),
            })
            .await?;
        let manifest = manifest.verified().ok_or("manifest publication is known")?;
        for digest in [leaf.digest, manifest.digest] {
            let (_, envelope) = account.stored_envelope(&digest).await?.ok_or("envelope")?;
            let mut selected = Selection::new(envelope.length)?;
            // Isolate the real engine's selection-frame construction from
            // allocations required by the subsequent physical I/O.
            let allocation = acyclic_test_support::measure(|| {
                drop(std::hint::black_box(account.select(
                    digest,
                    &envelope,
                    0..envelope.length,
                    &mut selected,
                )));
            });
            assert_eq!(
                allocation.count_total, 0,
                "root selection allocated an unnecessary heap frame"
            );
            assert_eq!(
                account.get(digest, ByteRange::FULL, u64::MAX).await?.body(),
                &bytes
            );
        }
        Ok(())
    }
}
