use super::*;

pub(super) fn admit(
    context: &crate::AuthorityWork<'_>,
    delta: WorkCounters,
) -> Result<WorkCounters, AuthorityStoreError> {
    context.check()?;
    let work = context.work.checked_add(delta)?;
    work.verify(context.budget)?;
    Ok(work)
}

pub(super) fn charge(
    context: &mut crate::AuthorityWork<'_>,
    delta: WorkCounters,
) -> Result<(), AuthorityStoreError> {
    context.work = admit(context, delta)?;
    Ok(())
}

impl crate::AuthorityWork<'_> {
    pub(super) async fn get(
        &self,
        tx: &Transaction<'_>,
        store: &str,
        key: &str,
    ) -> Result<Option<JsValue>, AuthorityStoreError> {
        let store = tx.object_store(store).map_err(backend)?;
        let request = store
            .get::<JsValue, _, _>(key)
            .primitive()
            .map_err(backend)?;
        await_cancellable(request, self.cancellation)
            .await
            .map_err(|e| match e {
                None => AuthorityStoreError::Cancelled,
                Some(e) => backend(e),
            })
    }
    pub(super) async fn fixed(
        &mut self,
        tx: &Transaction<'_>,
        store: &str,
        key: &str,
        length: usize,
    ) -> Result<Option<Vec<u8>>, AuthorityStoreError> {
        admit(self, authority_fixed_read_work(length))?;
        let value = self.get(tx, store, key).await?;
        let Some(value) = value else {
            charge(self, authority_fixed_read_work(0))?;
            return Ok(None);
        };
        let value = value
            .dyn_into::<Uint8Array>()
            .map_err(|_| corrupt("invalid fixed record"))?;
        if value.length() as usize != length {
            return Err(corrupt("invalid fixed record length"));
        }
        charge(self, authority_fixed_read_work(length))?;
        Ok(Some(value.to_vec()))
    }
    pub(super) async fn head(
        &mut self,
        tx: &Transaction<'_>,
        authority: AuthorityId,
    ) -> Result<Head, AuthorityStoreError> {
        let bytes = self
            .fixed(tx, AUTHORITY_HEADS, &authority_key(authority), HEAD_BYTES)
            .await?
            .ok_or_else(|| AuthorityStoreError::Missing)?;
        decode_head(&bytes).map_err(corrupt)
    }
    pub(super) async fn gate(
        &mut self,
        tx: &Transaction<'_>,
        authority: AuthorityId,
    ) -> Result<PublicationGateRecord, AuthorityStoreError> {
        let bytes = self
            .fixed(tx, AUTHORITY_GATES, &authority_key(authority), GATE_BYTES)
            .await?
            .ok_or_else(|| corrupt("missing publication gate"))?;
        decode_publication_gate(&bytes).map_err(corrupt)
    }
    pub(super) async fn set(
        &self,
        tx: &Transaction<'_>,
        store: &str,
        key: &str,
        bytes: &[u8],
    ) -> Result<(), AuthorityStoreError> {
        let store = tx.object_store(store).map_err(backend)?;
        let request = store
            .put(JsValue::from(Uint8Array::from(bytes)))
            .with_key(key)
            .primitive()
            .map_err(backend)?;
        await_cancellable(request, self.cancellation)
            .await
            .map_err(|e| match e {
                None => AuthorityStoreError::Cancelled,
                Some(e) => backend(e),
            })?;
        Ok(())
    }
    pub(super) async fn insert(
        &self,
        tx: &Transaction<'_>,
        store: &str,
        key: &str,
        value: JsValue,
    ) -> Result<(), AuthorityStoreError> {
        let store = tx.object_store(store).map_err(backend)?;
        let request = store
            .add(value)
            .with_key(key)
            .primitive()
            .map_err(backend)?;
        await_cancellable(request, self.cancellation)
            .await
            .map_err(|e| match e {
                None => AuthorityStoreError::Cancelled,
                Some(e) => backend(e),
            })?;
        Ok(())
    }
    pub(super) async fn blob(
        &self,
        tx: &Transaction<'_>,
        store: &str,
        key: &str,
    ) -> Result<Blob, AuthorityStoreError> {
        self.get(tx, store, key)
            .await?
            .ok_or_else(|| corrupt("missing Blob"))?
            .dyn_into::<Blob>()
            .map_err(|_| corrupt("invalid Blob"))
    }
    pub(super) async fn blob_bytes(
        &self,
        blob: &Blob,
        length: u64,
    ) -> Result<Vec<u8>, AuthorityStoreError> {
        if blob.size() != length as f64 {
            return Err(corrupt("invalid Blob length"));
        }
        let buffer = await_cancellable(JsFuture::from(blob.array_buffer()), self.cancellation)
            .await
            .map_err(|e| match e {
                None => AuthorityStoreError::Cancelled,
                Some(e) => backend(js_message(&e)),
            })?;
        let array = Uint8Array::new(&buffer);
        if u64::from(array.length()) != length {
            return Err(corrupt("invalid materialized length"));
        }
        Ok(array.to_vec())
    }
    pub(super) async fn commit(&mut self, tx: Transaction<'_>) -> Result<(), AuthorityStoreError> {
        charge(
            self,
            WorkCounters {
                durability_operations: 1,
                ..Default::default()
            },
        )?;
        tx.commit().await.map_err(backend)
    }
}
