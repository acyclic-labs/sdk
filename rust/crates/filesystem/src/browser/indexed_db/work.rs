use super::*;

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "Finite integral Blob sizes below 2^64 are checked before conversion"
)]
pub(super) fn blob_length(blob: &Blob) -> Result<u64, AuthorityStoreError> {
    let length = blob.size();
    if !length.is_finite()
        || length < 0.0
        || length >= 18_446_744_073_709_551_616.0
        || length.fract() != 0.0
    {
        return Err(corrupt("invalid Blob length"));
    }
    Ok(length as u64)
}

pub(super) fn admit(
    context: &crate::AuthorityWork<'_>,
    delta: WorkCounters,
) -> Result<(), AuthorityStoreError> {
    context.check()?;
    context
        .work
        .admit(&delta, &context.budget)
        .map_err(Into::into)
}

pub(super) fn charge(
    context: &mut crate::AuthorityWork<'_>,
    delta: WorkCounters,
) -> Result<(), AuthorityStoreError> {
    context.check()?;
    context
        .work
        .charge(&delta, &context.budget)
        .map_err(Into::into)
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
        await_cancellable(request, self.cancellation).await
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
        await_cancellable(request, self.cancellation).await?;
        Ok(())
    }
    pub(super) async fn blob_bytes(
        &self,
        blob: &Blob,
        length: u64,
    ) -> Result<Vec<u8>, AuthorityStoreError> {
        if blob_length(blob)? != length {
            return Err(corrupt("invalid Blob length"));
        }
        let request = JsFuture::from(blob.array_buffer());
        let buffer = await_cancellable(
            async { request.await.map_err(|error| js_message(&error)) },
            self.cancellation,
        )
        .await?;
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
