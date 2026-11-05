use super::*;

pub(super) fn blob_length(blob: &Blob) -> Result<u64, AuthorityStoreError> {
    crate::browser::blob_length(blob).map_err(|_| corrupt("invalid Blob length"))
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
        &mut self,
        blob: &Blob,
        length: u64,
    ) -> Result<OwnedBytes, ObjectStoreError> {
        let cancellation = self.cancellation;
        crate::browser::blob_bytes(
            blob,
            length,
            &std::cell::RefCell::new(self.accounting()),
            cancellation,
        )
        .await
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
