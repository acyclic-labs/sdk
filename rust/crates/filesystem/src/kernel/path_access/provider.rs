use super::*;
use crate::async_storage::object_error;

impl<S: AsyncObjectStore> AsyncObjectStore for OperationReadCache<'_, S> {
    crate::async_storage::forward_object_store!(self => self.store;
        requires_child_first_publication,
        maximum_reference_bytes,
        prepare_hashed,
        supports_decoded_cache,
        put_stored,
        read_many_stored,
        read_stored,
        decoded_cache_lookup,
        put,
        contains,
        collection,
        collection_sweeps,
    );

    async fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<crate::ObjectRead, crate::ObjectStoreError> {
        if context.read_observation.is_some_and(|observation| {
            self.data_custody
                .is_some_and(|owner| std::ptr::eq(observation, owner))
        }) {
            context.read_observation = None;
        }
        if let Some(value) = self
            .probe(object_id, &mut context.accounting())
            .map_err(object_error)?
        {
            if usize_to_u64(value.bytes.len()) > maximum_bytes {
                return Err(ObjectStoreError::TooLarge {
                    observed: usize_to_u64(value.bytes.len()),
                    maximum: maximum_bytes,
                });
            }
            return Ok(value);
        }
        let observation = self
            .data_custody
            .filter(|_| !context.is_native())
            .map(|owner| (owner, context.spent()));
        let read = self.store.read(object_id, maximum_bytes, context).await;
        if let Some((owner, before)) = observation {
            owner.record_original_work(context.spent().checked_sub_cumulative(&before)?)?;
        }
        let mut value = read?;
        let owned = if context.is_native() {
            0
        } else {
            value.retention.owned_bytes()
        };
        if self
            .admit(object_id, &value.bytes, owned, &mut context.accounting())
            .map_err(object_error)?
        {
            value.retention = ObjectReadRetention::Shared;
        }
        Ok(value)
    }

    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        context: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<crate::ObjectRead>, crate::AuthorityAllocation), crate::ObjectStoreError> {
        crate::async_storage::read_many_sequential(self, requests, context).await
    }
}
