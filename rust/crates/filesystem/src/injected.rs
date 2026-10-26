//! Host-injected immutable object I/O for any JavaScript storage provider.
use crate::{
    AsyncObjectStore, AuthorityWork, ObjectId, ObjectRead, ObjectReadRequest, ObjectStoreError,
    PublicationHold, PublicationScope, WorkCounters,
    storage::{OBJECT_DIGEST_ENVELOPE_BYTES, ObjectReadRetention, ObjectWrite, object_digest},
};
use acyclic_stream::OwnedBytes;
use js_sys::{Array, Uint8Array};
use wasm_bindgen::{JsCast, prelude::*};
use wasm_bindgen_futures::JsFuture;

#[wasm_bindgen]
extern "C" {
    #[derive(Clone)]
    type ImmutableObjects;
    #[wasm_bindgen(method, catch)]
    fn length(this: &ImmutableObjects, key: &[u8]) -> Result<js_sys::Promise, JsValue>;
    #[wasm_bindgen(method, catch)]
    fn read(this: &ImmutableObjects, key: &[u8], maximum: u64) -> Result<js_sys::Promise, JsValue>;
    #[wasm_bindgen(method, catch)]
    fn put(
        this: &ImmutableObjects,
        keys: &Array,
        values: &Array,
    ) -> Result<js_sys::Promise, JsValue>;
}

/// Host custody of immutable objects; authority remains in Streams.
/// The injected put promise must resolve only after all bytes are durable.
#[derive(Clone)]
pub struct InjectedObjectStore {
    storage: ImmutableObjects,
    maximum: u64,
}

fn backend(error: &JsValue) -> ObjectStoreError {
    ObjectStoreError::Rejected(format!("injected immutable-object I/O failed: {error:?}"))
}

impl InjectedObjectStore {
    /// Injects length/read/put promise methods, with a positive object bound.
    pub fn new(storage: JsValue, maximum: u64) -> Result<Self, ObjectStoreError> {
        if !storage.is_object() || maximum == 0 || usize::try_from(maximum).is_err() {
            return Err(ObjectStoreError::Rejected(
                "invalid injected object configuration".into(),
            ));
        }
        Ok(Self {
            storage: storage.unchecked_into(),
            maximum,
        })
    }

    async fn size(&self, id: ObjectId) -> Result<Option<u64>, ObjectStoreError> {
        let value = JsFuture::from(
            self.storage
                .length(&id.to_bytes())
                .map_err(|error| backend(&error))?,
        )
        .await
        .map_err(|error| backend(&error))?;
        if value.is_null() {
            return Ok(None);
        }
        value
            .as_f64()
            .filter(|n| n.is_finite() && *n >= 0.0 && n.fract() == 0.0 && *n <= f64::from(u32::MAX))
            .ok_or(ObjectStoreError::Corrupt)?;
        let integer = js_sys::BigInt::new(&value).map_err(|_| ObjectStoreError::Corrupt)?;
        Ok(Some(
            u64::try_from(integer).map_err(|_| ObjectStoreError::Corrupt)?,
        ))
    }

    fn bound(&self, size: u64, requested: u64) -> Result<(), ObjectStoreError> {
        let maximum = self.maximum.min(requested);
        if size > maximum {
            return Err(ObjectStoreError::TooLarge {
                observed: size,
                maximum,
            });
        }
        Ok(())
    }
}

impl AsyncObjectStore for InjectedObjectStore {
    fn supports_decoded_cache(&self) -> bool {
        false
    }

    async fn put(
        &self,
        id: ObjectId,
        bytes: OwnedBytes,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        self.put_many(
            &[ObjectWrite {
                object_id: id,
                bytes,
            }],
            work,
        )
        .await
    }

    async fn put_many(
        &self,
        writes: &[ObjectWrite],
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        work.check()?;
        if writes.is_empty() {
            return Err(ObjectStoreError::EmptyWriteBatch);
        }
        for write in writes {
            let length = write.bytes.len() as u64;
            self.bound(length, self.maximum)?;
            work.charge(WorkCounters {
                bytes_hashed: length + OBJECT_DIGEST_ENVELOPE_BYTES,
                ..WorkCounters::UNCHARGED
            })?;
            if object_digest(write.object_id.kind, &write.bytes) != write.object_id.digest {
                return Err(ObjectStoreError::DigestMismatch);
            }
        }
        let length = writes
            .iter()
            .try_fold(0_u64, |n, w| n.checked_add(w.bytes.len() as u64))
            .ok_or(crate::WorkError::Overflow)?;
        work.charge(WorkCounters {
            backend_write_operations: 1,
            object_bytes_written: length,
            bytes_copied: length,
            allocation_operations: writes.len() as u64 * 2,
            peak_allocation_bytes: length + writes.len() as u64 * ObjectId::ENCODED_LEN as u64,
            ..WorkCounters::UNCHARGED
        })?;
        let keys = Array::new();
        let values = Array::new();
        for write in writes {
            keys.push(&Uint8Array::from(write.object_id.to_bytes().as_slice()));
            values.push(&Uint8Array::from(write.bytes.as_ref()));
        }
        JsFuture::from(
            self.storage
                .put(&keys, &values)
                .map_err(|error| backend(&error))?,
        )
        .await
        .map_err(|error| backend(&error))?;
        work.check()?;
        Ok(())
    }

    async fn flush_before_publish(
        &self,
        _: PublicationScope<'_>,
        work: &mut AuthorityWork<'_>,
    ) -> Result<PublicationHold, ObjectStoreError> {
        work.check()?;
        // The injected put contract requires durability before resolving.
        Ok(PublicationHold::none())
    }

    async fn read(
        &self,
        id: ObjectId,
        maximum: u64,
        work: &mut AuthorityWork<'_>,
    ) -> Result<ObjectRead, ObjectStoreError> {
        work.check()?;
        work.charge(WorkCounters {
            object_probes: 1,
            backend_read_operations: 1,
            ..WorkCounters::UNCHARGED
        })?;
        let length = self.size(id).await?.ok_or(ObjectStoreError::Missing)?;
        self.bound(length, maximum)?;
        work.charge(WorkCounters {
            backend_read_operations: 1,
            object_bytes_read: length,
            bytes_hashed: length + OBJECT_DIGEST_ENVELOPE_BYTES,
            bytes_copied: length * 2,
            allocation_operations: 2,
            peak_allocation_bytes: length * 2,
            ..WorkCounters::UNCHARGED
        })?;
        let value = JsFuture::from(
            self.storage
                .read(&id.to_bytes(), length)
                .map_err(|error| backend(&error))?,
        )
        .await
        .map_err(|error| backend(&error))?;
        let value: Uint8Array = value.dyn_into().map_err(|error| backend(&error))?;
        if u64::from(value.length()) != length {
            return Err(ObjectStoreError::Corrupt);
        }
        let bytes = value.to_vec();
        if object_digest(id.kind, &bytes) != id.digest {
            return Err(ObjectStoreError::Corrupt);
        }
        work.check()?;
        Ok(ObjectRead {
            bytes: bytes.into(),
            retention: ObjectReadRetention::Owned {
                logical_bytes: length,
            },
        })
    }

    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        work: &mut AuthorityWork<'_>,
    ) -> Result<(Vec<ObjectRead>, crate::AuthorityAllocation), ObjectStoreError> {
        crate::async_storage::read_many_sequential(self, requests, work).await
    }

    async fn contains(
        &self,
        id: ObjectId,
        work: &mut AuthorityWork<'_>,
    ) -> Result<bool, ObjectStoreError> {
        work.check()?;
        work.charge(WorkCounters {
            object_probes: 1,
            backend_read_operations: 1,
            ..WorkCounters::UNCHARGED
        })?;
        let exists = self.size(id).await?.is_some();
        work.check()?;
        Ok(exists)
    }
}
