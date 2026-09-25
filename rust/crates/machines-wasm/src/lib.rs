//! Browser binding for the canonical Rust Machines simulator.

#[cfg(any(target_arch = "wasm32", test))]
mod request;
#[cfg(target_arch = "wasm32")]
mod wire;

#[cfg(target_arch = "wasm32")]
mod browser {
    use acyclic_machines::{
        CheckpointId, IdempotencyKey, MachineId, MachinesProvider, OperationId, ProviderError,
        SimulatedMachines,
    };
    use futures::StreamExt as _;
    use sha2::{Digest as _, Sha256};
    use std::{collections::BTreeSet, num::NonZeroU32};
    use uuid::Uuid;
    use wasm_bindgen::prelude::*;

    use crate::request as admission;
    use crate::wire as projection;

    fn error(value: impl std::fmt::Display) -> JsValue {
        js_sys::Error::new(&value.to_string()).into()
    }

    // The TypeScript API has always accepted caller-chosen string keys. Domain identities
    // remain UUIDs: retain valid UUIDs and deterministically name all other caller strings.
    fn identity(value: &str) -> Result<String, JsValue> {
        if value.trim().is_empty() {
            return Err(error("identity is empty"));
        }
        if let Ok(uuid) = Uuid::parse_str(value) {
            if !uuid.is_nil() {
                return Ok(uuid.to_string());
            }
        }
        let digest = Sha256::digest(
            [
                b"acyclic-machines-ts-identity-v1\0".as_slice(),
                value.as_bytes(),
            ]
            .concat(),
        );
        let mut bytes = [0_u8; 16];
        bytes.copy_from_slice(&digest[..16]);
        bytes[6] = (bytes[6] & 0x0f) | 0x50;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        Ok(Uuid::from_bytes(bytes).to_string())
    }

    #[wasm_bindgen(js_name = normalizeIdentityBytes)]
    pub fn normalize_identity_bytes(value: &str) -> Result<Vec<u8>, JsValue> {
        let normalized = identity(value)?;
        Ok(Uuid::parse_str(&normalized)
            .map_err(error)?
            .as_bytes()
            .to_vec())
    }

    fn key(value: &str) -> Result<IdempotencyKey, JsValue> {
        IdempotencyKey::parse(&identity(value)?).map_err(error)
    }
    fn machine(value: &str) -> Result<MachineId, JsValue> {
        MachineId::parse(&identity(value)?).map_err(error)
    }
    fn checkpoint(value: &str) -> Result<CheckpointId, JsValue> {
        CheckpointId::parse(&identity(value)?).map_err(error)
    }
    fn operation(value: &str) -> Result<OperationId, JsValue> {
        OperationId::parse(&identity(value)?).map_err(error)
    }
    fn provider(value: ProviderError) -> JsValue {
        error(value)
    }

    #[wasm_bindgen]
    pub struct SimulatedMachinesBinding {
        inner: SimulatedMachines,
    }

    #[wasm_bindgen]
    impl SimulatedMachinesBinding {
        #[wasm_bindgen(constructor)]
        pub fn new() -> Self {
            Self {
                inner: SimulatedMachines::default(),
            }
        }

        pub fn with_capabilities(values: Vec<i32>) -> Result<Self, JsValue> {
            let capabilities = values
                .into_iter()
                .map(admission::capability)
                .collect::<Result<BTreeSet<_>, _>>()
                .map_err(provider)?;
            Ok(Self {
                inner: SimulatedMachines::with_capabilities(capabilities),
            })
        }

        pub async fn qualify_image(&self, image: Vec<u8>) -> Result<Vec<u8>, JsValue> {
            let image = admission::image_bytes(&image).map_err(provider)?;
            Ok(projection::qualification(
                self.inner.qualify_image(image).await.map_err(provider)?,
            ))
        }
        pub async fn create(&self, request: Vec<u8>) -> Result<Vec<u8>, JsValue> {
            let request = admission::create(&request).map_err(provider)?;
            projection::mutation(self.inner.create(request).await.map_err(provider)?)
                .map_err(provider)
        }
        pub async fn inspect_machine(&self, id: String) -> Result<Vec<u8>, JsValue> {
            projection::machine(
                self.inner
                    .inspect_machine(machine(&id)?)
                    .await
                    .map_err(provider)?,
            )
            .map_err(provider)
        }
        pub async fn list_machines(
            &self,
            after: Option<String>,
            limit: u32,
        ) -> Result<Vec<u8>, JsValue> {
            let after = after.as_deref().map(machine).transpose()?;
            projection::machines(
                self.inner
                    .list_machines(after, limit)
                    .await
                    .map_err(provider)?,
            )
            .map_err(provider)
        }
        pub async fn checkpoint(
            &self,
            id: String,
            idempotency_key: String,
        ) -> Result<Vec<u8>, JsValue> {
            projection::mutation(
                self.inner
                    .checkpoint(machine(&id)?, key(&idempotency_key)?)
                    .await
                    .map_err(provider)?,
            )
            .map_err(provider)
        }
        pub async fn inspect_checkpoint(&self, id: String) -> Result<Vec<u8>, JsValue> {
            projection::checkpoint(
                self.inner
                    .inspect_checkpoint(checkpoint(&id)?)
                    .await
                    .map_err(provider)?,
            )
            .map_err(provider)
        }
        pub async fn fork(
            &self,
            id: String,
            count: u32,
            performance: u32,
            idempotency_key: String,
        ) -> Result<Vec<u8>, JsValue> {
            let count =
                NonZeroU32::new(count).ok_or_else(|| error("fork count must be positive"))?;
            projection::mutation(
                self.inner
                    .fork(
                        checkpoint(&id)?,
                        count,
                        admission::performance(performance).map_err(provider)?,
                        key(&idempotency_key)?,
                    )
                    .await
                    .map_err(provider)?,
            )
            .map_err(provider)
        }
        pub async fn fork_machine(
            &self,
            id: String,
            count: u32,
            idempotency_key: String,
        ) -> Result<Vec<u8>, JsValue> {
            let count =
                NonZeroU32::new(count).ok_or_else(|| error("fork count must be positive"))?;
            projection::mutation(
                self.inner
                    .fork_machine(machine(&id)?, count, key(&idempotency_key)?)
                    .await
                    .map_err(provider)?,
            )
            .map_err(provider)
        }
        pub async fn suspend(
            &self,
            id: String,
            idempotency_key: String,
        ) -> Result<Vec<u8>, JsValue> {
            projection::mutation(
                self.inner
                    .suspend(machine(&id)?, key(&idempotency_key)?)
                    .await
                    .map_err(provider)?,
            )
            .map_err(provider)
        }
        pub async fn wake(&self, id: String, idempotency_key: String) -> Result<Vec<u8>, JsValue> {
            projection::mutation(
                self.inner
                    .wake(machine(&id)?, key(&idempotency_key)?)
                    .await
                    .map_err(provider)?,
            )
            .map_err(provider)
        }
        pub async fn set_suspension_policy(
            &self,
            id: String,
            policy: Vec<u8>,
            idempotency_key: String,
        ) -> Result<Vec<u8>, JsValue> {
            projection::mutation(
                self.inner
                    .set_suspension_policy(
                        machine(&id)?,
                        admission::suspension_bytes(&policy).map_err(provider)?,
                        key(&idempotency_key)?,
                    )
                    .await
                    .map_err(provider)?,
            )
            .map_err(provider)
        }
        pub async fn destroy_machine(
            &self,
            id: String,
            idempotency_key: String,
        ) -> Result<Vec<u8>, JsValue> {
            projection::mutation(
                self.inner
                    .destroy_machine(machine(&id)?, key(&idempotency_key)?)
                    .await
                    .map_err(provider)?,
            )
            .map_err(provider)
        }
        pub async fn destroy_checkpoint(
            &self,
            id: String,
            idempotency_key: String,
        ) -> Result<Vec<u8>, JsValue> {
            projection::mutation(
                self.inner
                    .destroy_checkpoint(checkpoint(&id)?, key(&idempotency_key)?)
                    .await
                    .map_err(provider)?,
            )
            .map_err(provider)
        }
        pub async fn events(
            &self,
            id: String,
            after_sequence: Option<u64>,
            limit: u32,
        ) -> Result<Vec<u8>, JsValue> {
            Ok(projection::events(
                self.inner
                    .events(machine(&id)?, after_sequence, limit)
                    .await
                    .map_err(provider)?,
            ))
        }
        pub async fn usage(
            &self,
            id: String,
            start_unix_ms: u64,
            end_unix_ms: u64,
        ) -> Result<Vec<u8>, JsValue> {
            Ok(projection::usage(
                self.inner
                    .usage(machine(&id)?, start_unix_ms, end_unix_ms)
                    .await
                    .map_err(provider)?,
            ))
        }
        pub async fn recover(&self, idempotency_key: String) -> Result<Vec<u8>, JsValue> {
            projection::mutation(
                self.inner
                    .recover(key(&idempotency_key)?)
                    .await
                    .map_err(provider)?,
            )
            .map_err(provider)
        }
        pub async fn recover_operation(&self, idempotency_key: String) -> Result<Vec<u8>, JsValue> {
            Ok(projection::recovered_operation(
                self.inner
                    .recover_operation(key(&idempotency_key)?)
                    .await
                    .map_err(provider)?,
            ))
        }
        pub async fn inspect_operation(&self, id: String) -> Result<Vec<u8>, JsValue> {
            Ok(projection::operation(
                self.inner
                    .inspect_operation(operation(&id)?)
                    .await
                    .map_err(provider)?,
            ))
        }
        pub async fn cancel(&self, id: String) -> Result<Vec<u8>, JsValue> {
            Ok(projection::operation(
                self.inner.cancel(operation(&id)?).await.map_err(provider)?,
            ))
        }
        pub async fn watch_operation(&self, id: String) -> Result<Vec<u8>, JsValue> {
            let mut stream = self
                .inner
                .watch_operation(operation(&id)?)
                .await
                .map_err(provider)?;
            let mut observations = Vec::new();
            while let Some(value) = stream.next().await {
                observations.push(value.map_err(provider)?);
            }
            Ok(projection::operations(observations))
        }
    }
}
