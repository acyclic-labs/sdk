use super::*;

/// One independently configured volume owned by the embedded engine.
#[napi]
pub struct NativeVolume {
    pub(in crate::bindings) inner: LocalVolume,
    pub(in crate::bindings) cancellation: CancellationToken,
    pub(in crate::bindings) acquisition_work: acyclic_fs::WorkCounters,
}

#[napi]
impl NativeVolume {
    #[must_use]
    #[napi(getter)]
    pub fn id(&self) -> Buffer {
        Buffer::from(self.inner.id().into_bytes().to_vec())
    }

    #[napi(getter)]
    pub fn acquisition_work_json(&self) -> Result<String> {
        work_json(&self.acquisition_work)
    }

    #[napi]
    pub async fn diff_generations(
        &self,
        before: Buffer,
        after: Buffer,
        maximum_changes: NapiU32,
    ) -> Result<NativeGenerationDiff> {
        let maximum_changes = maximum_changes.into_inner();

        let before = acyclic_fs::GenerationId::new(Digest::from_bytes(fixed_32(
            &before,
            "before generation identity",
        )?));
        let after = acyclic_fs::GenerationId::new(Digest::from_bytes(fixed_32(
            &after,
            "after generation identity",
        )?));
        let receipt = self
            .inner
            .diff_generations(
                before,
                after,
                maximum_changes,
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
        encode_generation_diff(receipt.value, receipt.work)
    }

    #[napi]
    pub async fn checkout(&self, options: NativeCheckoutOptions) -> Result<NativeCheckout> {
        let consistency = ConsistencyMode::from_public_str(&options.consistency)
            .ok_or_else(|| Error::new(Status::InvalidArg, "invalid checkout consistency"))?;
        let access = AccessMode::from_public_str(&options.access)
            .ok_or_else(|| Error::new(Status::InvalidArg, "invalid checkout access"))?;
        let mutations = MutationMode::from_public_str(&options.mutation_mode)
            .ok_or_else(|| Error::new(Status::InvalidArg, "invalid checkout mutation mode"))?;
        let mode = CheckoutMode {
            access,
            consistency,
            mutations,
        };
        let receipt = self
            .inner
            .checkout(
                GenerationSelector::Head,
                mode,
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
        Ok(NativeCheckout {
            inner: Arc::new(SharedCheckout::new(receipt.value)),
            config: self.inner.config(),
            cancellation: CancellationToken::new(),
            acquisition_work: receipt.work,
        })
    }
}
