use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Fs<A, O> {


    /// Reads one exact immutable object for resumable manifest transfer.
    pub async fn export_object(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<crate::storage::ObjectRead> {
        let receipt = self
            .inner
            .objects
            .read(object_id, maximum_bytes, budget, cancellation)
            .await
            .map_err(|failure| OperationFailure::new(failure.error.into(), *failure.work))?;
        Ok(FsReceipt {
            value: receipt.value,
            work: receipt.work,
        })
    }


    /// Idempotently imports one immutable object under its canonical identity.
    pub async fn import_object(
        &self,
        object_id: ObjectId,
        bytes: impl Into<acyclic_stream::OwnedBytes>,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<()> {
        let receipt = self
            .inner
            .objects
            .put(object_id, bytes.into(), budget, cancellation)
            .await
            .map_err(|failure| OperationFailure::new(failure.error.into(), *failure.work))?;
        Ok(FsReceipt {
            value: (),
            work: receipt.work,
        })
    }


    /// Reads one ordered resumable page from a canonical generation manifest.
    pub async fn export_generation_batch(
        &self,
        manifest: &GenerationExportManifest,
        cursor: TransferCursor,
        maximum_objects: u32,
        maximum_object_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<GenerationTransferBatch> {
        let span = crate::obs::span!(
            INFO,
            "acyclic.fs.export_generation_batch",
            outcome = crate::obs::Empty,
            error.kind = crate::obs::Empty,
            work.items = crate::obs::Empty,
            work.bytes = crate::obs::Empty,
            work.durability = crate::obs::Empty,
        );
        let result = crate::obs::in_span(
            &span,
            export_generation_batch_async(
                &self.inner.objects,
                manifest,
                cursor,
                maximum_objects,
                maximum_object_bytes,
                budget,
                cancellation,
            ),
        )
        .await
        .map_err(|failure| OperationFailure::new(map_transfer_error(failure.error), *failure.work));
        result.observe_on(&span, "export_generation_batch")
    }


    /// Idempotently imports one manifest-aligned page of immutable bodies.
    pub async fn import_generation_batch(
        &self,
        manifest: &GenerationExportManifest,
        cursor: TransferCursor,
        bodies: &[acyclic_stream::OwnedBytes],
        maximum_objects: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<TransferCursor> {
        let span = crate::obs::span!(
            INFO,
            "acyclic.fs.import_generation_batch",
            outcome = crate::obs::Empty,
            error.kind = crate::obs::Empty,
            work.items = crate::obs::Empty,
            work.bytes = crate::obs::Empty,
            work.durability = crate::obs::Empty,
        );
        let result = crate::obs::in_span(
            &span,
            import_generation_batch_async(
                &self.inner.objects,
                manifest,
                cursor,
                bodies,
                maximum_objects,
                budget,
                cancellation,
            ),
        )
        .await
        .map_err(|failure| OperationFailure::new(map_transfer_error(failure.error), *failure.work));
        result.observe_on(&span, "import_generation_batch")
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Checkout<A, O> {


    /// Builds a deterministic complete manifest for the current immutable or
    pub async fn export_manifest(
        &self,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<GenerationExportManifest> {
        let checkpoint = self.checkpoint_root(budget, cancellation).await?;
        let generation_root = checkpoint.value;
        let mut work = checkpoint.work;
        let manifest = build_generation_export_manifest_async(
            &self.volume.fs.inner.objects,
            self.volume.id,
            self.volume.config,
            generation_root,
            closure_limits(self.volume.config),
            remaining(work, budget)?,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(work, map_transfer_error))?;
        work = add(work, manifest.work)?;
        Ok(FsReceipt {
            value: manifest.value,
            work,
        })
    }
}

