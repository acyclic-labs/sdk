use super::*;

/// Cheap immutable reader over one authenticated pinned checkout candidate.
pub struct PinnedReader<A, O> {
    pub(super) volume: Volume<A, O>,
    pub(super) generation_root: ObjectId,
    pub(super) root: PinnedReaderRoot,
}

#[derive(Clone)]
pub(super) enum PinnedReaderRoot {
    Owned(GenerationRoot),
    #[cfg(not(target_arch = "wasm32"))]
    FundedOwned(Arc<(GenerationRoot, NativeAllocation)>),
    #[cfg(not(target_arch = "wasm32"))]
    NativeCheckout(Arc<(crate::collection::CheckoutRoots, NativeAllocation)>),
}

impl std::ops::Deref for PinnedReaderRoot {
    type Target = GenerationRoot;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Owned(root) => root,
            #[cfg(not(target_arch = "wasm32"))]
            Self::FundedOwned(carrier) => &carrier.0,
            #[cfg(not(target_arch = "wasm32"))]
            Self::NativeCheckout(carrier) => &carrier.0,
        }
    }
}

impl<A, O> Clone for PinnedReader<A, O> {
    fn clone(&self) -> Self {
        Self {
            volume: self.volume.clone(),
            generation_root: self.generation_root,
            root: self.root.clone(),
        }
    }
}

impl<A, O> Checkout<A, O> {
    /// Creates an immutable reader for this checkout's current private candidate.
    #[must_use]
    pub fn snapshot_reader(&self) -> PinnedReader<A, O> {
        PinnedReader {
            volume: self.volume.clone(),
            generation_root: self.generation_root,
            root: PinnedReaderRoot::Owned(self.root.clone()),
        }
    }

    /// Creates an immutable reader for a pinned checkout.
    pub fn pinned_reader(&self) -> Result<PinnedReader<A, O>, FsError> {
        if self.tracks_observations() {
            return Err(FsError::MutationNotAllowed);
        }
        Ok(self.snapshot_reader())
    }

    /// Funds only the legacy readonly root copy. It keeps standalone query
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn pinned_reader_for_materialization_with_work(
        &self,
        work: &NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<PinnedReader<A, O>, FsError> {
        if self.tracks_observations() {
            return Err(FsError::MutationNotAllowed);
        }
        cancellation.check()?;
        work.admit(WorkCounters::UNCHARGED)?;
        let mut accounting = KernelAccounting::native(work, cancellation);
        let (root, allocation) =
            crate::collection::clone_generation_root_with_accounting(&self.root, &mut accounting)?;
        let mut carrier = (root, native_pinned_allocation(allocation)?);
        cancellation.check()?;
        carrier
            .1
            .grow(size_of::<(GenerationRoot, NativeAllocation)>() as u64)?;
        let root = PinnedReaderRoot::FundedOwned(Arc::new(carrier));
        Ok(PinnedReader {
            volume: self.volume.clone(),
            generation_root: self.generation_root,
            root,
        })
    }

    /// Clones only the registered original readonly roots and their exact hold.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn pinned_reader_with_native_work(
        &self,
        work: &NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<PinnedReader<A, O>, FsError> {
        if self.tracks_observations() {
            return Err(FsError::MutationNotAllowed);
        }
        cancellation.check()?;
        work.admit(WorkCounters::UNCHARGED)?;
        if !self
            .dependencies
            .data_custody
            .is_native_original(work.native_read_meter())
            || !work.owns_native_checkout_token(&self.dependencies.data_custody)
            || !self.root.same_original_scope(work.native_read_meter())
        {
            return Err(FsError::NativeKernel(
                NativeTreeMaterializationError::InvalidLayout,
            ));
        }
        let mut accounting = KernelAccounting::native(work, cancellation);
        let roots = self
            .root
            .replicate_with_accounting(&mut accounting, Some(&self.dependencies.data_custody))?;
        cancellation.check()?;
        let allocation = {
            let claim_bytes =
                size_of::<(crate::collection::CheckoutRoots, NativeAllocation)>() as u64;
            work.claim(
                claim_bytes,
                u64::from(claim_bytes != 0),
                crate::WorkCounters::UNCHARGED,
            )
        }?;
        let root = PinnedReaderRoot::NativeCheckout(Arc::new((roots, allocation)));
        Ok(PinnedReader {
            volume: self.volume.clone(),
            generation_root: self.generation_root,
            root,
        })
    }

    /// Consumes an original-native checkout into a cheap cloneable readonly
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn into_pinned_reader_with_native_work(
        self,
        work: &NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<PinnedReader<A, O>, FsError> {
        cancellation.check()?;
        if !self
            .dependencies
            .data_custody
            .is_native_original(work.native_read_meter())
            || !work.owns_native_checkout_token(&self.dependencies.data_custody)
            || !self.root.same_original_scope(work.native_read_meter())
        {
            return Err(FsError::NativeKernel(
                NativeTreeMaterializationError::InvalidLayout,
            ));
        }
        let allocation = {
            let claim_bytes =
                size_of::<(crate::collection::CheckoutRoots, NativeAllocation)>() as u64;
            work.claim(
                claim_bytes,
                u64::from(claim_bytes != 0),
                crate::WorkCounters::UNCHARGED,
            )
        }?;
        let carrier = Arc::new((self.root, allocation));
        Ok(PinnedReader {
            volume: self.volume,
            generation_root: self.generation_root,
            root: PinnedReaderRoot::NativeCheckout(carrier),
        })
    }
}

impl<A, O> PinnedReader<A, O> {
    /// Exact immutable generation identity this reader is pinned to.
    #[must_use]
    pub const fn generation_id(&self) -> GenerationId {
        GenerationId::new(self.generation_root.digest)
    }

    /// Immutable volume limits authenticated by the reader's constructor.
    #[must_use]
    pub const fn volume_config(&self) -> VolumeConfig {
        self.volume.config
    }

    /// Checks retained original custody without admitting work or granting rights.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn validate_native_work(
        &self,
        work: &NativeMaterializationWork,
    ) -> Result<(), FsError> {
        let PinnedReaderRoot::NativeCheckout(carrier) = &self.root else {
            return Err(FsError::NativeKernel(
                NativeTreeMaterializationError::InvalidLayout,
            ));
        };
        if !carrier.1.work().shares_ledger(work)
            || !carrier.0.same_original_scope(work.native_read_meter())
        {
            return Err(FsError::NativeKernel(
                NativeTreeMaterializationError::InvalidLayout,
            ));
        }
        Ok(())
    }
}
