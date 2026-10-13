use super::*;

/// A checkout's base and working roots, which a collection treats as live.
/// They change only through [`Self::set_prepared_root`] and [`Self::set_working`], which
/// keep the registration exact.
pub(crate) struct CheckoutRoots {
    pub(super) base: ObjectId,
    pub(super) working: GenerationRoot,
    pub(super) config: crate::model::VolumeConfig,
    pub(super) hold: Option<Hold>,
    // Last: both working and collection-held root buffers retire first.
    pub(super) working_control: crate::kernel::ProofDataClaim,
}

impl CheckoutRoots {
    pub(crate) fn new(
        collection: Option<&Arc<Collection>>,
        config: crate::model::VolumeConfig,
        base: ObjectId,
        working: (GenerationRoot, crate::kernel::ProofDataClaim),
        accounting: &mut KernelAccounting<'_>,
        native_owner: Option<&Arc<crate::kernel::ProofDataCustody>>,
    ) -> Result<Self, crate::FsError> {
        let hold = match collection {
            Some(collection) => {
                let held_working = clone_generation_root(&working.0, accounting, native_owner)?;
                Some(collection.register_with_accounting(
                    Held::Checkout(HeldRoots {
                        base,
                        working: held_working.0,
                        config,
                        control: held_working.1,
                    }),
                    accounting,
                )?)
            }
            None => None,
        };
        Ok(Self {
            base,
            working: working.0,
            config,
            hold,
            working_control: working.1,
        })
    }

    pub(crate) fn replicate(
        &self,
        accounting: &mut KernelAccounting<'_>,
        native_owner: Option<&Arc<crate::kernel::ProofDataCustody>>,
    ) -> Result<Self, crate::FsError> {
        let working = clone_generation_root(&self.working, accounting, native_owner)?;
        Self::new(
            self.hold.as_ref().map(|hold| &hold.collection),
            self.config,
            self.base,
            working,
            accounting,
            native_owner,
        )
    }

    /// Verifies the exact working/held vectors that move into an original
    /// pinned reader. Cached slot storage and token lineage prove no payment.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn same_original_scope(
        &self,
        original: &Arc<crate::demand::NativeDemandReadMeter>,
    ) -> bool {
        if !self
            .working_control
            .is_native_original(original, root_parent_bytes(&self.working))
        {
            return false;
        }
        let Some(hold) = &self.hold else {
            return true;
        };
        let holds = hold
            .collection
            .holds
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        match holds.slots.get(hold.id).and_then(|slot| slot.held.as_ref()) {
            Some(Held::Checkout(roots)) => roots
                .control
                .is_native_original(original, root_parent_bytes(&roots.working)),
            _ => false,
        }
    }

    pub(crate) fn logical_owned_bytes(&self) -> u64 {
        crate::foundation::usize_to_u64(
            self.working.parents.capacity() * std::mem::size_of::<crate::GenerationId>(),
        ) + self.hold.as_ref().map_or(0, Hold::logical_owned_bytes)
    }

    /// Only freshly cloned, measured root buffers receive these exact tags.
    /// Collection slot capacity is cached shared storage, not handle custody.
    pub(crate) fn attach_control_custody(&mut self, owner: &Arc<crate::kernel::ProofDataCustody>) {
        self.working_control =
            crate::kernel::ProofDataClaim::control(owner, root_parent_bytes(&self.working));
        if let Some(hold) = &self.hold {
            let mut holds = hold
                .collection
                .holds
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let held = holds
                .slots
                .get_mut(hold.id)
                .and_then(|slot| slot.held.as_mut())
                .expect("a freshly admitted hold remains registered");
            if let Held::Checkout(roots) = held {
                roots.control = crate::kernel::ProofDataClaim::control(
                    owner,
                    root_parent_bytes(&roots.working),
                );
            }
        }
    }

    pub(crate) fn prepare_held_root(
        &self,
        working: &GenerationRoot,
        accounting: &mut KernelAccounting<'_>,
        owner: Option<&Arc<crate::kernel::ProofDataCustody>>,
    ) -> Result<Option<(GenerationRoot, crate::kernel::ProofDataClaim)>, crate::FsError> {
        self.hold
            .as_ref()
            .map(|_| clone_generation_root(working, accounting, owner))
            .transpose()
    }

    /// Moves already admitted root copies after the publication seal.
    /// Replacements destroy each old vector before retiring its tag.
    pub(crate) fn set_prepared_root(
        &mut self,
        base: ObjectId,
        working: GenerationRoot,
        control: crate::kernel::ProofDataClaim,
        held: Option<(GenerationRoot, crate::kernel::ProofDataClaim)>,
    ) {
        self.base = base;
        self.set_prepared_working(working, control, held);
    }

    /// Replaces only the working tree with admitted buffers; base lineage and
    /// the checkout's base-root copy remain unchanged.
    pub(crate) fn set_prepared_working(
        &mut self,
        working: GenerationRoot,
        control: crate::kernel::ProofDataClaim,
        held: Option<(GenerationRoot, crate::kernel::ProofDataClaim)>,
    ) {
        if let Some(hold) = &self.hold {
            let (working, control) = held.expect("the existing registration has a prepared root");
            hold.update(Held::Checkout(HeldRoots {
                base: self.base,
                working,
                config: self.config,
                control,
            }));
        } else {
            debug_assert!(held.is_none());
        }
        self.working = working;
        self.working_control = control;
    }

    /// The generation the checkout is based on.
    pub(crate) const fn base(&self) -> ObjectId {
        self.base
    }

    /// Replaces the working tree.
    pub(crate) fn set_working(&mut self, working: GenerationRoot) {
        if let Some(hold) = &self.hold {
            hold.update(Held::Checkout(HeldRoots {
                base: self.base,
                working: working.clone(),
                config: self.config,
                control: crate::kernel::ProofDataClaim::default(),
            }));
        }
        self.working = working;
        self.working_control = crate::kernel::ProofDataClaim::default();
    }
}

impl PartialEq for CheckoutRoots {
    fn eq(&self, other: &Self) -> bool {
        self.base == other.base && self.working == other.working
    }
}

impl Deref for CheckoutRoots {
    type Target = GenerationRoot;

    fn deref(&self) -> &GenerationRoot {
        &self.working
    }
}
