use super::*;

/// A checkout's roots and their live collection registration.
pub(crate) struct CheckoutRoots {
    pub(crate) base: ObjectId,
    pub(super) working: GenerationRoot,
    pub(super) config: crate::model::VolumeConfig,
    pub(crate) hold: Option<Hold>,
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
                    Held::Checkout {
                        base,
                        working: held_working.0,
                        config,
                        control: held_working.1,
                    },
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

    #[cfg(any(test, feature = "native-watch"))]
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

    /// Verifies the working and held vectors' original payment.
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
        let holds = hold.collection.holds();
        match &holds.slots[hold.id] {
            Slot::Occupied(Held::Checkout {
                working, control, ..
            }) => control.is_native_original(original, root_parent_bytes(working)),
            _ => false,
        }
    }

    pub(crate) fn logical_owned_bytes(&self) -> u64 {
        root_parent_bytes(&self.working) + self.hold.as_ref().map_or(0, Hold::logical_owned_bytes)
    }

    /// Tags freshly admitted vectors, excluding shared slot backing.
    pub(crate) fn attach_control_custody(&mut self, owner: &Arc<crate::kernel::ProofDataCustody>) {
        self.working_control =
            crate::kernel::ProofDataClaim::control(owner, root_parent_bytes(&self.working));
        if let Some(hold) = &self.hold {
            let mut holds = hold.collection.holds();
            if let Slot::Occupied(Held::Checkout {
                working, control, ..
            }) = &mut holds.slots[hold.id]
            {
                *control =
                    crate::kernel::ProofDataClaim::control(owner, root_parent_bytes(working));
            }
        }
    }

    #[cfg(feature = "native-mount")]
    pub(crate) fn set_file_table(&mut self, table: ObjectId) -> Result<(), crate::FsError> {
        if let Some(hold) = &self.hold {
            let mut holds = hold.collection.holds();
            let Some(Slot::Occupied(Held::Checkout { working, .. })) = holds.slots.get_mut(hold.id)
            else {
                return Err(crate::FsError::InvalidDiff);
            };
            working.file_table = table;
        }
        self.working.file_table = table;
        Ok(())
    }

    /// Installs admitted roots, retiring each old buffer before its guard.
    pub(crate) fn set(
        &mut self,
        base: ObjectId,
        working: GenerationRoot,
        control: crate::kernel::ProofDataClaim,
        held: Option<(GenerationRoot, crate::kernel::ProofDataClaim)>,
    ) {
        self.base = base;
        if let Some(hold) = &self.hold {
            let (working, control) = held.expect("the existing registration has a prepared root");
            hold.update(Held::Checkout {
                base: self.base,
                working,
                config: self.config,
                control,
            });
        } else {
            debug_assert!(held.is_none());
        }
        self.working = working;
        self.working_control = control;
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
