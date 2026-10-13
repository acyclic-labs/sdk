use super::*;

impl MemoryAuthorityStore {
    pub(super) fn lookup_reservation(
        &self,
        authority: AuthorityId,
        operation: OperationId,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Option<crate::PublicationReservation>, FsError> {
        accounting.charge(read_work())?;
        let authorities = self
            .authorities
            .lock()
            .map_err(|_| AuthorityStoreError::MemoryAuthorityPoisoned)?;
        examine(accounting)?;
        let state = authorities
            .get(&authority)
            .ok_or(AuthorityStoreError::Missing)?;
        let proof = state
            .publication_reservation
            .filter(|proof| proof.operation_id == operation);
        if proof.is_some() {
            accounting.charge(copy_work(1, size_of::<crate::PublicationReservation>())?)?;
        }
        Ok(proof)
    }

    pub(super) fn acquire_reservation(
        &self,
        authority: AuthorityId,
        expected: Head,
        operation: OperationId,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<crate::ReservationOutcome, FsError> {
        accounting.charge(read_work())?;
        let mut authorities = self
            .authorities
            .lock()
            .map_err(|_| AuthorityStoreError::MemoryAuthorityPoisoned)?;
        examine(accounting)?;
        let state = authorities
            .get_mut(&authority)
            .ok_or(AuthorityStoreError::Missing)?;
        let actual = state.head();
        if actual != expected {
            return Ok(crate::ReservationOutcome::Conflict { actual });
        }
        if let Some(proof) = state.publication_reservation {
            if proof.operation_id != operation || proof.expected != expected {
                return Ok(crate::ReservationOutcome::Conflict { actual });
            }
            accounting.charge(copy_work(1, size_of::<crate::PublicationReservation>())?)?;
            return Ok(crate::ReservationOutcome::AlreadyReserved(proof));
        }
        let gate_tail = state
            .publication_gate_tail
            .max(1)
            .checked_add(1)
            .ok_or(WorkError::Overflow)?;
        let copied = copy_work(2, size_of::<crate::PublicationReservation>())?;
        accounting.admit(write_work().checked_add(copied)?)?;
        let envelope = accounting.claim_owned(
            size_of::<(u64, (crate::PublicationReservation, KernelOwnedAllocation))>() as u64,
            0,
        )?;
        reserve(&mut state.publication_proofs, accounting)?;
        accounting.check()?;
        accounting.record_completed(write_work())?;
        accounting.record_completed(copied)?;
        let proof = crate::PublicationReservation {
            authority_id: authority,
            operation_id: operation,
            expected,
            gate_tail,
        };
        state
            .publication_proofs
            .insert(gate_tail, (proof, envelope));
        state.publication_reservation = Some(proof);
        state.publication_gate_tail = gate_tail;
        Ok(crate::ReservationOutcome::Reserved(proof))
    }

    pub(super) fn drop_reservation(
        &self,
        proof: crate::PublicationReservation,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), FsError> {
        accounting.charge(read_work())?;
        let mut authorities = self
            .authorities
            .lock()
            .map_err(|_| AuthorityStoreError::MemoryAuthorityPoisoned)?;
        examine(accounting)?;
        let state = authorities
            .get_mut(&proof.authority_id)
            .ok_or(AuthorityStoreError::Missing)?;
        let original = state.publication_proofs.get(&proof.gate_tail).ok_or(
            AuthorityStoreError::RejectedStatic(
                "publication reservation is no longer owned by this operation",
            ),
        )?;
        if original.0 != proof {
            return Err(AuthorityStoreError::RejectedStatic(
                "publication reservation does not match its original proof",
            )
            .into());
        }
        if state.publication_gate_tail > proof.gate_tail {
            return Ok(());
        }
        if state.publication_reservation != Some(proof) {
            return Err(AuthorityStoreError::RejectedStatic(
                "publication reservation is no longer active",
            )
            .into());
        }
        let next = proof.gate_tail.checked_add(1).ok_or(WorkError::Overflow)?;
        accounting.admit(write_work())?;
        accounting.check()?;
        accounting.record_completed(write_work())?;
        state.publication_reservation = None;
        state.publication_gate_tail = next;
        Ok(())
    }
}
