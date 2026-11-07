//! The original generation preparation, before any authority submission.
use super::*;
use crate::{AsyncAuthorityStore, AsyncObjectStore};

pub(super) struct Preparation<'a, P> {
    pub(super) owner: &'a P,
    pub(super) request: PublishGenerationRequest,
    pub(super) intent: PublicationIntent,
    pub(super) limits: ClosureLimits,
    pub(super) cache: Option<&'a Mutex<CachedProofs>>,
    pub(super) cancellation: &'a CancellationToken,
    pub(super) resolved: Option<crate::foundation::DurableCommit>,
}

pub(super) enum Prepared {
    Ready(PreparedGeneration),
    Complete(RetainedPublication<ProofGuard>),
}

// Data retires before its proof guard; both remain inside the projection and
// flush custody until submission and accepted-outcome handling finish.
pub(super) struct PreparedGeneration {
    pub(super) candidate: combiner::Candidate,
    pub(super) authentication: authenticate::Authenticated,
    pub(super) append: GuardedAppend,
    pub(super) projection_fence: crate::access::projection::Lease,
    pub(super) publication_hold: crate::PublicationHold,
}

pub(super) struct ValidatedPreparation<'a, P>(pub(super) Preparation<'a, P>);

impl<'a, P: PublicationOwner> Preparation<'a, P> {
    pub(super) fn validate(
        self,
        accounting: &KernelAccounting<'_>,
    ) -> Result<ValidatedPreparation<'a, P>, crate::FsError> {
        let expected = volume_authority_id(self.request.volume_id);
        if self.request.authority_id != expected {
            return Err(PublicationError::AuthorityMismatch {
                volume_id: self.request.volume_id,
                expected,
                actual: self.request.authority_id,
            }
            .into());
        }
        self.cancellation.check()?;
        accounting.check()?;
        Ok(ValidatedPreparation(self))
    }
}

impl<P: PublicationOwner> ValidatedPreparation<'_, P> {
    async fn candidate(
        &mut self,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<combiner::Candidate, crate::FsError> {
        if let Some(record) = self.0.resolved.as_ref() {
            let decoded = decode_publication_evidence_with_accounting(
                &record.payload,
                acyclic_stream::MAX_RECORD_BYTES as u64,
                accounting,
            )?;
            return Ok(combiner::Candidate {
                root: decoded.publication.generation_root,
                expected: self.0.request.expected,
                predecessor: None,
                ticket: None,
            });
        }
        combiner::prepare(
            self.0.owner,
            self.0.request,
            &mut self.0.intent,
            self.0.limits,
            accounting,
            self.0.cancellation,
        )
        .await
    }

    async fn authenticate(
        &self,
        candidate: &combiner::Candidate,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<authenticate::Authenticated, crate::FsError> {
        let prepared = candidate
            .ticket
            .as_ref()
            .and_then(|ticket| ticket.take_authentication(candidate.root));
        let authenticated = if let Some(prepared) = prepared {
            prepared
        } else {
            authenticate::prove(
                self.0.owner.objects(),
                candidate.root,
                candidate
                    .ticket
                    .as_ref()
                    .map(combiner::Ticket::authority_root),
                self.0.limits,
                self.0.cache,
                accounting,
                self.0.cancellation,
            )
            .await?
        };
        if authenticated.proof.root.volume_id != self.0.request.volume_id {
            return Err(PublicationError::VolumeMismatch {
                expected: self.0.request.volume_id,
                actual: authenticated.proof.root.volume_id,
            }
            .into());
        }
        Ok(authenticated)
    }

    pub(super) async fn prepare<E, R>(
        mut self,
        accounting: &mut KernelAccounting<'_>,
        prepare_roots: &mut R,
    ) -> Result<Prepared, E>
    where
        E: From<crate::FsError> + Send,
        R: FnMut(
            &crate::kernel::GenerationRoot,
            &mut ProofGuard,
            &mut KernelAccounting<'_>,
        ) -> Result<(), E>,
    {
        let candidate = self.candidate(accounting).await.map_err(E::from)?;
        let proven_at = self.0.owner.objects().collection_sweeps();
        let mut authentication = self
            .authenticate(&candidate, accounting)
            .await
            .map_err(E::from)?;
        let fingerprint_permit = self
            .0
            .intent
            .fingerprint_permit
            .unwrap_or(self.0.intent.permit);
        let operation_context = permit_context(
            self.0.intent.operation_context,
            fingerprint_permit,
            accounting,
        )
        .map_err(E::from)?;
        let mut commit = prepare_publication_commit(
            self.0.request,
            Some(candidate.root),
            operation_context,
            self.0.intent.original_evidence.take(),
            accounting,
            self.0.cancellation,
        )
        .map_err(E::from)?;
        if let Some(resolved) = self.0.resolved.as_ref()
            && resolved.fingerprint != commit.fingerprint
        {
            return Ok(Prepared::Complete(RetainedPublication {
                receipt: PublicationReceipt {
                    proof: authentication.proof,
                    outcome: AppendOutcome::IdempotencyConflict {
                        committed_fingerprint: resolved.fingerprint,
                    },
                    work: accounting.spent(),
                },
                published: None,
                allocation: authentication.guard,
            }));
        }
        prepare_roots(
            &authentication.proof.root,
            &mut authentication.guard,
            accounting,
        )?;
        if let Some(resolved) = self.0.resolved.take() {
            let outcome = AppendOutcome::AlreadyCommitted(resolved);
            let published = authentication
                .pending
                .and_then(|pending| pending.publish(&outcome));
            return Ok(Prepared::Complete(RetainedPublication {
                receipt: PublicationReceipt {
                    proof: authentication.proof,
                    outcome,
                    work: accounting.spent(),
                },
                published,
                allocation: authentication.guard,
            }));
        }
        let objects = self.0.owner.objects();
        let authority = self.0.owner.authority();
        let request = PublishGenerationRequest {
            generation_root: candidate.root,
            expected: candidate.expected,
            ..self.0.request
        };
        commit.publication = if let Some(ticket) = candidate.ticket.as_ref() {
            Some(ticket.changes(candidate.root).map_err(E::from)?)
        } else {
            crate::PublicationChanges::between(
                objects,
                authority,
                crate::publication_changes::PublicationBasis {
                    request,
                    predecessor: candidate.predecessor,
                },
                &authentication.proof.root,
                accounting,
                self.0.cancellation,
            )
            .await
            .map_err(E::from)?
        };
        let (projection_fence, projected) = crate::access::projection::publication(
            authority.operation_claims(),
            self.0.request.volume_id,
        )
        .map_err(E::from)?;
        if projected {
            projection::check(
                objects,
                authority,
                projection::Candidate {
                    request,
                    root: &authentication.proof.root,
                    lease: &projection_fence,
                },
                self.0.limits.decode,
                accounting,
                self.0.cancellation,
            )
            .await
            .map_err(E::from)?;
        }
        let scope = crate::PublicationScope {
            objects: &authentication.proof.objects,
            proven_at,
        };
        let mut work = crate::async_storage::AuthorityFrame::borrow(accounting).map_err(E::from)?;
        #[cfg(not(target_arch = "wasm32"))]
        let native = work.is_native();
        let publication_hold = objects.flush_before_publish(scope, &mut work).await;
        drop(work);
        let publication_hold = publication_hold
            .map_err(|error| {
                #[cfg(not(target_arch = "wasm32"))]
                if native {
                    return native_object_error(error);
                }
                crate::FsError::Object(error)
            })
            .map_err(E::from)?;
        let append = GuardedAppend {
            authority_id: self.0.request.authority_id,
            epoch: self.0.request.epoch,
            expected: candidate.expected,
            commit,
            permit: self.0.intent.permit,
        };
        Ok(Prepared::Ready(PreparedGeneration {
            candidate,
            authentication,
            append,
            projection_fence,
            publication_hold,
        }))
    }
}
