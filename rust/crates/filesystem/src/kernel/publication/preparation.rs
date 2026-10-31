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
    Complete((PublicationReceipt, ProofGuard)),
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

struct Unflushed {
    candidate: combiner::Candidate,
    authentication: authenticate::Authenticated,
    commit: ProposedCommit,
    proven_at: u64,
}

impl<P: PublicationOwner> Preparation<'_, P> {
    async fn candidate(
        &mut self,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<combiner::Candidate, crate::FsError> {
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
        if let Some(record) = self.resolved.as_ref() {
            let decoded = decode_publication_evidence_with_accounting(
                &record.payload,
                acyclic_stream::MAX_RECORD_BYTES as u64,
                accounting,
            )?;
            return Ok(combiner::Candidate {
                root: decoded.publication.generation_root,
                expected: self.request.expected,
                predecessor: None,
                ticket: None,
            });
        }
        combiner::prepare(
            self.owner,
            self.request,
            &mut self.intent,
            self.limits,
            accounting,
            self.cancellation,
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
            .map(|ticket| ticket.take_authentication(candidate.root, accounting))
            .transpose()?
            .flatten();
        let authenticated = if let Some(prepared) = prepared {
            prepared
        } else {
            authenticate::prove(
                self.owner.objects(),
                candidate.root,
                candidate
                    .ticket
                    .as_ref()
                    .map(combiner::Ticket::authority_root),
                self.limits,
                self.cache,
                accounting,
                self.cancellation,
            )
            .await?
        };
        if authenticated.proof.root.volume_id != self.request.volume_id {
            return Err(PublicationError::VolumeMismatch {
                expected: self.request.volume_id,
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
        let proven_at = self.owner.objects().collection_sweeps();
        let mut authentication = self
            .authenticate(&candidate, accounting)
            .await
            .map_err(E::from)?;
        let fingerprint_permit = self.intent.fingerprint_permit.unwrap_or(self.intent.permit);
        let operation_context = permit_context(
            self.intent.operation_context,
            fingerprint_permit,
            accounting,
        )
        .map_err(E::from)?;
        let commit = prepare_publication_commit(
            self.request,
            Some(candidate.root),
            operation_context,
            self.intent.original_evidence.take(),
            accounting,
            self.cancellation,
        )
        .map_err(E::from)?;
        if let Some(resolved) = self.resolved.as_ref()
            && resolved.fingerprint != commit.fingerprint
        {
            return Ok(Prepared::Complete((
                PublicationReceipt {
                    proof: authentication.proof,
                    outcome: AppendOutcome::IdempotencyConflict {
                        committed_fingerprint: resolved.fingerprint,
                    },
                    work: accounting.spent(),
                },
                authentication.guard,
            )));
        }
        prepare_roots(
            &authentication.proof.root,
            &mut authentication.guard,
            accounting,
        )?;
        if let Some(resolved) = self.resolved.take() {
            let outcome = AppendOutcome::AlreadyCommitted(resolved);
            if let Some(pending) = authentication.pending {
                pending.publish(&outcome);
            }
            return Ok(Prepared::Complete((
                PublicationReceipt {
                    proof: authentication.proof,
                    outcome,
                    work: accounting.spent(),
                },
                authentication.guard,
            )));
        }
        self.flush(
            Unflushed {
                candidate,
                authentication,
                commit,
                proven_at,
            },
            accounting,
        )
        .await
        .map(Prepared::Ready)
        .map_err(E::from)
    }

    async fn flush(
        &self,
        mut prepared: Unflushed,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<PreparedGeneration, crate::FsError> {
        let objects = self.owner.objects();
        let authority = self.owner.authority();
        let request = PublishGenerationRequest {
            generation_root: prepared.candidate.root,
            expected: prepared.candidate.expected,
            ..self.request
        };
        prepared.commit.publication = if let Some(ticket) = prepared.candidate.ticket.as_ref() {
            Some(ticket.changes(prepared.candidate.root)?)
        } else {
            crate::PublicationChanges::between(
                objects,
                authority,
                crate::publication_changes::PublicationBasis {
                    request,
                    predecessor: prepared.candidate.predecessor,
                },
                &prepared.authentication.proof.root,
                accounting,
                self.cancellation,
            )
            .await?
        };
        let (projection_fence, projected) = crate::access::projection::publication(
            authority.operation_claims(),
            self.request.volume_id,
        )?;
        if projected {
            projection::check(
                objects,
                authority,
                projection::Candidate {
                    request,
                    root: &prepared.authentication.proof.root,
                    lease: &projection_fence,
                },
                self.limits.decode,
                accounting,
                self.cancellation,
            )
            .await?;
        }
        let scope = if self.cache.is_some() && objects.collection().is_none() {
            crate::PublicationScope::Everything
        } else {
            crate::PublicationScope::Closure {
                objects: &prepared.authentication.proof.objects,
                proven_at: prepared.proven_at,
            }
        };
        let publication_hold = match accounting {
            #[cfg(not(target_arch = "wasm32"))]
            KernelAccounting::NativeOriginal { work: original, .. } => {
                let mut work = crate::AuthorityWork::native(original, self.cancellation);
                objects
                    .flush_before_publish(scope, &mut work)
                    .await
                    .map_err(native_object_error)?
            }
            _ => {
                let mut work =
                    crate::AuthorityWork::new(accounting.remaining()?, self.cancellation);
                let result = objects.flush_before_publish(scope, &mut work).await;
                let result = work
                    .receipt(result)
                    .map(|receipt| (receipt.value, Some(receipt.work)))
                    .map_err(|failure| PublicationStepFailure {
                        cause: crate::FsError::Object(failure.error),
                        work: Some(*failure.work),
                    });
                accept_step(accounting, result)?
            }
        };
        let append = GuardedAppend {
            authority_id: self.request.authority_id,
            epoch: self.request.epoch,
            expected: prepared.candidate.expected,
            commit: prepared.commit,
            permit: self.intent.permit,
        };
        Ok(PreparedGeneration {
            candidate: prepared.candidate,
            authentication: prepared.authentication,
            append,
            projection_fence,
            publication_hold,
        })
    }
}
