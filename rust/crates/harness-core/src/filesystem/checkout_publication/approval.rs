//! Exact selected-plan approval binding shared by every host adapter.
use super::*;
use serde::Serialize;

/// Validated original-root selection. Its binding and publication gate share
/// one owner; adapters supply creator facts and authenticated approval only.
pub struct CheckoutApproval<'a, C> {
    publication: &'a CheckoutPublication,
    from: &'a LocalGeneration,
    to: &'a LocalGeneration,
    paths: &'a [String],
    journal: &'a MaterializationJournal,
    context: C,
}

impl<C: Serialize> Serialize for CheckoutApproval<'_, C> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        (
            "acyclic.checkout-approval/v1",
            &self.context,
            self.publication.identity.to_bytes(),
            &self.publication.root,
            &self.publication.directory,
            self.publication.operation,
            self.from.id(),
            self.to.id(),
            self.paths,
            &self.journal.plan,
            self.journal.root_identity,
            self.journal.preparation_identity,
            &self.journal.preimages,
        )
            .serialize(serializer)
    }
}

impl<'a, C: Serialize> CheckoutApproval<'a, C> {
    /// Canonical identity on the original ledger, shared by every host.
    pub fn digest(&self, work: &NativeMaterializationWork) -> Result<[u8; 32]> {
        let mut io =
            crate::store::HistoryWork::new(super::super::PreparationIo::from_native_work(work)?);
        crate::executor::json::canonical_json_digest(
            self,
            work.budget().bytes_encoded,
            &mut Some(&mut io),
        )
    }

    /// A confirmation must name the validated preparation, never only its
    /// operation or a host-supplied raw tuple.
    fn require_confirmation(
        &self,
        confirmed: &[u8; 32],
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        if &self.digest(work)? != confirmed {
            return Err(Error::Unauthorized(
                "checkout lacks exact original-root approval".into(),
            ));
        }
        Ok(())
    }

    /// Authenticates the exact prepared plan through the shared journal verifier.
    /// Possession of its public digest alone never permits publication.
    pub async fn authorize(
        self,
        verifier: &dyn crate::tool::ToolApprovalVerifier,
        interaction: crate::InteractionId,
        confirmed: &[u8; 32],
        work: &NativeMaterializationWork,
    ) -> Result<AuthorizedCheckout<'a, C>> {
        self.require_original_work(work)?;
        self.require_confirmation(confirmed, work)?;
        verifier
            .verify_with_native_work(
                interaction,
                crate::OperationId::from_bytes(self.publication.operation.into_bytes()),
                *confirmed,
                work,
            )?
            .await?;
        Ok(AuthorizedCheckout {
            approval: self,
            work: work.clone(),
        })
    }

    fn require_original_work(&self, work: &NativeMaterializationWork) -> Result<()> {
        if !self
            .publication
            ._custody
            .work()
            .shares_invocation_context(work)
            || self
                .journal
                .custody
                .as_ref()
                .is_none_or(|custody| !custody.work().shares_invocation_context(work))
        {
            return Err(Error::Unauthorized(
                "checkout approval changed its original work owner".into(),
            ));
        }
        Ok(())
    }
}

/// A selected plan whose exact operation and digest have authenticated approval.
/// Only `CheckoutApproval::authorize` can construct this publication authority.
pub struct AuthorizedCheckout<'a, C> {
    approval: CheckoutApproval<'a, C>,
    work: NativeMaterializationWork,
}

impl<C> AuthorizedCheckout<'_, C> {
    /// Publishes the authenticated selection through the common root and owner
    /// guard, retaining the caller's original native work ledger.
    pub async fn publish<F, Fut>(
        self,
        state: &LocalCoreStateStore,
        options: &MaterializeOptions,
        guard: F,
    ) -> Result<MaterializationJournal>
    where
        F: FnMut() -> Fut + Send,
        Fut: Future<Output = Result<()>> + Send,
    {
        self.approval
            .publication
            .publish(
                self.approval.from,
                self.approval.to,
                state,
                options,
                &self.work,
                guard,
            )
            .await
    }
}

impl CheckoutPublication {
    /// Prepares and validates a selected plan before presenting it to an approver.
    pub async fn prepare_selected<C: Serialize>(
        &self,
        from: &LocalGeneration,
        to: &LocalGeneration,
        state: &LocalCoreStateStore,
        options: &MaterializeOptions,
        work: &NativeMaterializationWork,
        selection: (&[String], C),
    ) -> Result<MaterializationJournal> {
        validate_paths(selection.0)?;
        let journal = self.prepare(from, to, state, options, work).await?;
        if journal.phase != acyclic_fs::MaterializationPhase::Prepared {
            return Err(Error::Conflict(
                "selected checkout is not ready for approval".into(),
            ));
        }
        self.approval_value(from, to, selection.0, &journal, selection.1)?;
        Ok(journal)
    }

    /// Borrows the exact prepared action for canonical approval encoding.
    /// Host-specific creator facts enter as opaque typed context; selection,
    /// original root, operation, generations and journal facts are owned here.
    pub fn approval_value<'a, C: Serialize + 'a>(
        &'a self,
        from: &'a LocalGeneration,
        to: &'a LocalGeneration,
        paths: &'a [String],
        journal: &'a MaterializationJournal,
        context: C,
    ) -> Result<CheckoutApproval<'a, C>> {
        validate_paths(paths)?;
        if from.workspace_id() != to.workspace_id()
            || !journal.is_valid_for(self.operation)
            || !matches!(
                journal.phase,
                acyclic_fs::MaterializationPhase::Prepared
                    | acyclic_fs::MaterializationPhase::Applied
            )
        {
            return Err(Error::Unauthorized(
                "invalid selected checkout approval".into(),
            ));
        }
        completed(journal, self.identity, from.id(), to.id())?;
        validate_plan(&journal.plan)?;
        require_selected_edits(&journal.plan.edits, paths)?;
        Ok(CheckoutApproval {
            publication: self,
            from,
            to,
            paths,
            journal,
            context,
        })
    }
}

fn validate_paths(paths: &[String]) -> Result<()> {
    if paths.is_empty() || paths.len() > crate::conversation::MAX_PRIVATE_DIRECTORY_PAGE {
        return Err(Error::Unauthorized(
            "invalid selected checkout paths".into(),
        ));
    }
    for path in paths {
        crate::conversation::validate_content_path(path, &mut None)?;
    }
    Ok(())
}

fn require_selected_edits(
    edits: &[acyclic_fs::MaterializationEdit],
    paths: &[String],
) -> Result<()> {
    let selected = |path: &str| {
        paths.iter().any(|prefix| {
            path == prefix
                || path
                    .strip_prefix(prefix)
                    .is_some_and(|suffix| suffix.starts_with('/'))
        })
    };
    for edit in edits {
        let allowed = match edit {
            acyclic_fs::MaterializationEdit::Install { path, .. }
            | acyclic_fs::MaterializationEdit::Remove { path }
            | acyclic_fs::MaterializationEdit::SetMetadata { path, .. } => selected(path),
            acyclic_fs::MaterializationEdit::Rename { from, to } => selected(from) && selected(to),
            _ => false,
        };
        if !allowed {
            return Err(Error::Unauthorized(
                "native candidate changes an unselected writeback path".into(),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_fs::MaterializationEdit;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        #[test]
        fn selected_edits_require_path_boundaries_and_both_rename_ends(
            suffix in "[a-z]{1,12}", from_selected in any::<bool>(), to_selected in any::<bool>(),
        ) {
            let paths = vec!["selected".into()];
            let from = if from_selected { format!("selected/{suffix}") } else { format!("selected-{suffix}") };
            let to = if to_selected { format!("selected/{suffix}-to") } else { format!("selected-{suffix}-to") };
            let edits = vec![MaterializationEdit::Rename { from: from.clone(), to: to.clone() }];
            prop_assert_eq!(require_selected_edits(&edits, &paths).is_ok(), from_selected && to_selected);
            // A string-prefix-only selector and a source-only rename selector
            // each authorize an effect the canonical selector refuses.
            prop_assert!(from.starts_with("selected") && to.starts_with("selected"));
            if from_selected && !to_selected { prop_assert!(require_selected_edits(&edits, &paths).is_err()); }
            let exact = [MaterializationEdit::Remove { path: "selected".into() }];
            prop_assert!(require_selected_edits(&exact, &paths).is_ok());
        }
    }
}
