//! Exact selected-plan approval binding shared by every host adapter.
use super::*;
use serde::Serialize;

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
        from: &LocalGeneration,
        to: &LocalGeneration,
        paths: &'a [String],
        journal: &'a MaterializationJournal,
        context: C,
    ) -> Result<impl Serialize + 'a> {
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
        Ok((
            "acyclic.checkout-approval/v1",
            context,
            self.identity.to_bytes(),
            &self.root,
            &self.directory,
            self.operation,
            from.id(),
            to.id(),
            paths,
            &journal.plan,
            journal.root_identity,
            journal.preparation_identity,
            &journal.preimages,
        ))
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
            prop_assert!(require_selected_edits(&[MaterializationEdit::Remove { path: "selected".into() }], &paths).is_ok());
        }
    }
}
