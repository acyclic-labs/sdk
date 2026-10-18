use super::*;

pub(crate) fn validate_candidate(candidate: &MultiRootMergeCandidate) -> Result<(), ()> {
    if candidate.plan.roots.is_empty()
        || candidate.plan.parent_context_id == candidate.plan.child_context_id
        || candidate.plan.roots.keys().ne(candidate.resolutions.keys())
        || candidate
            .plan
            .roots
            .values()
            .any(|root| root.source_workspace_id == root.target_workspace_id)
    {
        return Err(());
    }
    Ok(())
}

pub(super) fn is_pristine_unclaimed(journal: &MultiRootPublication) -> bool {
    journal.phase == MultiRootPublicationPhase::Validating
        && journal.fences.is_empty()
        && journal.conflicts.is_empty()
        && journal.projected_roots.is_empty()
        && journal.declared_conflicts.is_empty()
        && journal.published_roots.is_empty()
        && journal.published_generations.is_empty()
        && journal.paused_root.is_none()
}

pub(crate) fn validate_journal(journal: &MultiRootPublication) -> Result<(), ()> {
    let roots: BTreeSet<_> = journal.candidate.plan.roots.keys().copied().collect();
    let fence_roots: BTreeSet<_> = journal.fences.keys().copied().collect();
    let generation_roots: BTreeSet<_> = journal.published_generations.keys().copied().collect();
    let no_publication_progress =
        journal.published_roots.is_empty() && journal.published_generations.is_empty();
    let no_conflict_state = journal.conflicts.is_empty()
        && journal.projected_roots.is_empty()
        && journal.declared_conflicts.is_empty();
    let phase_valid = match journal.phase {
        MultiRootPublicationPhase::Validating => {
            no_publication_progress
                && journal.projected_roots.is_empty()
                && journal.declared_conflicts.is_empty()
                && fence_roots.is_subset(&roots)
                && journal.paused_root.is_none()
        }
        MultiRootPublicationPhase::Conflicted => {
            no_publication_progress
                && journal.fences.is_empty()
                && !journal.conflicts.is_empty()
                && journal.paused_root.is_none()
        }
        MultiRootPublicationPhase::Aborting => {
            journal.published_generations.is_empty()
                && journal.fences.is_empty()
                && !journal.conflicts.is_empty()
                && journal.declared_conflicts.is_empty()
                && journal.paused_root.is_none()
        }
        MultiRootPublicationPhase::Committed => {
            no_publication_progress
                && no_conflict_state
                && fence_roots == roots
                && journal.paused_root.is_none()
        }
        MultiRootPublicationPhase::Publishing => {
            no_conflict_state
                && fence_roots == roots
                && generation_roots == journal.published_roots
                && journal.paused_root.is_none()
        }
        MultiRootPublicationPhase::Paused => {
            no_conflict_state
                && fence_roots == roots
                && generation_roots == journal.published_roots
                && journal.paused_root.is_some_and(|root| {
                    roots.contains(&root) && !journal.published_roots.contains(&root)
                })
        }
        MultiRootPublicationPhase::Applied => {
            journal.conflicts.is_empty()
                && journal.projected_roots.is_empty()
                && fence_roots == roots
                && journal.published_roots == roots
                && generation_roots == roots
                && journal.paused_root.is_none()
        }
    };
    if journal.version != MULTI_ROOT_VERSION
        || journal.revision == 0
        || !journal.published_roots.is_subset(&roots)
        || !journal.conflicts.keys().all(|root| roots.contains(root))
        || !journal
            .projected_roots
            .keys()
            .all(|root| roots.contains(root))
        || !journal
            .declared_conflicts
            .keys()
            .all(|root| roots.contains(root))
        || !phase_valid
    {
        return Err(());
    }
    validate_candidate(&journal.candidate)
}
