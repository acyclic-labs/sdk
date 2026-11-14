use super::*;

pub(super) async fn retain<P, O>(
    fs: &Fs<StreamAuthorityStore<P>, O>,
    (agent, sequence): (&AgentPath, u64),
    binding: &crate::agent::WorkspaceEntry,
    head: &acyclic_fs::Generation<StreamAuthorityStore<P>, O>,
    cuts: Option<&dyn super::super::VolumeResources>,
    commit: &mut FilesystemCommit,
    cancel: &CancellationToken,
) -> Result<SavedVolume, GuestError>
where
    P: AuthorityProvider + ?Sized + 'static,
    O: AsyncObjectStore + 'static,
{
    let mut hash = blake3::Hasher::new_derive_key("acyclic.harness.history.volume.v1");
    hash.update(agent.as_str().as_bytes());
    hash.update(&sequence.to_be_bytes());
    hash.update(&binding.id.into_bytes());
    // A refused publication can retry this turn at a newer project
    // head. Both the companion cut and fork operation belong to the
    // complete selected generation, not just the conversation slot.
    hash.update(head.id().to_bytes().as_slice());
    let identity = hash.finalize();
    let mut key = [0; 16];
    key.copy_from_slice(&identity.as_bytes()[..16]);
    let destination = WorkspaceName::new(
        format!("history-{}", identity.to_hex()),
        &WorkBudget::UNBOUNDED,
        cancel,
    )
    .map_err(|failure| guest(failure.error))?
    .value;
    let retained_workspace = destination.as_str().to_owned();
    if let Some(cuts) = cuts {
        cuts.cut(
            &binding
                .volume()
                .map_err(guest)?
                .storage_name(&mut None)
                .map_err(guest)?,
            &retained_workspace,
            acyclic_fs::OperationId::from_bytes(key),
            Some(head.id()),
        )
        .await
        .map_err(GuestError::from)?;
    }
    let prepared = fs
        .prepare_fork_many(
            &[ForkSpec {
                destination,
                source: head,
                paths: None,
                idempotency_key: IdempotencyKey::from_bytes(key),
            }],
            commit,
            WorkBudget::UNBOUNDED,
            cancel,
        )
        .await
        .map_err(|failure| guest(failure.error))?
        .value;
    let prepared = prepared
        .first()
        .ok_or_else(|| guest("retained generation is absent"))?;
    Ok(SavedVolume {
        binding: binding.clone(),
        generation: head.id(),
        retained_workspace,
        retained_generation: prepared.generation,
    })
}
