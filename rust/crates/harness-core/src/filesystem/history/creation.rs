//! The original adoption boundary joins the existing canonical history stream.
use super::*;
use crate::agent::InboxRecord;
#[cfg(test)]
mod tests;

pub(in crate::filesystem) async fn capture_creation<P, O>(
    fs: &Fs<StreamAuthorityStore<P>, O>,
    agent: &AgentPath,
    prepared: &[acyclic_fs::PreparedGeneration],
    commit: &mut FilesystemCommit,
) -> Result<(), GuestError>
where
    P: AuthorityProvider + ?Sized + 'static,
    O: AsyncObjectStore + 'static,
{
    let history = agent.history().map_err(guest)?;
    // Inherited cases keep their original boundaries, including boundary zero.
    if commit.request().effects.iter().flat_map(|effect| &effect.mutations)
        .any(|mutation| matches!(mutation, CommitMutation::Fork {destination, ..} if *destination == history))
    {
        return Ok(());
    }
    let inbox = agent.inbox().map_err(guest)?;
    let original = commit
        .request()
        .effects
        .iter()
        .flat_map(|effect| &effect.mutations)
        .find_map(|mutation| match mutation {
            CommitMutation::Append { path, records }
            | CommitMutation::Fork {
                destination: path,
                records,
                ..
            } if *path == inbox => records.last(),
            _ => None,
        })
        .ok_or_else(|| guest("creation has no original adoption"))?;
    let InboxRecord::ForkAdopted(adopted) = InboxRecord::decode(&original.value).map_err(guest)?
    else {
        return Err(guest("creation does not end with its adoption"));
    };
    if adopted.agent != agent.id() || adopted.conversation_tail != 0 {
        return Err(guest("creation history differs from its original adoption"));
    }
    let cancellation = CancellationToken::new();
    let mut volumes = Vec::new();
    for binding in &adopted.volumes {
        let Some(volume) = binding
            .as_volume()
            .filter(|volume| volume.class() == VolumeClass::Project)
        else {
            continue;
        };
        let name = volume.storage_name(&mut None).map_err(guest)?;
        let workspace = fs.workspace_id(&name).map_err(guest)?;
        if let Some(original) = prepared
            .iter()
            .find(|original| original.workspace == workspace)
        {
            // A prepared creation/fork retains its immutable original generation;
            // its containing transaction still owns all publication custody.
            volumes.push(SavedVolume {
                binding: binding.clone(),
                generation: original.generation,
                retained_workspace: name,
                retained_generation: original.generation,
            });
        } else {
            let source = fs
                .open_workspace(&name, WorkBudget::UNBOUNDED, &cancellation)
                .await
                .map_err(|failure| guest(failure.error))?
                .value;
            let head = source
                .head(WorkBudget::UNBOUNDED, &cancellation)
                .await
                .map_err(|failure| guest(failure.error))?
                .value;
            fs.require_head(&head, commit, WorkBudget::UNBOUNDED, &cancellation)
                .await
                .map_err(|failure| guest(failure.error))?;
            volumes.push(
                retention::retain(fs, (agent, 0), binding, &head, None, commit, &cancellation)
                    .await?,
            );
        }
    }
    append(agent, volumes, commit)
}

fn append(
    agent: &AgentPath,
    volumes: Vec<crate::agent::SavedVolume>,
    commit: &mut FilesystemCommit,
) -> Result<(), GuestError> {
    let baseline = HistoryCheckpoint {
        version: 3,
        live_children: false,
        conversation: 0,
        checkpoint: None,
        context: ContextCut {
            path: acyclic_stream::StreamPath::new(format!("{agent}/context/0")).map_err(guest)?,
            tail: 0,
            checkpoint: None,
        },
        volumes,
    };
    let history = agent.history().map_err(guest)?;
    let request = commit
        .request_mut()
        .ok_or_else(|| guest("creation commit is sealed"))?;
    request.effects[0].conditions.push(CommitCondition::Tail {
        path: history.clone(),
        expected: 0,
    });
    request.effects[0].mutations.push(CommitMutation::Append {
        path: history,
        records: vec![serde_json::to_vec(&baseline).map_err(guest)?.into()],
    });
    Ok(())
}
