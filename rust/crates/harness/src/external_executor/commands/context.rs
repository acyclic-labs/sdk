//! Pin an authenticated native cut before admitting its prepared child.
use super::super::{
    launch::Spawn,
    native_context::{ContextCut, source::Selection},
};
use super::*;
use crate::{conversation::FileRef, durable_host::TaskJournalOwner};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ContextFile {
    source: Selection,
    cut: ContextCut,
    seed: FileRef,
}

impl<P, A, O> RequestCommands<'_, P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn native_context(
        &self,
        context: &TaskContext,
        owner: &TaskJournalOwner<P>,
        request: &ExternalRequest,
        spawn: &Spawn,
        workspace: &str,
        reconcile: bool,
    ) -> Result<Option<FileRef>> {
        let Some(selected) = &spawn.executor.context else {
            return Ok(None);
        };
        let key = FileKey::Task {
            task: Component::new(owner.task_id().to_string()).map_err(storage)?,
            file: TaskFile::Context(
                Component::new(format!("native-{}", request.operation_id)).map_err(storage)?,
            ),
        };
        let retained = self
            .results
            .snapshot(&Query::File(key.clone()), 1)
            .await
            .map_err(storage)?;
        if let Some(file) = retained.files.first() {
            let pinned: ContextFile = volume::decode(&file.bytes).map_err(storage)?;
            if &pinned.source != selected {
                return Err(Error::Conflict(
                    "native context selection changed on retry".into(),
                ));
            }
            return Ok(Some(pinned.seed));
        }
        let workspace = if owner.admission().input.get("executor").is_some() {
            self.children
                .ok_or_else(|| Error::Unsupported("native workspace is not bound".into()))?
                .native_workspace(owner.task_id())?
        } else {
            std::path::PathBuf::from(workspace)
        };
        let ExternalAction::RunTool { call_id, .. } = &request.action else {
            unreachable!()
        };
        let (bytes, source) = selected
            .read(
                self.requests,
                &request.session_id,
                context.scope().limits().file_bytes,
            )
            .await?;
        let (source_file, cut) =
            selected.select(&bytes, &request.session_id, &workspace, call_id)?;
        let seed = source_file.child_file(
            selected.head.as_deref(),
            &cut,
            spawn.executor.session_bytes.as_bytes(),
            &workspace,
        )?;
        let seed = self
            .payloads
            .stage_input(request.operation_id, "native-context", &seed, None)?
            .await?;
        let file = FileValue::new(
            key,
            volume::encode(&ContextFile {
                source: Selection {
                    manifest: selected.manifest.clone(),
                    head: selected.head.clone(),
                    sha256: selected.sha256,
                },
                cut,
                seed: seed.clone(),
            })
            .map_err(storage)?,
        );
        selected
            .publish(
                self.requests,
                self.results,
                (
                    &request.session_id,
                    context.scope().limits().file_bytes,
                    source,
                ),
                &file,
                || async {
                    let condition = owner
                        .condition(
                            if reconcile {
                                JournalWrite::Settlement
                            } else {
                                JournalWrite::Fresh
                            },
                            &mut None,
                        )
                        .await?;
                    Ok::<_, Error>((condition, owner.admission().run_limits.deadline_epoch_ms))
                },
            )
            .await?;
        Ok(Some(seed))
    }
}
