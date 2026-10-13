//! Original Root SOURCE reads through the selected SDK inspection.
use super::*;

impl crate::pi_host::PiControlHost<'_> {
    pub(crate) async fn run_builtin(
        &self,
        context: std::sync::Arc<acyclic_harness::runtime::TaskContext>,
        fence: acyclic_harness::scheduler::LeaseFence,
        command: acyclic_harness::workflow::WorkflowCommand,
        payload: serde_json::Value,
    ) -> Result<acyclic_harness::filesystem::TaskCommandProgress> {
        let work = &self.work;
        let decoded = PiBuiltinRequest::decode(&payload, work)?;
        drop(payload);
        let request = &decoded.0;
        if context.durable_task_id() != Some(request.caller)
            || command.kind != PI_BUILTIN_COMMAND_KIND
            || command.operation_id != request.operation(self.protected.policy().root_task, work)?
        {
            return Err(denied("builtin command changed its task or SDK operation"));
        }
        let caller = self.caller(&context, &fence, work).await?;
        let owner = &caller.0;
        let input = &caller.1;
        if request.caller != self.protected.policy().root_task {
            let live = self
                .native()?
                .live_source(request.caller, work)
                .await?
                .ok_or_else(|| denied("builtin child has no original live view"))?;
            if live.prepared.binding != input.binding {
                return Err(denied("builtin child changed its admitted binding"));
            }
            let (value, custody) = native_io::accepted_builtin_io(
                &context,
                &live.owner,
                &live.view,
                request,
                &input.binding,
                work,
            )
            .await?;
            return Ok(
                acyclic_harness::filesystem::TaskCommandProgress::ReadyWithNativeCustody {
                    value,
                    custody,
                },
            );
        }
        request.require_root_source_read(self.protected.policy().root_task)?;
        if input.binding.task_id != request.caller
            || input.binding.agent_id != self.protected.policy().agent
        {
            return Err(denied(
                "builtin Root binding differs from its admitted owner",
            ));
        }
        let crate::pi_task::PiTaskExecution::Host { root, .. } = &input.execution else {
            return Err(denied("builtin Root has no host admission"));
        };
        let crate::pi_root::PiRootIntent::Project { paths, root_id, .. } = root else {
            return Err(denied("builtin Root has no physical SOURCE"));
        };
        let (key, relative) = select_root_path(&request.path, paths, root_id)?;
        let root = Path::new(&paths[key]);
        let PiBuiltinIo::Read {
            maximum_bytes,
            variants,
        } = &request.io
        else {
            unreachable!("Root read checked");
        };
        let relatives = scoped_variants(variants, root)?;
        let selection = self
            .selected(
                &context,
                owner,
                request.caller,
                (Some(key), None),
                request.operation(self.protected.policy().root_task, work)?,
                work,
            )
            .await?;
        let mut candidates = std::iter::once(relative)
            .chain(relatives.into_iter().flatten())
            .zip(std::iter::once(&request.path).chain(variants.iter()))
            .peekable();
        while let Some((path, candidate)) = candidates.next() {
            let path = logical_path(path, work)?;
            match selection.read(&path.0, *maximum_bytes, work).await {
                Ok(bytes) => {
                    authorize_owner(&context, owner, request.caller, work).await?;
                    let (value, custody) =
                        read_response(bytes.as_slice(), candidate, "SOURCE", work)?;
                    return Ok(
                        acyclic_harness::filesystem::TaskCommandProgress::ReadyWithNativeCustody {
                            value,
                            custody,
                        },
                    );
                }
                Err(error) if is_absent(&error) && candidates.peek().is_some() => {}
                Err(error) => return Err(error),
            }
        }
        Err(denied("requested candidate is absent"))
    }
}

pub(crate) async fn authorize_owner(
    context: &acyclic_harness::runtime::TaskContext,
    owner: &Owner,
    caller: TaskId,
    work: &NativeMaterializationWork,
) -> Result<()> {
    if owner.task_id() != caller || context.durable_task_id() != Some(caller) {
        return Err(denied(
            "builtin caller differs from its original task owner/context",
        ));
    }
    owner
        .verify(
            false,
            &mut Some(&mut acyclic_harness::store::HistoryWork::new(
                acyclic_harness::filesystem::PreparationIo::from_native_work(work)?,
            )),
        )
        .await
}

fn select_root_path<'p, 'r>(
    path: &'p str,
    roots: &'r std::collections::BTreeMap<String, String>,
    default: &[u8; 16],
) -> Result<(&'r str, &'p Path)> {
    validate_path(path)?;
    if !Path::new(path).is_absolute() {
        let (key, root) = roots
            .iter()
            .find(|(key, _)| {
                let mut identity = [0; 16];
                hex::decode_to_slice(key.as_bytes(), &mut identity).is_ok() && identity == *default
            })
            .ok_or_else(|| denied("builtin original Root cwd is absent"))?;
        return Ok((key, relative_path(path, Path::new(root))?));
    }
    let mut selected = None;
    for (key, root) in roots {
        if let Ok(relative) = relative_path(path, Path::new(root)) {
            if selected.is_some() {
                return Err(denied("builtin original source selection is ambiguous"));
            }
            selected = Some((key.as_str(), relative));
        }
    }
    selected.ok_or_else(|| denied("builtin path is outside the original physical source"))
}
