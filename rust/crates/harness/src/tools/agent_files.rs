//! File bindings and bounded projections shared by child tools and filesystem views.
use crate::{
    Error, Result,
    runtime::ToolContext,
    task_volume::{
        ChildFile, ChildState, ChildStatus, Component, Control, FileKey, FileValue, TaskFiles,
    },
};
use acyclic_stream::StreamProvider;
use serde_json::{Value, json};
use std::sync::Arc;

pub(super) async fn files(context: &ToolContext) -> Result<Arc<TaskFiles<dyn StreamProvider>>> {
    let root = context
        .task()
        .harness
        .task_files
        .as_ref()
        .ok_or_else(|| Error::Unsupported("agent files are absent".into()))?;
    #[cfg(feature = "filesystem")]
    if let Some(child) = root
        .for_created_task(context.task(), context.effective_scope())
        .await?
    {
        return Ok(Arc::new(child));
    }
    #[cfg(not(feature = "filesystem"))]
    if context.task().scope().created_access.is_some() {
        return Err(Error::Unsupported(
            "created agent files require filesystem authority".into(),
        ));
    }
    Ok(root.clone())
}

/// Selects the registered-agent inspections `fs ls`/`fs cat` under
/// `.volumes/agents`. A command that does not parse is never an inspection:
/// the fs facade owner alone parses and reports it, as a model-visible
/// argument error, so this interception cannot turn it into a task failure.
/// Returns whether it is a `cat`, the query and the inspected path.
fn selection(command: &str) -> Result<Option<(bool, crate::task_volume::Query, String)>> {
    use crate::task_volume::Query;
    let Ok(command) = super::parse_fs_command(command) else {
        return Ok(None);
    };
    let [verb, path] = command.arguments.as_slice() else {
        return Ok(None);
    };
    if command.volume.is_some() || !path.starts_with(".volumes/agents") {
        return Ok(None);
    }
    let query = match (verb.as_str(), path.as_str()) {
        ("ls", ".volumes/agents") => Query::Children,
        ("ls", path) => Query::Child(
            Component::new(
                path.strip_prefix(".volumes/agents/")
                    .ok_or_else(|| Error::Invalid("invalid child directory".into()))?,
            )
            .map_err(storage)?,
        ),
        ("cat", path) => Query::File(
            FileKey::parse(
                path.strip_prefix(".volumes/")
                    .ok_or_else(|| Error::Invalid("invalid child file".into()))?,
            )
            .map_err(storage)?,
        ),
        _ => return Ok(None),
    };
    Ok(Some((verb == "cat", query, path.clone())))
}

pub(super) async fn inspect(
    context: &ToolContext,
    command: &str,
) -> Result<Option<crate::tool::ToolResult>> {
    use crate::task_volume::FileQuerySource;
    let Some((cat, query, path)) = selection(command)? else {
        return Ok(None);
    };
    let limits = context.effective_scope().limits();
    let files = files(context).await?;
    let stdout = if cat {
        let snapshot = files
            .observe_files(
                query,
                None,
                limits.attachments,
                usize::try_from(limits.render_bytes).map_err(storage)?,
            )
            .await
            .map_err(storage)?
            .snapshot
            .ok_or_else(|| Error::Conflict("child file snapshot missing".into()))?;
        let [file] = snapshot.files.as_slice() else {
            return Err(Error::NotFound(path));
        };
        std::str::from_utf8(&file.bytes)
            .map_err(storage)?
            .to_owned()
    } else {
        listing(&files.inner, &query, limits.attachments).await?
    };
    if stdout.len() as u64 > limits.render_bytes {
        return Err(Error::Invalid(
            "child inspection exceeds the original render bound".into(),
        ));
    }
    Ok(Some(crate::tool::ToolResult {
        value: json!({"success":true,"exit_code":0,"stdout":stdout,"stderr":""}),
    }))
}

async fn listing<P: StreamProvider + ?Sized>(
    files: &crate::task_volume::TaskVolume<P>,
    query: &crate::task_volume::Query,
    maximum_files: usize,
) -> Result<String> {
    Ok(files
        .list_files(query, maximum_files)
        .await
        .map_err(storage)?
        .iter()
        .map(|key| format!(".volumes/{}\n", key.path()))
        .collect())
}

#[cfg(all(test, not(acyclic_harness_loom)))]
mod listing_tests;

#[cfg(all(test, not(acyclic_harness_loom)))]
pub(super) fn status(
    reference: &Component,
    files: &[FileValue],
    maximum_bytes: usize,
) -> Result<Value> {
    let inputs = files
        .iter()
        .filter(|file| {
            matches!(
                &file.key,
                FileKey::Child {
                    file: ChildFile::Request | ChildFile::Inbox(_),
                    ..
                }
            )
        })
        .count() as u64;
    status_at(reference, files, maximum_bytes, inputs)
}

pub(super) fn status_at(
    reference: &Component,
    files: &[FileValue],
    maximum_bytes: usize,
    inputs: u64,
) -> Result<Value> {
    let mut state = ChildState::Running;
    let mut execution = None;
    for file in files {
        match &file.key {
            FileKey::Child {
                file: ChildFile::Control,
                ..
            } => {
                state = state.apply(Control::parse(&file.bytes).map_err(storage)?);
            }
            FileKey::Child {
                file: ChildFile::Status,
                ..
            } => {
                execution = Some(ChildStatus::parse(&file.bytes).map_err(storage)?);
            }
            _ => {}
        }
    }
    let latest = files
        .iter()
        .filter_map(|file| match &file.key {
            FileKey::Child {
                reference: candidate,
                file: ChildFile::Result(turn),
            } if candidate == reference => Some((*turn, file)),
            _ => None,
        })
        .max_by_key(|(turn, _)| *turn)
        .map(|(turn, file)| {
            let result =
                crate::task_volume::ChildTurnResult::parse(&file.bytes).map_err(storage)?;
            if result.turn != turn || execution.is_none_or(|status| status.task != result.task) {
                return Err(Error::Conflict(
                    "child result differs from its admitted status".into(),
                ));
            }
            Ok((
                turn,
                json!({"turn":turn,"file":{"path":format!(".volumes/{}", file.key.path())},
            "outcome":result.outcome}),
            ))
        })
        .transpose()?;
    let settled = execution.is_some_and(|execution| {
        execution.settled
            && execution.state == state
            && (state != ChildState::Running
                || latest
                    .as_ref()
                    .is_some_and(|(turn, _)| *turn == execution.inputs))
            && (matches!(
                state,
                ChildState::Cancelled | ChildState::Archived | ChildState::Interrupted
            ) || execution.inputs == inputs)
    });
    let mut value = json!({"reference":reference,"state":state,"settled":settled,
        "execution":execution,"result":latest.map(|(_, result)| result)});
    if serde_json::to_vec(&value).map_err(storage)?.len() > maximum_bytes {
        if let Some(result) = value.get_mut("result").and_then(Value::as_object_mut) {
            result.remove("outcome");
        }
        if serde_json::to_vec(&value).map_err(storage)?.len() > maximum_bytes {
            return Err(Error::Invalid(
                "child result references exceed the original render bound".into(),
            ));
        }
    }
    Ok(value)
}

fn storage(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests {
    use super::*;
    use crate::task_volume::Query;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        // Every command the fs facade cannot parse is left to the facade owner,
        // which reports it to the model; an exact agent inspection is selected.
        #[test]
        fn only_parsed_agent_inspections_are_intercepted(
            verb in prop::sample::select(vec!["ls", "cat", "git ls", "status"]),
            path in prop::sample::select(vec![
                ".volumes/agents", ".volumes/agents/worker", ".volumes/agents/worker/status.json",
            ]),
            suffix in prop::sample::select(vec!["", " | cat", "; ls", " && ls", " `x`", " < in", " > out",
                " 'open", " \\", " \0", " --volume", " --volume ''"]),
        ) {
            let command = format!("{verb} {path}{suffix}");
            let selected = selection(&command);
            if super::super::parse_fs_command(&command).is_err() {
                prop_assert!(matches!(selected, Ok(None)), "{command}: {selected:?}");
            }
        }
    }

    // Negative control: the parse error this interception used to propagate is
    // real, so the property above is not vacuous.
    #[test]
    fn unparseable_agent_listing_is_a_facade_error_not_an_inspection() {
        let command = "ls .volumes/agents | cat";
        assert!(super::super::parse_fs_command(command).is_err());
        assert!(matches!(selection(command), Ok(None)));
        assert!(matches!(selection("ls .volumes/agents"), Ok(Some((false, Query::Children, _)))));
        assert!(matches!(selection("cat .volumes/agents/worker/status.json"), Ok(Some((true, Query::File(_), _)))));
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        #[test]
        fn wait_keeps_a_bounded_latest_reference_and_only_inlines_a_fitting_outcome(
            turns in 1u64..16, text in ".{0,4096}", failed in any::<bool>(),
        ) {
            let reference = Component::new("worker").unwrap();
            let task = crate::TaskId::from_bytes([7;16]);
            let file = |kind, bytes| FileValue::new(FileKey::Child { reference: reference.clone(), file: kind }, bytes);
            let mut files = vec![file(ChildFile::Request, b"work".to_vec()),
                file(ChildFile::Status, serde_json::to_vec(&ChildStatus { task, state: ChildState::Running,
                    settled: true, inputs: turns }).unwrap())];
            for turn in 1..=turns {
                if turn > 1 { files.push(file(ChildFile::Inbox(Component::new(format!("mail-{turn}")).unwrap()), b"more".to_vec())); }
                let outcome = if failed { json!({"Failed":{"message":text}}) }
                    else { json!({"Succeeded":{"text":text,"metadata":null,"steps":1,"attachments":[]}}) };
                files.push(file(ChildFile::Result(turn), serde_json::to_vec(&json!({"task":task,"turn":turn,"outcome":outcome})).unwrap()));
            }
            let value = status(&reference, &files, 2048).unwrap();
            prop_assert_eq!(value["settled"].as_bool(), Some(true));
            prop_assert!(serde_json::to_vec(&value).unwrap().len() <= 2048);
            prop_assert_eq!(value["result"]["file"].as_object().unwrap().len(), 1);
            prop_assert!(value.get("results").is_none());
            prop_assert_eq!(value["result"]["turn"].as_u64(), Some(turns));
            let path = format!(".volumes/agents/worker/results/{turns}.json");
            prop_assert_eq!(value["result"]["file"]["path"].as_str(), Some(path.as_str()));
            files.retain(|file| file.key != FileKey::Child { reference: reference.clone(), file: ChildFile::Result(turns) });
            prop_assert_eq!(status(&reference, &files, 2048).unwrap()["settled"].as_bool(), Some(false));
        }
    }
}
