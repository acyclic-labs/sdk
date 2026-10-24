//! Inspection never opens or recovers a publication window.
use super::*;

pub(super) async fn run(
    tools: &CheckoutTools,
    workspace: &LocalWorkspace,
    view: crate::tools::git::View<'_>,
    source: Option<(
        &workspace::MountedVolume,
        Option<&workspace::projection::Native>,
    )>,
    reserved: &[String],
    arguments: &[String],
    context: &ToolContext,
) -> Result<ToolResult> {
    let owner = context
        .task()
        .owned_mailbox
        .as_ref()
        .ok_or_else(|| Error::Unauthorized("git command has no execution owner".into()))?;
    let selected = crate::tools::git::Workspace {
        filesystem: &workspace.filesystem,
        store: &workspace.store,
        view,
        root: source.map_or(workspace.path.as_path(), |(source, _)| {
            source.destination.as_path()
        }),
        repository: if source.is_some() {
            view.workspace().id()
        } else {
            workspace.view().workspace().id()
        },
        lease: None,
        checkout: None,
        reserved,
        author: "LocalHarness",
        authored_at_seconds: i64::try_from(tools.clock.now_unix_millis() / 1000)
            .unwrap_or(i64::MAX),
    };
    let volume_policy = acyclic_fs::CapturePolicy::excluding(Vec::new()).map_err(storage)?;
    let physical = physical_view(tools, workspace, source, &volume_policy)?;
    let verify = || async { owner.verify_owner(None)?.await };
    let result = if let Some(physical) = &physical {
        crate::tools::git::inspect_with_guard(
            selected,
            arguments,
            acyclic_fs::NativeGitView {
                options: &physical.options,
                policy: physical.policy,
                materialized: physical.materialized,
            },
            verify,
        )
        .await
    } else {
        crate::tools::git::run_with_guard(selected, arguments, verify).await
    };
    owner.verify_owner(None)?.await?;
    match result {
        Ok(output) => {
            let command = output
                .command
                .ok_or_else(|| Error::Conflict("read-only git produced no result".into()))?;
            Ok(ToolResult {
                value: serde_json::json!({"result": command}),
            })
        }
        Err(
            error @ (Error::Conflict(_)
            | Error::Invalid(_)
            | Error::NotFound(_)
            | Error::Unsupported(_)),
        ) => Ok(crate::tools::rejected_input(error)),
        Err(error) => Err(error),
    }
}

struct PhysicalInspection<'a> {
    options: acyclic_fs::CaptureOptions,
    policy: &'a acyclic_fs::CapturePolicy,
    materialized: bool,
}

fn physical_view<'a>(
    tools: &'a CheckoutTools,
    workspace: &LocalWorkspace,
    source: Option<(
        &workspace::MountedVolume,
        Option<&workspace::projection::Native>,
    )>,
    volume_policy: &'a acyclic_fs::CapturePolicy,
) -> Result<Option<PhysicalInspection<'a>>> {
    let (root, identity, policy, materialized) = match source {
        Some((source, Some(native))) => (
            &source.destination,
            native.native_root_identity(),
            volume_policy,
            true,
        ),
        Some((_, None)) => return Ok(None),
        None => (
            &workspace.path,
            match &workspace.mounted {
                Some(mount) => mount.native_root_identity(),
                None => Some(workspace.identity),
            },
            &tools.capture_policy,
            workspace.mounted.is_some(),
        ),
    };
    Ok(Some(PhysicalInspection {
        options: acyclic_fs::CaptureOptions {
            source_root: root.clone(),
            expected_root_identity: identity.ok_or_else(|| {
                Error::Conflict("mounted Git inspection has no native root witness".into())
            })?,
            maximum_paths: tools.capture_options.maximum_paths,
            maximum_extent_spans: tools.capture_options.maximum_extent_spans,
        },
        policy,
        materialized,
    }))
}
