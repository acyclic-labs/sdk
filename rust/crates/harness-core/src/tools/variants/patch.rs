//! The trained V4A operation uses the original tool journal and one conditional
//! filesystem publication, including multi-file changes and moves.
use super::*;
use crate::conversation::{ContentChange, FileRef};
use crate::tool::edit::{allocation, charge, reserve_string, reserve_vec};

#[derive(schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct PatchInput {
    patch: String,
}

#[derive(Serialize, Deserialize, schemars::JsonSchema, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
struct PatchResult {
    files: Vec<FileRef>,
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl crate::filesystem::UploadRecord for PatchResult {
    fn logical_owned_bytes(&self) -> Result<u64> {
        self.files.iter().try_fold(
            self.files.capacity() as u64 * std::mem::size_of::<FileRef>() as u64,
            |bytes, file| crate::core::owned_sum([bytes, file.logical_owned_bytes()?]),
        )
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Kind {
    Add,
    Update,
    Delete,
}

struct Action<'a> {
    kind: Kind,
    path: &'a str,
    destination: Option<&'a str>,
    body: &'a str,
}

fn line(raw: &str) -> &str {
    raw.strip_suffix('\n')
        .map_or(raw, |text| text.strip_suffix('\r').unwrap_or(text))
}

fn header(raw: &str) -> Option<(Kind, &str)> {
    let text = line(raw);
    [
        ("*** Add File: ", Kind::Add),
        ("*** Update File: ", Kind::Update),
        ("*** Delete File: ", Kind::Delete),
    ]
    .into_iter()
    .find_map(|(prefix, kind)| text.strip_prefix(prefix).map(|path| (kind, path)))
}

fn input(arguments: &Value) -> Result<&str> {
    let object = arguments
        .as_object()
        .filter(|object| object.len() == 1)
        .ok_or_else(|| Error::tool_rejected("apply_patch requires exactly patch"))?;
    object
        .get("patch")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::tool_rejected("apply_patch.patch must be text"))
}

fn parse<'a>(patch: &'a str, meter: &mut NativeCustody) -> Result<Vec<Action<'a>>> {
    charge(meter, 0, patch.len() as u64)?;
    let mut lines = patch.split_inclusive('\n').peekable();
    let first = lines
        .next()
        .ok_or_else(|| Error::tool_rejected("patch is empty"))?;
    if line(first) != "*** Begin Patch" {
        return Err(Error::tool_rejected(
            "patch must begin with *** Begin Patch",
        ));
    }
    let mut offset = first.len();
    let mut actions = Vec::new();
    while let Some(raw) = lines.next() {
        offset += raw.len();
        if line(raw) == "*** End Patch" {
            if lines.next().is_some() || actions.is_empty() {
                return Err(Error::tool_rejected(
                    "patch has trailing data or no changes",
                ));
            }
            return Ok(actions);
        }
        let (kind, path) = header(raw).ok_or_else(|| {
            Error::tool_rejected("patch needs an add, update or delete file header")
        })?;
        let destination = if kind == Kind::Update {
            if let Some(target) = lines
                .peek()
                .and_then(|raw| line(raw).strip_prefix("*** Move to: "))
            {
                offset += lines
                    .next()
                    .ok_or_else(|| Error::tool_rejected("missing move target"))?
                    .len();
                Some(target)
            } else {
                None
            }
        } else {
            None
        };
        let start = offset;
        while let Some(raw) = lines.peek() {
            if header(raw).is_some() || line(raw) == "*** End Patch" {
                break;
            }
            offset += lines
                .next()
                .ok_or_else(|| Error::tool_rejected("missing patch body"))?
                .len();
        }
        let body = patch
            .get(start..offset)
            .ok_or_else(|| Error::tool_rejected("patch body boundary is invalid"))?;
        if (kind == Kind::Delete && !body.is_empty()) || (kind == Kind::Update && body.is_empty()) {
            return Err(Error::tool_rejected(
                "delete must have no body and update must have a diff",
            ));
        }
        let count = actions.len() + 1;
        reserve_vec(meter, &mut actions, count)?;
        actions.push(Action {
            kind,
            path,
            destination,
            body,
        });
    }
    Err(Error::tool_rejected("patch must end with *** End Patch"))
}

fn added(body: &str, maximum: u64, meter: &mut NativeCustody) -> Result<String> {
    let mut output = String::new();
    for raw in body.split_inclusive('\n') {
        let text = raw
            .strip_prefix('+')
            .ok_or_else(|| Error::tool_rejected("added file lines must begin with +"))?;
        let size = output
            .len()
            .checked_add(text.len())
            .filter(|size| *size as u64 <= maximum)
            .ok_or_else(|| Error::tool_rejected("added file exceeds original file capacity"))?;
        reserve_string(meter, &mut output, size)?;
        charge(meter, text.len() as u64, 0)?;
        output.push_str(text);
    }
    Ok(output)
}

struct Plan<'a> {
    source: Option<FileRef>,
    output: Option<String>,
    target: &'a str,
    moving: bool,
    _source_custody: NativeCustody,
}

#[derive(Clone, Copy)]
struct ApplyPatch;

/// Declares the canonical client implementation. The model binding must expose
/// OpenAI's trained native declaration, preserve its raw custom input, and map
/// that input to this `patch` argument at the original dispatch boundary.
pub fn apply_patch() -> Result<Tool> {
    let definition = ToolDefinition {
        name: "apply_patch".into(), revision: "openai-v4a-1".into(),
        description: "Apply exact V4A add, update, delete and move operations in one atomic publication. Start with *** Begin Patch and finish with *** End Patch; update hunks use @@ and exact context".into(),
        input_schema: schema::input::<PatchInput>()?,
        output_schema: schema::output::<PatchResult>()?,
        projection_schema: schema::output::<Vec<String>>()?,
    };
    definition.validate(&mut None)?;
    Ok(Tool {
        definition,
        skill_source: None,
        hook_names: &["Write", "Edit"],
        executor: Arc::new(ApplyPatch),
        projection: Arc::new(ApplyPatch),
    })
}

impl ApplyPatch {
    async fn run(
        &self,
        context: &ToolContext,
        invocation: &ToolInvocation,
        work: NativeWork<'_>,
        recovering: bool,
    ) -> Result<(Option<ToolResult>, NativeCustody)> {
        self.authorize(
            Some(context.effective_scope()),
            Some(context),
            invocation,
            work,
        )?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let owned_io = work
            .map(crate::executor::PreparationIo::from_native_work)
            .transpose()?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let io = owned_io.as_ref();
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let io = None;
        let mut meter = allocation(work, 0)?;
        let actions = parse(input(&invocation.arguments)?, &mut meter)?;
        let (volume, _) = context.tool_path_selection(actions[0].path)?;
        if recovering {
            let observed = context
                .observe_tool_changes(invocation.operation_id, volume, io)
                .await?;
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            let _receipt = io
                .map(|io| io.take_native_custody(observed.1))
                .transpose()?;
            return match observed.0 {
                Some(files) => {
                    let encoded = crate::tool::value(&PatchResult { files }, u64::MAX, work)?;
                    Ok((Some(ToolResult { value: encoded.0 }), encoded.1))
                }
                None => Ok((None, allocation(work, 0)?)),
            };
        }
        let mut plans = Vec::new();
        reserve_vec(&mut meter, &mut plans, actions.len())?;
        for action in &actions {
            let (_, target) =
                context.tool_path_selection(action.destination.unwrap_or(action.path))?;
            let (source, output, custody) = if action.kind == Kind::Add {
                (
                    None,
                    Some(added(
                        action.body,
                        context.effective_scope().limits().file_bytes,
                        &mut meter,
                    )?),
                    None,
                )
            } else {
                let (file, bytes, retained) = context.read_tool_path(action.path, io).await?;
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                let custody = io.map(|io| io.take_native_custody(retained)).transpose()?;
                #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
                let custody = {
                    let _ = retained;
                    None
                };
                let output = if action.kind == Kind::Update {
                    let source = std::str::from_utf8(&bytes)
                        .map_err(|_| Error::tool_rejected("patch updates require UTF-8"))?;
                    Some(crate::tool::patch::apply_update_admitted(
                        source,
                        action.body,
                        context.effective_scope().limits().file_bytes,
                        &mut meter,
                    )?)
                } else {
                    None
                };
                (Some(file), output, custody)
            };
            plans.push(Plan {
                source,
                output,
                target,
                moving: action.destination.is_some(),
                _source_custody: custody,
            });
        }
        let mut changes = Vec::new();
        let count = plans
            .len()
            .checked_add(plans.iter().filter(|plan| plan.moving).count())
            .ok_or_else(|| Error::tool_rejected("patch operation count overflows"))?;
        reserve_vec(&mut meter, &mut changes, count)?;
        for plan in &plans {
            match (&plan.source, &plan.output) {
                (None, Some(text)) => changes.push(ContentChange::CreateAbsent {
                    path: plan.target,
                    bytes: text.as_bytes(),
                    media_type: "text/plain",
                    display_name: plan.target.rsplit('/').next().unwrap_or(plan.target),
                }),
                (Some(file), Some(text)) if plan.moving => {
                    changes.push(ContentChange::CreateAbsent {
                        path: plan.target,
                        bytes: text.as_bytes(),
                        media_type: file.descriptor().media_type(),
                        display_name: plan.target.rsplit('/').next().unwrap_or(plan.target),
                    });
                    changes.push(ContentChange::DeleteExpected { source: file });
                }
                (Some(file), Some(text)) => changes.push(ContentChange::UpdateExpected {
                    source: file,
                    bytes: text.as_bytes(),
                }),
                (Some(file), None) => changes.push(ContentChange::DeleteExpected { source: file }),
                (None, None) => return Err(Error::tool_rejected("patch member has no effect")),
            }
        }
        let (files, retained) = context
            .stage_tool_changes(invocation.operation_id, volume, &changes, io)
            .await?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let _receipt = io.map(|io| io.take_native_custody(retained)).transpose()?;
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let _ = retained;
        let encoded = crate::tool::value(&PatchResult { files }, u64::MAX, work)?;
        Ok((Some(ToolResult { value: encoded.0 }), encoded.1))
    }
}

impl ToolExecutor for ApplyPatch {
    fn authorize(
        &self,
        _scope: Option<&RuntimeScope>,
        context: Option<&ToolContext>,
        invocation: &ToolInvocation,
        work: NativeWork<'_>,
    ) -> Result<()> {
        let context = context
            .filter(|context| {
                context.operation_id() == invocation.operation_id
                    && context.call_id() == invocation.call_id
                    && invocation.name == "apply_patch"
            })
            .ok_or_else(|| {
                Error::tool_rejected("apply_patch requires its original admitted call context")
            })?;
        let mut meter = allocation(work, 0)?;
        let actions = parse(input(&invocation.arguments)?, &mut meter)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let owned_io = work
            .map(crate::executor::PreparationIo::from_native_work)
            .transpose()?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let io = owned_io.as_ref();
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let io = None;
        let (selected, _) = context.tool_path_selection(actions[0].path)?;
        for action in actions {
            for path in std::iter::once(action.path).chain(action.destination) {
                let (volume, relative) = context.tool_path_selection(path)?;
                if volume != selected {
                    return Err(Error::tool_rejected(
                        "one atomic patch must select one volume",
                    ));
                }
                crate::executor::with_history_io(io, |history| {
                    crate::conversation::validate_content_path(relative, history)
                })?;
                if relative.len() > context.effective_scope().limits().path_bytes
                    || crate::conversation::is_internal_path(relative)
                {
                    return Err(Error::tool_rejected(
                        "patch path exceeds its original public admission",
                    ));
                }
                context.authorize_tool_path_publication(path, None, io)?;
            }
            if action.kind != Kind::Add {
                context.authorize_tool_path_read(action.path, io)?;
            }
        }
        Ok(())
    }
    fn execute<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(async move {
            let context =
                context.ok_or_else(|| Error::tool_rejected("patch context is unavailable"))?;
            self.run(&context, &invocation, None, false)
                .await?
                .0
                .ok_or_else(|| Error::Storage("patch publication receipt is missing".into()))
        })
    }
    fn reconcile<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async move {
            let context =
                context.ok_or_else(|| Error::tool_rejected("patch context is unavailable"))?;
            self.run(&context, &invocation, None, true)
                .await
                .map(|result| result.0)
        })
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_execution(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        work.remaining()?;
        Ok(())
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn execute_with_native_context<'a>(
        &'a self,
        context: &'a ToolContext,
        invocation: &'a ToolInvocation,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (ToolResult, acyclic_fs::NativeAllocation)>> {
        crate::NativeFutureBundle::new(work, || async move {
            let (result, custody) = self.run(context, invocation, Some(work), false).await?;
            Ok((
                result.ok_or_else(|| Error::tool_rejected("patch receipt is missing"))?,
                custody
                    .ok_or_else(|| Error::tool_rejected("patch result lacks original custody"))?,
            ))
        })
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn reconcile_with_native_context<'a>(
        &'a self,
        context: &'a ToolContext,
        invocation: &'a ToolInvocation,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Option<ToolResult>, acyclic_fs::NativeAllocation)>>
    {
        crate::NativeFutureBundle::new(work, || async move {
            let (result, custody) = self.run(context, invocation, Some(work), true).await?;
            Ok((
                result,
                custody
                    .ok_or_else(|| Error::tool_rejected("patch recovery lacks original custody"))?,
            ))
        })
    }
}

impl ToolProjection for ApplyPatch {
    fn project(
        &self,
        invocation: &ToolInvocation,
        result: &ToolResult,
        work: NativeWork<'_>,
    ) -> Result<(Value, NativeCustody)> {
        let _result = crate::tool::decode::<PatchResult>(&result.value, work)?;
        let mut meter = allocation(work, 0)?;
        let actions = parse(input(&invocation.arguments)?, &mut meter)?;
        struct Paths<'a>(&'a [Action<'a>]);
        impl Serialize for Paths<'_> {
            fn serialize<S: serde::Serializer>(
                &self,
                serializer: S,
            ) -> std::result::Result<S::Ok, S::Error> {
                serializer.collect_seq(
                    self.0
                        .iter()
                        .flat_map(|action| std::iter::once(action.path).chain(action.destination)),
                )
            }
        }
        schema::project_json(&Paths(&actions), work)
    }
    fn supports_native_projection(&self, work: NativeWork<'_>) -> Result<()> {
        FileProjection::Publication(ProjectionMode::Full).supports_native_projection(work)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn patch_envelope_preserves_exact_bodies_and_refuses_partial_grammar() -> Result<()> {
        let mut meter = None;
        let actions = parse(
            "*** Begin Patch\n*** Add File: a\n+🦀\n*** Update File: b\n*** Move to: c\n@@\n-old\n+new\n*** Delete File: d\n*** End Patch\n",
            &mut meter,
        )?;
        assert_eq!(actions.len(), 3);
        assert_eq!(added(actions[0].body, 8, &mut meter)?, "🦀\n");
        assert_eq!(actions[1].destination, Some("c"));
        assert_eq!(actions[1].body, "@@\n-old\n+new\n");
        assert!(
            parse(
                "*** Begin Patch\n*** Delete File: d\n-oops\n*** End Patch\n",
                &mut meter
            )
            .is_err()
        );
        assert!(parse("*** Begin Patch\n*** Add File: a\n+new\n", &mut meter).is_err());
        assert!(
            parse(
                "*** Begin Patch\n*** Delete File: a\n*** End Patch\nextra\n",
                &mut meter
            )
            .is_err()
        );
        Ok(())
    }
}
