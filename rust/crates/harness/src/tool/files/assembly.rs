use super::super::text_files::{SearchInput, SearchResult};
use super::*;
use schema::ProjectionMode;

impl FileTool {
    pub(in crate::tool) fn assemble(self, mode: ProjectionMode) -> Result<Tool> {
        let (
            name,
            revision,
            description,
            mut input_schema,
            output_schema,
            projection_schema,
            projection,
        ) = match self {
            Self::Plan => (
                "update_plan",
                "plan-1",
                "Replace plan.md in the task volume with ordered pending, in_progress or completed steps; at most one step may be in_progress",
                schema::input::<super::super::plan::UpdatePlanInput>()?,
                schema::output::<FileResult>()?,
                mode.schema::<FileResult>()?,
                FileProjection::Publication(mode),
            ),
            Self::Read => (
                "read",
                "portable-6",
                "Read exact UTF-8 from an authorized immutable file, optionally selecting [start, end) byte offsets",
                schema::input::<ReadFileInput>()?,
                schema::output::<String>()?,
                mode.schema::<String>()?,
                FileProjection::Read(mode),
            ),
            Self::Write => (
                "write",
                "portable-5",
                "Write bounded UTF-8 through the original owner-bound content publisher",
                schema::input::<WriteFileInput>()?,
                schema::output::<FileResult>()?,
                mode.schema::<FileResult>()?,
                FileProjection::Publication(mode),
            ),
            Self::Edit => (
                "edit",
                "portable-5",
                "Replace one exact UTF-8 match at an authorized pinned source generation",
                schema::input::<EditFileInput>()?,
                schema::output::<FileResult>()?,
                mode.schema::<FileResult>()?,
                FileProjection::Publication(mode),
            ),
            Self::Patch { .. } => (
                "acyclic.patch_file",
                "portable-patch-4",
                "Apply exact V4A update-file diff hunks to one authorized immutable file; @@ headers, exact context and optional EOF; ambiguous context fails",
                schema::input::<PatchFileInput>()?,
                schema::output::<FileResult>()?,
                mode.schema::<FileResult>()?,
                FileProjection::Publication(mode),
            ),
            Self::Search { .. } => (
                "acyclic.search_file",
                "portable-text-1",
                "Search one authorized immutable UTF-8 source with finite byte comparisons and explicit omitted-match accounting",
                schema::input::<SearchInput>()?,
                schema::output::<SearchResult>()?,
                mode.schema::<SearchResult>()?,
                FileProjection::Search(mode),
            ),
        };
        self.configure_schema(&mut input_schema, mode)?;
        let definition = ToolDefinition {
            name: name.into(),
            revision: revision.into(),
            description: description.into(),
            input_schema,
            output_schema,
            projection_schema,
        };
        definition.validate(&mut None)?;
        Ok(Tool {
            hook_names: match self {
                Self::Read => &["Read"],
                Self::Write => &["Write"],
                Self::Edit | Self::Patch { .. } => &["Edit"],
                Self::Search { .. } | Self::Plan => &[],
            },
            definition,
            executor: Arc::new(self),
            projection: Arc::new(projection),
        })
    }

    fn configure_schema(&self, input_schema: &mut Value, mode: ProjectionMode) -> Result<()> {
        let configuration = match self {
            Self::Patch {
                maximum_work,
                maximum_hunks,
            } => Some((
                "x-harness-patch-limits",
                json!({"maximum_work":maximum_work,"maximum_hunks":maximum_hunks}),
            )),
            Self::Search {
                options,
                maximum_result_bytes,
            } => Some((
                "x-harness-text-configuration",
                json!({"search":options,"maximum_result_bytes":maximum_result_bytes,"projection":mode}),
            )),
            _ => None,
        };
        if let Some((key, configuration)) = configuration {
            input_schema
                .as_object_mut()
                .ok_or_else(|| {
                    Error::Invalid("generated file-tool schema is not an object".into())
                })?
                .insert(key.into(), configuration);
        }
        Ok(())
    }
}
