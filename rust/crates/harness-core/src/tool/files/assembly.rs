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
            Self::Read => return crate::tools::catalog::READ.bind_with_projection(mode),
            Self::Write => return crate::tools::catalog::WRITE.bind_with_projection(mode),
            Self::Edit => return crate::tools::catalog::EDIT.bind_with_projection(mode),
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
            hook_names: if matches!(self, Self::Patch { .. }) {
                &["Edit"]
            } else {
                &[]
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
