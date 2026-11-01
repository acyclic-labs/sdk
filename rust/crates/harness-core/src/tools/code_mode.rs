//! The stock caller owns composition; this tool carries only its stable contract.
use crate::{
    Result,
    runtime::ToolContext,
    tool::{ToolExecutor, ToolInvocation, ToolResult},
};
use acyclic_stream::BoxProviderFuture;

pub(crate) struct CodeMode;

impl crate::tool::ToolProjection for CodeMode {
    fn project(
        &self,
        _: &ToolInvocation,
        result: &ToolResult,
        work: crate::NativeWork<'_>,
    ) -> Result<(serde_json::Value, crate::NativeCustody)> {
        crate::tool::schema::project_json(output(&result.value)?, work)
    }

    fn supports_native_projection(&self, work: crate::NativeWork<'_>) -> Result<()> {
        #[cfg(feature = "filesystem")]
        if let Some(work) = work {
            work.remaining()?;
        }
        let _ = work;
        Ok(())
    }
}

pub(crate) fn output(value: &serde_json::Value) -> Result<&serde_json::Value> {
    if value.get("version") != Some(&serde_json::json!(1)) {
        return Err(crate::Error::Invalid(
            "unsupported JavaScript receipt version".into(),
        ));
    }
    value
        .get("output")
        .ok_or_else(|| crate::Error::Invalid("JavaScript receipt has no output".into()))
}

impl ToolExecutor for CodeMode {
    fn is_code_mode(&self) -> bool {
        true
    }

    fn execute<'a>(
        &'a self,
        _: ToolInvocation,
        _: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(async {
            Err(crate::Error::Unsupported(
                "run_js requires the stock tool caller".into(),
            ))
        })
    }

    fn reconcile<'a>(
        &'a self,
        _: ToolInvocation,
        _: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async { Ok(None) })
    }

    fn authorize(
        &self,
        _: Option<&crate::runtime::RuntimeScope>,
        _: Option<&ToolContext>,
        _: &ToolInvocation,
        work: crate::NativeWork<'_>,
    ) -> Result<()> {
        #[cfg(feature = "filesystem")]
        if let Some(work) = work {
            work.remaining()?;
        }
        let _ = work;
        Ok(())
    }

    #[cfg(feature = "filesystem")]
    fn supports_native_execution(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        work.remaining()?;
        Ok(())
    }
}
