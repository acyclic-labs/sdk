//! The shared git facade over the active checkout view.
use super::*;

impl CheckoutTools {
    pub(super) async fn fs(
        &self,
        view: &workspace::View,
        command: &str,
    ) -> Result<(ToolResult, Option<workspace::View>)> {
        let argv = crate::tools::parse_command(command)?;
        let args = match argv.split_first() {
            Some((prefix, args)) if prefix == "git" => args,
            _ => &argv,
        };
        let output = crate::tools::git::run(
            crate::tools::git::Workspace {
                filesystem: &self.workspace.filesystem,
                store: &self.workspace.store,
                view,
                root: &self.workspace.path,
                repository: self.workspace.view().workspace().id(),
                lease: None,
                checkout: Some(self.workspace.identity),
                author: "LocalHarness",
                authored_at_seconds: i64::try_from(self.clock.now_unix_millis() / 1000)
                    .unwrap_or(i64::MAX),
            },
            args,
        )
        .await?;
        let command = output.command.ok_or_else(|| {
            Error::Conflict("recovered a pending fs transition; retry the command".into())
        })?;
        Ok((
            ToolResult {
                value: serde_json::json!({"result":command}),
            },
            output.switched,
        ))
    }
}
