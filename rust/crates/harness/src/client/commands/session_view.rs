use super::*;
impl<P: StreamProvider + ?Sized> CommandSession<P> {
    pub(in crate::client::commands) fn view_maximum_bytes(&self) -> u64 {
        self.maximum_bytes
    }
    pub(super) async fn view_input(&mut self, input: &str) -> Result<Option<OperationId>> {
        if input.len() as u64 > self.maximum_bytes {
            return Err(Error::Invalid("command input exceeds byte bound".into()));
        }
        if input.starts_with('/') {
            return Ok(None);
        }
        let mut layout = self
            .state(CommandFileRoot::Session, "layout.json")?
            .unwrap_or(json!({}));
        let Some(form) = layout["view_input"].as_str() else {
            return Ok(None);
        };
        let operation = OperationId::new();
        match form {
            "find" | "feedback" => {
                let name = if form == "find" {
                    "search.json"
                } else {
                    "feedback.json"
                };
                self.write_state(
                    CommandFileRoot::Session,
                    name,
                    &json!({"operation":operation,"text":input}),
                )
                .await?;
            }
            "theme" => {
                let CommandResolution::Submit {
                    request: CommandRequest::Theme { name },
                } = self
                    .catalog
                    .resolve(&format!("/theme {input}"), &NoReads, self.maximum_bytes)
                    .await?
                else {
                    return Err(Error::Invalid("select a configured theme name".into()));
                };
                self.write_state(CommandFileRoot::Config, "theme.json", &json!({"name":name}))
                    .await?;
                layout["open_view"] = Value::Null;
            }
            _ => return Err(Error::Invalid("unknown view input".into())),
        }
        layout["view_input"] = Value::Null;
        layout["view_request"] = json!(operation);
        self.write_state(CommandFileRoot::Session, "layout.json", &layout)
            .await?;
        Ok(Some(operation))
    }
}
struct NoReads;
impl ContentResidencyVerifier for NoReads {}
