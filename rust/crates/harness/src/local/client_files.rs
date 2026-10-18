use super::*;
use crate::client::{CommandFileRoot, CommandSession};

#[cfg(test)]
thread_local! {
    pub(super) static OMIT_IMAGE_POLICY: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

impl LocalHarness {
    pub(crate) async fn bind_client_files<P: acyclic_stream::StreamProvider + ?Sized>(
        &self,
        files: &mut CommandSession<P>,
    ) -> Result<()> {
        #[cfg(test)]
        if OMIT_IMAGE_POLICY.with(std::cell::Cell::get) {
            return Ok(());
        }
        // Unsupported models still accept bounded attachments for the provider's
        // text fallback; declaring admission does not claim native image support.
        let policy = self.provider.image_input(&self.model)?.unwrap_or_default();
        policy.validate()?;
        let name = format!("{}/{}", self.model.provider, self.model.name);
        let mut catalog = files
            .state(CommandFileRoot::Config, "image-input.json")?
            .unwrap_or_else(|| serde_json::json!({"models": {}}));
        let policy = serde_json::to_value(policy).map_err(|e| Error::Invalid(e.to_string()))?;
        if catalog["default"].as_str() != Some(name.as_str())
            || catalog["models"].get(&name) != Some(&policy)
        {
            let object = catalog
                .as_object_mut()
                .ok_or_else(|| Error::Invalid("image catalog must be an object".into()))?;
            let models = object
                .entry("models")
                .or_insert_with(|| serde_json::json!({}))
                .as_object_mut()
                .ok_or_else(|| Error::Invalid("image models must be an object".into()))?;
            models.insert(name.clone(), policy);
            object.insert("default".into(), serde_json::json!(name));
            files
                .write_state(CommandFileRoot::Config, "image-input.json", &catalog)
                .await?;
        }
        Ok(())
    }
}
