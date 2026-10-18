use super::*;

impl<P: StreamProvider + ?Sized> CommandSession<P> {
    pub(crate) fn image_policy(&self) -> Result<crate::model::ImageInput> {
        let catalog = self
            .state(CommandFileRoot::Config, "image-input.json")?
            .ok_or_else(|| {
                Error::Unsupported("selected model has no image admission policy".into())
            })?;
        let default = catalog["default"]
            .as_str()
            .ok_or_else(|| Error::Invalid("image catalog has no default model".into()))?;
        let configured = self
            .state(CommandFileRoot::Task, "settings.json")?
            .and_then(|s| s["model"].as_str().map(str::to_owned))
            .or(self
                .state(CommandFileRoot::Config, "settings.json")?
                .and_then(|s| s["model"].as_str().map(str::to_owned)))
            .unwrap_or_else(|| default.into());
        let selection = if configured.contains('/') {
            configured
        } else {
            format!(
                "{}/{}",
                default.split_once('/').map_or(default, |(p, _)| p),
                configured
            )
        };
        let policy: crate::model::ImageInput = serde_json::from_value(
            catalog["models"]
                .get(&selection)
                .unwrap_or(&catalog["fallback"])
                .clone(),
        )
        .map_err(|_| Error::Unsupported("selected model has no image admission policy".into()))?;
        policy.validate()?;
        Ok(policy)
    }
}
