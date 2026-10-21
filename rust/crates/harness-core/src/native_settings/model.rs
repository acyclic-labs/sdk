use super::*;

impl SettingsConfig {
    /// Reads the user-configured model from this snapshot's explicit Codex root.
    pub fn configured_model(&self) -> Result<Option<String>> {
        let path = self.codex_home.join("config.toml");
        let file = match std::fs::File::open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(Error::process_io(error)),
        };
        if self.maximum_file_bytes == 0 || self.maximum_file_bytes == u64::MAX {
            return Err(Error::Invalid(
                "settings require a finite positive byte bound".into(),
            ));
        }
        let mut bytes = Vec::new();
        file.take(self.maximum_file_bytes + 1)
            .read_to_end(&mut bytes)
            .map_err(Error::process_io)?;
        if bytes.len() as u64 > self.maximum_file_bytes {
            return Err(Error::Invalid(
                "settings file exceeds its read bound".into(),
            ));
        }
        super::model_selection::parse(&bytes)
            .map_err(|error| Error::Invalid(format!("settings model: {error}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(16))]
        // Only the configured fixture root supplies defaults, and every accepted
        // setting resolves to the same provider/model identity after TOML replay.
        #[test]
        fn configured_models_use_the_fixture_root(model in "[a-z][a-z0-9-]{0,20}", provider in "[a-z]{1,12}") {
            let fixture = tempfile::tempdir().unwrap();
            let config = SettingsConfig::new(fixture.path(), fixture.path())
                .codex_home(fixture.path().join("codex"));
            std::fs::create_dir(&config.codex_home).unwrap();
            prop_assert_eq!(config.configured_model().unwrap(), None);
            for selected_provider in [None, Some(provider.as_str())] {
                let mut value = BTreeMap::from([("model", model.as_str())]);
                if let Some(provider) = selected_provider { value.insert("model_provider", provider); }
                std::fs::write(config.codex_home.join("config.toml"), toml::to_string(&value).unwrap()).unwrap();
                let provider = selected_provider.unwrap_or("openai");
                prop_assert_eq!(config.configured_model().unwrap(), Some(format!("{provider}/{model}")));
            }
        }
        #[test]
        fn model_selection_round_trips_quoted_settings(model in ".{0,64}", provider in ".{0,24}") {
            let document = BTreeMap::from([("model", model.as_str()), ("model_provider", provider.as_str())]);
            let encoded = toml::to_string(&document).unwrap();
            if let Ok(Some(selection)) = super::super::model_selection::parse(encoded.as_bytes()) {
                let canonical = toml::to_string(&BTreeMap::from([("model", selection.as_str())])).unwrap();
                prop_assert_eq!(super::super::model_selection::parse(canonical.as_bytes()).unwrap(), Some(selection));
            }
        }
    }
}
