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
