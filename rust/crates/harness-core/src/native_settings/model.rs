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
        let bytes = self.read_file(file)?;
        super::model_selection::parse(&bytes)
            .map_err(|error| Error::Invalid(format!("settings model: {error}")))
    }
}
