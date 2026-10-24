#![no_main]
#[path = "../../../harness-core/src/native_settings/model_config.rs"]
mod model_config;
use model_config::{ModelConfiguration, ModelRoles};
#[path = "../../../harness-core/src/native_settings/model_selection.rs"]
mod model_selection;

libfuzzer_sys::fuzz_target!(|input: &[u8]| {
    if let Ok(config) = model_selection::parse_configuration(input, |_| false) {
        let mut document = serde_json::json!({
            "model":config.selection, "alloys": config.alloys,
            "roles":{"root":config.roles.root,"child":config.roles.child}
        });
        if config.selection.is_none() {
            document.as_object_mut().unwrap().remove("model");
        }
        let canonical = toml::Value::try_from(document).unwrap();
        let encoded = toml::to_string(&canonical).unwrap();
        let replay = model_selection::parse_configuration(encoded.as_bytes(), |name| {
            config.alloys.contains_key(name)
        })
        .unwrap();
        assert_eq!(replay.selection, config.selection);
        assert_eq!(replay.alloys, config.alloys);
        assert_eq!(replay.roles.root, config.roles.root);
        assert_eq!(replay.roles.child, config.roles.child);
    }
});
