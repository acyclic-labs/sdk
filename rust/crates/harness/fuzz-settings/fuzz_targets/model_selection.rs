#![no_main]
#[path = "../../src/native_settings/model_selection.rs"]
mod model_selection;

libfuzzer_sys::fuzz_target!(|input: &[u8]| {
    if let Ok(Some(selection)) = model_selection::parse(input) {
        let document = std::collections::BTreeMap::from([("model", selection.as_str())]);
        let encoded = toml::to_string(&document).unwrap();
        assert_eq!(
            model_selection::parse(encoded.as_bytes()),
            Ok(Some(selection))
        );
    }
});
