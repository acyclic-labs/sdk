//! Check canonical model selections with the production settings parser.
#![no_main]
#[path = "../../../harness-core/src/native_settings/model_selection.rs"]
mod model_selection;

libfuzzer_sys::fuzz_target!(|input: &[u8]| {
    if let Ok(Some(selection)) = model_selection::parse(input) {
        let document = std::collections::BTreeMap::from([("model", selection.as_str())]);
        assert!(toml::to_string(&document).is_ok_and(|encoded| {
            model_selection::parse(encoded.as_bytes()) == Ok(Some(selection))
        }));
    }
});
