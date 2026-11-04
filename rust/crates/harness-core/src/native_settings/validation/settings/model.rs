//! File admission through the public configured-model entrance.

use super::super::selection;
use super::{Files, SettingsConfig, write};

fn config(files: &Files, maximum: u64) -> SettingsConfig {
    SettingsConfig::new(&files.workspace, &files.home)
        .codex_home(&files.codex)
        .maximum_file_bytes(maximum)
}

fn replay(files: &Files, bytes: &[u8]) {
    write(&files.codex.join("config.toml"), bytes);
    match (
        config(files, u32::MAX.into()).configured_model(),
        selection::parse(bytes),
    ) {
        (Ok(actual), Ok(expected)) => {
            assert_eq!(actual, expected, "model file must preserve parser identity")
        }
        (Err(_), Err(_)) => {}
        _ => panic!("model file admission must refine the original parser"),
    }
}

pub(super) fn check(bytes: &[u8]) {
    let files = Files::new();
    // Invalid shadow roots must neither supply nor block the explicitly selected file.
    write(&files.home.join(".codex/config.toml"), b"model=42");
    write(&files.workspace.join(".codex/config.toml"), b"model=42");
    replay(&files, bytes);
    let middle = bytes.len() / 2;
    let model = String::from_utf8_lossy(&bytes[..middle]);
    let provider = String::from_utf8_lossy(&bytes[middle..]);
    let document = toml::to_string(&std::collections::BTreeMap::from([
        ("model", model.as_ref()),
        ("model_provider", provider.as_ref()),
    ]))
    .unwrap();
    replay(&files, document.as_bytes());
    if !model.trim().is_empty() && !provider.trim().is_empty() && !provider.contains('/') {
        assert_eq!(
            config(&files, u32::MAX.into()).configured_model().unwrap(),
            Some(format!("{provider}/{model}"))
        );
    }

    static ADMISSION: std::sync::Once = std::sync::Once::new();
    ADMISSION.call_once(|| {
        let files = Files::new();
        write(&files.home.join(".codex/config.toml"), b"model='shadow'");
        assert_eq!(
            config(&files, 512).configured_model().unwrap(),
            None,
            "absent selected model file must stay absent"
        );
        for input in [
            b"model='tiny'".as_slice(),
            b"model='v/m'",
            b"model='v/m'\nmodel_provider='chosen'",
        ] {
            replay(&files, input);
        }
        for maximum in [0, u64::MAX] {
            assert!(
                config(&files, maximum).configured_model().is_err(),
                "model reads require a finite positive byte bound"
            );
        }
        for length in [512, 513] {
            let mut input = b"model='tiny'\n".to_vec();
            input.resize(length, b' ');
            write(&files.codex.join("config.toml"), &input);
            let result = config(&files, 512).configured_model();
            if length == 512 {
                assert_eq!(
                    result.unwrap(),
                    Some("openai/tiny".into()),
                    "selected model root must retain its original value"
                );
            } else {
                assert!(
                    result.is_err(),
                    "valid over-bound model settings must be refused"
                );
            }
        }
    });
}
