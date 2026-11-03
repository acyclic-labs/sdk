//! Byte-input properties for the same argument admission used by the host.
#![no_main]

#[path = "../../crates/native-runtime/src/process_tree/external_host/arguments.rs"]
mod arguments;

use arguments::Invocation;
use std::ffi::OsString;

fn check(input: Vec<OsString>) -> bool {
    let parsed = match Invocation::parse(input) {
        Ok(parsed) => parsed,
        Err(error) => {
            assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
            return false;
        }
    };
    // Encode an admitted command through its literal boundary. This must
    // preserve every option and OS-string byte, including option-like args.
    let endpoints = [
        ("--control", &parsed.control),
        ("--custody", &parsed.custody),
        ("--lease", &parsed.lease),
        ("--completion", &parsed.completion),
    ];
    let mut canonical = Vec::new();
    for (option, value) in endpoints {
        if let Some(value) = value {
            assert!(value.is_absolute());
            canonical.extend([OsString::from(option), value.clone().into_os_string()]);
        }
    }
    assert!(parsed.custody.is_none() || parsed.lease.is_none());
    if let Some(binding) = &parsed.invocation {
        assert!(parsed.completion.is_some());
        assert!(!binding.is_empty());
        assert!(
            binding
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
        );
        canonical.extend(["--invocation".into(), binding.into()]);
    }
    if parsed.inherit_input {
        canonical.push("--inherit-input".into());
    }
    canonical.extend(["--".into(), parsed.executable.clone()]);
    canonical.extend(parsed.arguments.iter().cloned());
    assert_eq!(Invocation::parse(canonical.clone()).unwrap(), parsed);
    // Round trips alone cannot detect a discarded duplicate. Apply the
    // refusal relation to every singleton in any admitted arbitrary input,
    // including records whose metadata is not a trusted executable oracle.
    for (option, value) in endpoints {
        if let Some(value) = value {
            let mut duplicate = canonical.clone();
            duplicate.splice(0..0, [option.into(), value.clone().into_os_string()]);
            assert!(
                Invocation::parse(duplicate).is_err(),
                "duplicate endpoint admitted"
            );
        }
    }
    if let Some(binding) = &parsed.invocation {
        let mut duplicate = canonical.clone();
        duplicate.splice(0..0, ["--invocation".into(), binding.into()]);
        assert!(
            Invocation::parse(duplicate).is_err(),
            "duplicate invocation admitted"
        );
    }
    // A duplicate flag before the literal boundary must be refused, even
    // when the remaining bytes form an otherwise admitted invocation.
    canonical.splice(0..0, ["--inherit-input".into(), "--inherit-input".into()]);
    assert!(Invocation::parse(canonical).is_err());
    true
}

libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    // Reuse the original executable's kept argv/status corpus verbatim.
    let original = data
        == include_bytes!(
            "../../crates/native-runtime/src/process_tree/external_host/corpus/linux-arguments.json"
        )
        || cfg!(unix)
            && data
                == include_bytes!(
                    "../../crates/native-runtime/src/process_tree/external_host/corpus/linux-endpoints.json"
                );
    if let Ok(records) = serde_json::from_slice::<Vec<serde_json::Value>>(data) {
        for record in records {
            if let (Some(input), Some(code)) =
                (record["arguments"].as_array(), record["code"].as_u64())
            {
                if let Some(input) = input
                    .iter()
                    .map(|argument| argument.as_str().map(OsString::from))
                    .collect::<Option<Vec<_>>>()
                {
                    let admitted = check(input);
                    if original {
                        assert_eq!(
                            admitted,
                            code == 17,
                            "original executable admission changed"
                        );
                    }
                }
            }
        }
    }
    let input: Vec<_> = data
        .split(|byte| *byte == 0)
        .map(|bytes| {
            #[cfg(unix)]
            {
                use std::os::unix::ffi::OsStringExt as _;
                OsString::from_vec(bytes.to_vec())
            }
            #[cfg(windows)]
            {
                use std::os::windows::ffi::OsStringExt as _;
                let wide: Vec<_> = bytes
                    .chunks(2)
                    .map(|pair| u16::from_le_bytes([pair[0], *pair.get(1).unwrap_or(&0)]))
                    .collect();
                OsString::from_wide(&wide)
            }
        })
        .collect();
    check(input.clone());
    // The same arbitrary bytes must remain literal after the delimiter.
    let mut literal = vec!["--".into(), "node".into()];
    literal.extend(input);
    assert!(check(literal));
});
