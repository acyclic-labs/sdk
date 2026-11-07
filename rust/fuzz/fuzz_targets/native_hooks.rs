//! Fuzz the original native version and installed hook ownership decoders.
#![no_main]

use acyclic_harness::host_hooks::{
    CLAUDE_EVENTS, check_codex_version, claude_hook_executable, claude_hooks,
};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    assert!(
        check_codex_version(include_str!(
            "../../crates/harness/fixtures/hosts/codex-0.164.0.stdout"
        ))
        .is_ok(),
        "captured supported Codex version was refused"
    );
    assert!(
        check_codex_version(include_str!(
            "../../crates/harness/fixtures/hosts/codex-0.146.0.stdout"
        ))
        .is_err(),
        "captured obsolete Codex version was admitted"
    );
    for (event, groups) in claude_hooks("acyclic") {
        assert_eq!(
            claude_hook_executable(event, &groups[0]).as_deref(),
            Some("acyclic"),
            "native hook writer output was refused"
        );
        let mut foreign = groups[0].clone();
        foreign["hooks"][0]["statusMessage"] = serde_json::Value::Null;
        assert!(
            claude_hook_executable(event, &foreign).is_none(),
            "foreign hook metadata was admitted"
        );
    }
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };
    let _ = check_codex_version(text);
    if let Ok(group) = serde_json::from_str(text) {
        for event in CLAUDE_EVENTS {
            if let Some(executable) = claude_hook_executable(event, &group) {
                // An owned group must equal the writer's manifest. Permission
                // selectors remain editable strings under the public contract.
                let mut expected = claude_hooks(&executable)[event][0].clone();
                if event == "PermissionRequest"
                    && let Some(matcher) = group.get("matcher").and_then(serde_json::Value::as_str)
                {
                    expected["matcher"] = matcher.into();
                }
                assert_eq!(group, expected, "foreign hook group was admitted");
            }
        }
    }
});
