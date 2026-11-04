//! Exercise the shipping native version and owned-hook admission directly.
#![no_main]

use acyclic_harness_core::{Error, Result, tools};
use libfuzzer_sys::fuzz_target;
use serde_json::{Value, json};

#[path = "../../harness/src/host_hooks.rs"]
pub mod host_hooks;

fuzz_target!(|bytes: &[u8]| {
    let Ok(text) = std::str::from_utf8(bytes) else {
        return;
    };
    // Native captures keep both acceptance and refusal reachable; accepting no
    // version must not satisfy an acceptance-only oracle.
    if text == include_str!("../../harness/fixtures/hosts/codex-0.164.0.stdout") {
        assert!(host_hooks::check_codex_version(text).is_ok());
    }
    if text == include_str!("../../harness/fixtures/hosts/codex-0.146.0.stdout") {
        assert!(host_hooks::check_codex_version(text).is_err());
    }
    if host_hooks::check_codex_version(text).is_ok() {
        let version = text.trim().strip_prefix("codex-cli ").unwrap();
        let mut fields = version.split('.');
        let major: u64 = fields.next().unwrap().parse().unwrap();
        let minor: u64 = fields.next().unwrap().parse().unwrap();
        let _: u64 = fields.next().unwrap().parse().unwrap();
        assert!(fields.next().is_none());
        assert!(major != 0 || minor >= 164);
    }
    if let Ok(group) = serde_json::from_str::<Value>(text) {
        for event in host_hooks::CLAUDE_EVENTS {
            if let Some(executable) = host_hooks::claude_hook_executable(event, &group) {
                let mut expected = host_hooks::claude_hooks(&executable)
                    .remove(event)
                    .unwrap()
                    .remove(0);
                // Ownership deliberately ignores an edited permission selector,
                // so uninstall can still retire the original installed group.
                if event == "PermissionRequest" {
                    assert!(group["matcher"].is_string());
                    expected["matcher"] = group["matcher"].clone();
                }
                assert_eq!(expected, group);
            }
        }
    }

    // These are our own generated configuration records, not substitute native
    // process output. Formatting and ownership recognition must compose for
    // every valid UTF-8 path, including quotes and native separators.
    if text.contains('\0') {
        return;
    }
    let executable = format!("{text}/acyclic");
    #[cfg(windows)]
    let executable = executable.replace('\\', "/");
    let executable = executable.as_ref();
    for (event, groups) in host_hooks::claude_hooks(executable) {
        for original in groups {
            assert!(original["hooks"][0].get("args").is_none());
            assert_eq!(
                tools::parse_command(original["hooks"][0]["command"].as_str().unwrap()).unwrap(),
                [executable, "__hook", "claude-code", event]
            );
            assert_eq!(
                host_hooks::claude_hook_executable(event, &original).as_deref(),
                Some(executable)
            );
            // Extra commands or fields cannot become owned configuration.
            let mut extra = original.clone();
            extra["unowned"] = json!(text);
            assert!(host_hooks::claude_hook_executable(event, &extra).is_none());
            let mut foreign = original.clone();
            foreign["hooks"][0]["permissionDecision"] = json!("allow");
            assert!(host_hooks::claude_hook_executable(event, &foreign).is_none());
            let mut additional = original;
            additional["hooks"]
                .as_array_mut()
                .unwrap()
                .push(json!({"command":text}));
            assert!(host_hooks::claude_hook_executable(event, &additional).is_none());
        }
    }
});
