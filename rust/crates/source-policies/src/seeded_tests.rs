//! Harness library tests run in a seeded world, so their ids repeat exactly.
//!
//! A test on a host runtime draws fresh identities from host entropy, and a
//! failure that prints one differs in every run. Raw failure text then cannot
//! be compared between two runs or two commits. Every harness library test
//! enters through `acyclic_native_runtime::testing::seeded_test`, where ids,
//! `select!` order and time come from the seed.
use std::collections::BTreeMap;

const HARNESS_SOURCES: [&str; 4] = [
    "rust/crates/harness/src/",
    "rust/crates/harness-local/src/",
    "rust/crates/harness-hosting/src/",
    "rust/crates/harness-suite/src/",
];

/// The one named opt-out: a test that needs host entropy says why on the line
/// directly above its `#[tokio::test]`.
const HOST_ENTROPY: &str = "// host entropy:";

/// Unconverted tests, per file. This list only shrinks: converting a test
/// lowers its count, and the last one removes the file. An unseeded test in a
/// file that isn't listed, or beyond its count, fails the policy.
const NOT_YET_SEEDED: [(&str, usize); 64] = [
    ("rust/crates/harness-hosting/src/hosting_tests.rs", 3),
    (
        "rust/crates/harness-hosting/src/hosting_tests/editor_input.rs",
        1,
    ),
    (
        "rust/crates/harness-hosting/src/hosting_tests/machine_results.rs",
        1,
    ),
    ("rust/crates/harness-hosting/src/hosting_tests/merge.rs", 1),
    (
        "rust/crates/harness-hosting/src/hosting_tests/peer_delivery.rs",
        1,
    ),
    (
        "rust/crates/harness-hosting/src/hosting_tests/session_pages.rs",
        1,
    ),
    (
        "rust/crates/harness-local/src/local/agent_conversation/pending_tests.rs",
        1,
    ),
    (
        "rust/crates/harness-local/src/local/agent_conversation/run.rs",
        7,
    ),
    (
        "rust/crates/harness-local/src/local/agent_conversation/transcripts_tests.rs",
        1,
    ),
    ("rust/crates/harness-local/src/local/agent_settings.rs", 4),
    ("rust/crates/harness-local/src/local/agents.rs", 1),
    (
        "rust/crates/harness-local/src/local/checkout_tools/capture/tests.rs",
        20,
    ),
    (
        "rust/crates/harness-local/src/local/checkout_tools/git.rs",
        3,
    ),
    (
        "rust/crates/harness-local/src/local/checkout_tools/hooks.rs",
        1,
    ),
    (
        "rust/crates/harness-local/src/local/checkout_tools/publisher/mount_tests.rs",
        5,
    ),
    (
        "rust/crates/harness-local/src/local/checkout_tools/publisher/tests.rs",
        14,
    ),
    (
        "rust/crates/harness-local/src/local/checkout_tools/publisher/tests/admission_cost.rs",
        1,
    ),
    (
        "rust/crates/harness-local/src/local/checkout_tools/publisher/tests/granted.rs",
        1,
    ),
    (
        "rust/crates/harness-local/src/local/checkout_tools/publisher/tests/observed_cost.rs",
        9,
    ),
    (
        "rust/crates/harness-local/src/local/checkout_tools/publisher/tests/recovery.rs",
        8,
    ),
    (
        "rust/crates/harness-local/src/local/checkout_tools/shell/job/tests.rs",
        4,
    ),
    ("rust/crates/harness-local/src/local/config.rs", 6),
    ("rust/crates/harness-local/src/local/context/checkout.rs", 4),
    ("rust/crates/harness-local/src/local/context/skills.rs", 1),
    ("rust/crates/harness-local/src/local/endpoint_tests.rs", 6),
    (
        "rust/crates/harness-local/src/local/host_files/tests.rs",
        14,
    ),
    (
        "rust/crates/harness-local/src/local/host_owner/agents.rs",
        2,
    ),
    (
        "rust/crates/harness-local/src/local/host_owner/tests.rs",
        16,
    ),
    ("rust/crates/harness-local/src/local/host_process.rs", 1),
    (
        "rust/crates/harness-local/src/local/host_process/children/tests.rs",
        7,
    ),
    (
        "rust/crates/harness-local/src/local/host_process/children/tools.rs",
        1,
    ),
    (
        "rust/crates/harness-local/src/local/host_workspace/tests.rs",
        5,
    ),
    (
        "rust/crates/harness-local/src/local/owner/publication_tests.rs",
        1,
    ),
    ("rust/crates/harness-local/src/local/service.rs", 4),
    ("rust/crates/harness-local/src/local/service_exit.rs", 1),
    (
        "rust/crates/harness-local/src/local/service_ipc_tests.rs",
        2,
    ),
    (
        "rust/crates/harness-local/src/local/sessions/prompt_history/tests.rs",
        1,
    ),
    ("rust/crates/harness-local/src/local/tests.rs", 1),
    ("rust/crates/harness-local/src/local/tests/agents.rs", 24),
    (
        "rust/crates/harness-local/src/local/tests/agents/workspace_files.rs",
        2,
    ),
    ("rust/crates/harness-local/src/local/tests/context.rs", 1),
    (
        "rust/crates/harness-local/src/local/tests/context_documents.rs",
        2,
    ),
    (
        "rust/crates/harness-local/src/local/tests/history_retry.rs",
        1,
    ),
    ("rust/crates/harness-local/src/local/tests/platform.rs", 2),
    ("rust/crates/harness-local/src/local/tests/programs.rs", 3),
    ("rust/crates/harness-local/src/local/tests/questions.rs", 5),
    ("rust/crates/harness-local/src/local/tests/replay.rs", 2),
    ("rust/crates/harness-local/src/local/tests/settings.rs", 12),
    ("rust/crates/harness-local/src/local/tests/settlement.rs", 1),
    ("rust/crates/harness-local/src/local/tests/signing.rs", 2),
    (
        "rust/crates/harness-local/src/local/tests/transcripts.rs",
        1,
    ),
    ("rust/crates/harness-local/src/local/tool_output.rs", 1),
    ("rust/crates/harness-local/src/local/web/tests.rs", 6),
    (
        "rust/crates/harness-local/src/local/workspace/bootstrap/tests.rs",
        1,
    ),
    (
        "rust/crates/harness-local/src/local/workspace/git/tests.rs",
        2,
    ),
    (
        "rust/crates/harness-local/src/local/workspace/performance_tests.rs",
        3,
    ),
    (
        "rust/crates/harness-local/src/local/workspace/source_tests.rs",
        1,
    ),
    (
        "rust/crates/harness-local/src/local/workspace/task_views/concurrency_tests.rs",
        1,
    ),
    (
        "rust/crates/harness-local/src/local/workspace/task_views/tests.rs",
        4,
    ),
    (
        "rust/crates/harness-local/src/local/workspace/volumes/publication_tests.rs",
        9,
    ),
    (
        "rust/crates/harness-local/src/local/workspace/volumes/recovery_tests.rs",
        1,
    ),
    ("rust/crates/harness-local/src/server/tls_tests.rs", 3),
    (
        "rust/crates/harness-suite/src/conformance/observe/tests.rs",
        1,
    ),
    ("rust/crates/harness/src/bin/virtiofsd.rs", 1),
];

fn test_file(path: &str) -> bool {
    let mut parts = path.rsplit('/');
    let name = parts.next().unwrap_or_default();
    name == "tests.rs"
        || name.ends_with("_tests.rs")
        || name.starts_with("tests_")
        || parts.any(|part| part == "tests" || part.ends_with("_tests"))
}

/// Host runtimes and host identities: a `#[tokio::test]` without the opt-out,
/// a direct `Uuid::new_v4`, and a test's own Tokio runtime, anywhere in a test
/// file or from a production file's first `#[cfg(test)]` on.
fn unseeded_sites(path: &str, text: &str) -> usize {
    let mut previous = "";
    let mut count = 0;
    for line in text.lines() {
        let code = line.trim_start();
        if code.starts_with("#[tokio::test") && !previous.starts_with(HOST_ENTROPY) {
            count += 1;
        }
        if !code.is_empty() {
            previous = code;
        }
    }
    count += text.matches("Uuid::new_v4").count();
    let tests = if test_file(path) {
        text
    } else {
        text.split_once("#[cfg(test)]")
            .map_or("", |(_, tests)| tests)
    };
    count + tests.matches("runtime::Builder::new_").count()
}

fn seeded_test_violations<'a>(files: impl IntoIterator<Item = (&'a str, &'a str)>) -> Vec<String> {
    let mut found: BTreeMap<&str, usize> = files
        .into_iter()
        .map(|(path, text)| (path, unseeded_sites(path, text)))
        .filter(|(_, count)| *count > 0)
        .collect();
    let mut violations = Vec::new();
    for (path, count) in NOT_YET_SEEDED {
        match found.remove(path) {
            Some(actual) if actual == count => (),
            actual => violations.push(format!(
                "{path}: unseeded tests expected {count}, found {}; lower the count as tests convert",
                actual.unwrap_or(0)
            )),
        }
    }
    violations.extend(found.into_iter().map(|(path, count)| {
        format!("{path}: unseeded test x{count}; run it through seeded_test")
    }));
    violations
}

#[test]
fn harness_library_tests_cannot_draw_host_entropy() {
    let files: Vec<_> = crate::repository_rust_sources()
        .iter()
        .filter(|(path, _)| HARNESS_SOURCES.iter().any(|root| path.starts_with(*root)))
        .copied()
        .collect();
    let found = seeded_test_violations(files.iter().copied());
    assert!(found.is_empty(), "unseeded harness tests: {found:#?}");
    let seeded = "rust/crates/harness-local/src/local/tests/agents.rs";
    let original = files.iter().find(|(path, _)| *path == seeded).unwrap().1;
    let fresh = "rust/crates/harness-local/src/local/tests/fresh.rs";
    let scan = |planted: (&str, &str)| seeded_test_violations(planted_into(&files, planted));
    // Every unseeded spelling fails, in a listed file and in a new one.
    for plant in [
        "#[tokio::test]\nasync fn drawn() {}",
        "    #[tokio::test(flavor = \"multi_thread\")]\n    async fn drawn() {}",
        "let id = uuid::Uuid::new_v4();",
    ] {
        let text = format!("{original}\n{plant}");
        assert_eq!(scan((seeded, &text)).len(), 1, "{plant}");
        assert_eq!(scan((fresh, plant)).len(), 1, "new file: {plant}");
    }
    let runtime = "let runtime = tokio::runtime::Builder::new_current_thread().build()?;";
    assert_eq!(scan((fresh, runtime)).len(), 1);
    // Production builds its host runtime; only a test's own runtime counts,
    // including one in a production file's inline test module.
    assert!(scan(("rust/crates/harness-local/src/local/fresh.rs", runtime)).is_empty());
    let inline = format!(
        "fn main() {{}}
#[cfg(test)]
mod tests {{
    {runtime}
}}"
    );
    assert_eq!(
        scan(("rust/crates/harness-local/src/local/fresh.rs", &inline)).len(),
        1
    );
    // The seeded entry and the named opt-out stay legal.
    let entry = "#[test]\nfn drawn() -> Result<()> {\n    super::seeded(drawn_world)\n}";
    assert!(scan((fresh, entry)).is_empty());
    let opted =
        "// host entropy: probes the host's own generator\n#[tokio::test]\nasync fn drawn() {}";
    assert!(scan((fresh, opted)).is_empty());
    // Converting a listed test without lowering its count also fails.
    let (listed, count) = NOT_YET_SEEDED[0];
    assert_eq!(
        scan((listed, "")),
        [format!(
            "{listed}: unseeded tests expected {count}, found 0; lower the count as tests convert"
        )]
    );
}

/// The catalog with one file's text replaced, or added when it's new.
fn planted_into<'a>(
    files: &[(&'a str, &'a str)],
    planted: (&'a str, &'a str),
) -> Vec<(&'a str, &'a str)> {
    let mut files: Vec<_> = files
        .iter()
        .copied()
        .filter(|(path, _)| *path != planted.0)
        .collect();
    files.push(planted);
    files
}
