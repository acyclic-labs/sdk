//! The seed layer: the read-only defaults under every agent's effective tree.
//!
//! The tree is built into the binary. A store holds it as one immutable
//! volume per build, named by [`digest`], so every agent on a store shares
//! the same generation and nothing is copied per agent.
use crate::{
    Result,
    conversation::{VolumeClass, VolumeOwner, VolumeRef},
    resources::ProviderRef,
};

/// The common agent instruction, shared by every host: the seed `AGENT.md`.
pub(crate) const AGENT_PROMPT: &str = include_str!("AGENT.md");

/// The context manager's file-based rewrite contract.
pub(crate) const CONTEXT_MANAGER: &str = include_str!("context-manager.md");

/// Exact lab-trained tool declarations, by family: `sys/native/<family>/tools.json`.
pub(crate) const ANTHROPIC_TOOLS: &str = include_str!("../tools/native/anthropic.json");
pub(crate) const OPENAI_TOOLS: &str = include_str!("../tools/native/openai.json");
pub(crate) const GOOGLE_TOOLS: &str = include_str!("../tools/native/google.json");

/// Versioned family layers pinning those declarations: `sys/native/<family>/profile.md`.
pub(crate) const ANTHROPIC_PROFILE: &str = include_str!("../profile/native/anthropic.md");
pub(crate) const OPENAI_PROFILE: &str = include_str!("../profile/native/openai.md");
pub(crate) const GOOGLE_PROFILE: &str = include_str!("../profile/native/google.md");

/// Where the seed's skills live in its tree.
pub const SKILLS: &str = "skills";

macro_rules! seed_file {
    ($path:expr) => {
        seed_file!($path, $path)
    };
    ($path:expr, $source:expr) => {
        ($path, include_bytes!($source))
    };
}

macro_rules! skill {
    ($name:literal) => {
        seed_file!(
            concat!("skills/", $name, "/SKILL.md"),
            concat!("../skills/", $name, "/SKILL.md")
        )
    };
}

/// Every embedded seed file by its path in the tree, sorted by path.
const FILES: &[(&str, &[u8])] = &[
    ("AGENT.md", AGENT_PROMPT.as_bytes()),
    seed_file!("agent.ts"),
    seed_file!("plugins.ts"),
    seed_file!("plugins/approvals/hooks/before-tool.ts"),
    seed_file!("plugins/approvals/modes/ask.ts"),
    seed_file!("plugins/approvals/modes/model.ts"),
    seed_file!("plugins/approvals/modes/parent.ts"),
    seed_file!("plugins/approvals/plugin.ts"),
    seed_file!("plugins/feedback/hooks/after-settlement.ts"),
    seed_file!("plugins/feedback/plugin.ts"),
    seed_file!("plugins/question/plugin.ts"),
    seed_file!("plugins/question/policy/attention.md"),
    seed_file!("plugins/question/skills/ui/SKILL.md"),
    seed_file!("plugins/spawn/plugin.ts"),
    seed_file!("plugins/spawn/spawn.ts"),
    seed_file!("plugins/spawn/topologies/blackboard.md"),
    seed_file!("plugins/spawn/topologies/debate-ring.md"),
    seed_file!("plugins/spawn/topologies/hierarchy.md"),
    seed_file!("plugins/spawn/topologies/market.md"),
    seed_file!("plugins/spawn/topologies/mesh.md"),
    seed_file!("plugins/spawn/topologies/pipeline.md"),
    seed_file!("plugins/spawn/topologies/search-derived.md"),
    seed_file!("plugins/spawn/topologies/service.md"),
    skill!("advisor-consultation"),
    skill!("candidate-search"),
    skill!("context-checkpoint"),
    skill!("context"),
    skill!("debugging"),
    skill!("fresh-context-review"),
    skill!("learn-from-evals"),
    skill!("memory"),
    skill!("plans"),
    skill!("search-history"),
    skill!("swarm-topology"),
    skill!("ui"),
    skill!("verify-before-done"),
    skill!("writing-skills"),
    ("sys/context-manager.md", CONTEXT_MANAGER.as_bytes()),
    seed_file!("sys/kernel.md", "kernel.md"),
    (
        "sys/native/anthropic/profile.md",
        ANTHROPIC_PROFILE.as_bytes(),
    ),
    (
        "sys/native/anthropic/tools.json",
        ANTHROPIC_TOOLS.as_bytes(),
    ),
    ("sys/native/google/profile.md", GOOGLE_PROFILE.as_bytes()),
    ("sys/native/google/tools.json", GOOGLE_TOOLS.as_bytes()),
    ("sys/native/openai/profile.md", OPENAI_PROFILE.as_bytes()),
    ("sys/native/openai/tools.json", OPENAI_TOOLS.as_bytes()),
    seed_file!("sys/types/index.d.ts", "types/index.d.ts"),
    seed_file!("templates/advisor/AGENT.md"),
    seed_file!("templates/blackboard/AGENT.md"),
    seed_file!("templates/debate-ring/AGENT.md"),
    seed_file!("templates/group-chat/AGENT.md"),
    seed_file!("templates/hierarchy/AGENT.md"),
    seed_file!("templates/orchestrator/AGENT.md"),
    seed_file!("templates/pipeline/AGENT.md"),
    seed_file!("templates/reviewer/AGENT.md"),
    seed_file!("templates/single/AGENT.md"),
    seed_file!("templates/worker/AGENT.md"),
];

/// Every seed file by its path in the tree, sorted by path: the embedded
/// files and the settings declarations generated from the kernel's types.
#[must_use]
pub fn tree() -> &'static [(&'static str, &'static [u8])] {
    static TREE: std::sync::OnceLock<Vec<(&str, &[u8])>> = std::sync::OnceLock::new();
    TREE.get_or_init(|| {
        let mut tree = FILES.to_vec();
        tree.push((
            crate::config::TYPES_PATH,
            crate::config::typescript().as_bytes(),
        ));
        tree.sort_unstable_by_key(|(path, _)| *path);
        tree
    })
}

/// The seed's copy of `path`, read without I/O: the seed volume holds
/// exactly these bytes.
#[must_use]
pub fn file(path: &str) -> Option<&'static [u8]> {
    let tree = tree();
    tree.binary_search_by_key(&path, |(name, _)| *name)
        .ok()
        .and_then(|index| tree.get(index).map(|(_, bytes)| *bytes))
}

/// This build's seed generation: a hash of every path and its bytes.
#[must_use]
pub fn digest() -> &'static [u8; 32] {
    static DIGEST: std::sync::OnceLock<[u8; 32]> = std::sync::OnceLock::new();
    DIGEST.get_or_init(|| {
        let mut hasher = blake3::Hasher::new_derive_key("acyclic.harness.seed.v1");
        for (path, bytes) in tree() {
            hasher.update(&(path.len() as u64).to_be_bytes());
            hasher.update(path.as_bytes());
            hasher.update(&(bytes.len() as u64).to_be_bytes());
            hasher.update(bytes);
        }
        hasher.finalize().into()
    })
}

/// The seed volume of this build on `provider`. Agents hold only read
/// grants on it; the host writes it once, before any agent reads it.
pub fn volume(provider: &ProviderRef) -> Result<VolumeRef> {
    let name = blake3::Hash::from_bytes(*digest()).to_hex();
    VolumeRef::new(
        provider.clone(),
        format!(
            "seed-{}",
            name.get(..32).ok_or_else(|| crate::Error::Invalid(
                "seed digest is shorter than its name".into()
            ))?
        ),
        VolumeClass::Project,
        VolumeOwner::Project("seed".into()),
        &mut None,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn tree_is_sorted_and_kernel_lists_every_layer_rule() {
        assert!(tree().windows(2).all(|pair| pair[0].0 < pair[1].0));
        let kernel = std::str::from_utf8(file("sys/kernel.md").unwrap()).unwrap();
        for rule in crate::layers::Rule::ALL {
            assert!(kernel.contains(&format!("| {} |", rule.name())), "{rule:?}");
        }
    }

    /// Command-line tools an instruction might tell a model to run, spelled
    /// distinctly enough that a whole word in prose names the tool.
    const COMMAND_TOOLS: &[&str] = &[
        "ack", "ag", "awk", "bash", "bun", "cargo", "clang", "curl", "deno", "docker", "egrep",
        "fd", "fzf", "gcc", "gh", "git", "grep", "jq", "kubectl", "node", "npm", "npx", "perl",
        "pip", "pnpm", "pwsh", "python", "python3", "rg", "ruby", "rustc", "sed", "tsc", "wget",
        "yarn", "yq", "zsh",
    ];

    /// The executables an instruction text tells the model to run, or an
    /// error naming the first known tool it names outside the command form.
    ///
    /// An instruction names a command only as a code span `` `$ <tool> ...` ``,
    /// whose first word is the executable, whatever that name is, or as a
    /// tool call: a tool's name span, then a `{"command":"..."}` span. A
    /// `shell` call's first word is an executable; any other tool's command
    /// is that tool's own subcommand, never an executable. A known tool named
    /// anywhere else is refused, so a command cannot reach the model in a
    /// form this check does not read.
    fn commands(text: &str) -> std::result::Result<BTreeSet<&str>, String> {
        fn executable(line: &str) -> &str {
            line.split_whitespace().next().unwrap_or_default()
        }
        // Only the command matters; other call options are no part of it.
        #[derive(serde::Deserialize)]
        struct Call<'a> {
            command: &'a str,
        }
        let mut commands = BTreeSet::new();
        let mut prose = String::new();
        // The tool a following call span invokes: its name span, joined to
        // the call by nothing but "with".
        let mut called = None;
        for (index, part) in text.split('`').enumerate() {
            let tool = called.take();
            if index % 2 == 1 {
                if let Some(line) = part.strip_prefix("$ ") {
                    commands.insert(executable(line));
                    continue;
                }
                // A call whose command needs JSON escapes stays prose.
                let call = serde_json::from_str::<Call>(part)
                    .ok()
                    .map(|call| call.command);
                match (call, tool) {
                    (Some(line), Some("shell")) => {
                        commands.insert(executable(line));
                        continue;
                    }
                    (Some(_), Some(_)) => continue,
                    _ => {}
                }
                if !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_lowercase()) {
                    called = Some(part);
                }
            } else if part.trim() == "with" {
                called = tool;
            }
            prose.push_str(part);
            prose.push(' ');
        }
        // A word keeps its path characters, so `.git` or `src/rg` names a
        // path, not a tool; only sentence punctuation is trimmed.
        match prose
            .split(|c: char| !(c.is_ascii_alphanumeric() || "_-./".contains(c)))
            .map(|word| word.trim_end_matches('.'))
            .find(|word| COMMAND_TOOLS.iter().any(|tool| tool == word))
        {
            Some(tool) => Err(format!("`{tool}` named outside a `$ ` command span")),
            None => Ok(commands),
        }
    }

    /// The executables `text` tells the model to run that no task
    /// environment guarantees.
    fn unprovided(text: &str) -> std::result::Result<Vec<&str>, String> {
        let named = commands(text)?;
        Ok(named
            .into_iter()
            .filter(|tool| {
                !crate::context::TASK_TOOLS
                    .iter()
                    .any(|provided| provided == tool)
            })
            .collect())
    }

    // A seed instruction tells the model to run only executables every task
    // environment provides, and names commands in no form that escapes this.
    #[test]
    fn seed_instructions_run_only_task_tools() {
        for (path, bytes) in tree().iter().filter(|(path, _)| path.ends_with(".md")) {
            let text = std::str::from_utf8(bytes).unwrap_or_else(|_| panic!("{path}"));
            assert_eq!(unprovided(text), Ok(vec![]), "{path}");
        }
        // Planted controls: any executable outside the guarantee is
        // refused, a known tool in prose is refused, and prose that only
        // shares words with commands passes.
        assert_eq!(unprovided("Run `$ make test` first."), Ok(vec!["make"]));
        assert_eq!(
            unprovided("Search with `$ rg -n x`, then `$ jq .`."),
            Ok(vec!["jq"])
        );
        assert!(unprovided("Read relevant code with jq before editing.").is_err());
        assert!(unprovided("Then use git.").is_err());
        // A facade's own subcommand is no executable; a shell call's is, and
        // a call span no tool names is prose.
        assert_eq!(
            unprovided(r#"Search with `fs` with `{"command":"grep 'x'"}`."#),
            Ok(vec![])
        );
        assert_eq!(
            unprovided(r#"Run `shell` with `{"command":"make test"}`."#),
            Ok(vec!["make"])
        );
        // A call's other options, numeric or boolean, never hide its command.
        assert_eq!(
            unprovided(r#"Run `shell` with `{"command":"make test","timeout_ms":1000}`."#),
            Ok(vec!["make"])
        );
        assert_eq!(
            unprovided(r#"Run `shell` with `{"command":"make test","tty":true}`."#),
            Ok(vec!["make"])
        );
        assert_eq!(
            unprovided(r#"Search with `fs` with `{"command":"grep x","timeout_ms":1000}`."#),
            Ok(vec![])
        );
        assert!(unprovided(r#"Run `{"command":"grep x"}`."#).is_err());
        assert!(unprovided(r#"Run `fs`, then `{"command":"grep x"}`."#).is_err());
        assert_eq!(
            unprovided("Files under `.git` are project files."),
            Ok(vec![])
        );
        assert_eq!(
            unprovided("Make a tree of the tail and find the path."),
            Ok(vec![])
        );
    }

    #[cfg(not(target_arch = "wasm32"))]
    // The shipped default is evaluated by the production evaluator: it
    // passes the kernel's base through unchanged and decodes.
    #[tokio::test]
    async fn seed_agent_settings_evaluate_to_the_base() -> crate::Result<()> {
        struct Seed;
        impl crate::config::Tree for Seed {
            fn read<'a>(
                &'a self,
                path: &'a str,
            ) -> futures::future::BoxFuture<'a, crate::Result<Option<crate::conversation::Custody>>>
            {
                Box::pin(async move {
                    Ok(file(path).map(|bytes| {
                        crate::conversation::Custody::Unmetered(
                            acyclic_stream::OwnedBytes::from_static(bytes),
                        )
                    }))
                })
            }
        }
        let base = serde_json::json!({"model": "p/m"});
        let source = file(crate::config::AGENT).unwrap();
        let evaluated = crate::config::evaluate(
            crate::config::AGENT,
            source,
            &Seed,
            &base,
            crate::config::Surface::Compose,
        )
        .await?;
        assert_eq!(evaluated.value, base);
        serde_json::from_value::<crate::config::AgentConfig>(evaluated.value.into_unbounded()?)
            .unwrap();
        Ok(())
    }
}
