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

/// The kernel backstop and its read-only explanation share these exact bytes.
pub(crate) const CONTEXT_BACKSTOP: &str = include_str!("context-backstop.md");

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
    seed_file!("plugins/approvals/modes/ask.ts"),
    seed_file!("plugins/approvals/modes/model.ts"),
    seed_file!("plugins/approvals/modes/parent.ts"),
    seed_file!("plugins/approvals/plugin.ts"),
    seed_file!("plugins/compaction/hooks/after-usage.ts"),
    seed_file!("plugins/compaction/plugin.ts"),
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
    skill!("debugging"),
    skill!("fresh-context-review"),
    skill!("swarm-topology"),
    skill!("verify-before-done"),
    skill!("writing-skills"),
    ("sys/context-backstop.md", CONTEXT_BACKSTOP.as_bytes()),
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
    seed_file!("templates/advisor/AGENT.md"),
    seed_file!("templates/advisor/agent.ts"),
    seed_file!("templates/orchestrator/AGENT.md"),
    seed_file!("templates/orchestrator/agent.ts"),
    seed_file!("templates/reviewer/AGENT.md"),
    seed_file!("templates/reviewer/agent.ts"),
    seed_file!("templates/worker/AGENT.md"),
    seed_file!("templates/worker/agent.ts"),
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
        .map(|index| tree[index].1)
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
        format!("seed-{}", &name[..32]),
        VolumeClass::Project,
        VolumeOwner::Project("seed".into()),
        &mut None,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tree_is_sorted_and_kernel_lists_every_layer_rule() {
        assert!(tree().windows(2).all(|pair| pair[0].0 < pair[1].0));
        let kernel = std::str::from_utf8(file("sys/kernel.md").unwrap()).unwrap();
        for rule in crate::layers::Rule::ALL {
            assert!(kernel.contains(&format!("| {} |", rule.name())), "{rule:?}");
        }
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
            ) -> futures::future::BoxFuture<'a, crate::Result<Option<acyclic_stream::OwnedBytes>>>
            {
                Box::pin(async move { Ok(file(path).map(acyclic_stream::OwnedBytes::from_static)) })
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
        serde_json::from_value::<crate::config::AgentConfig>(evaluated.value).unwrap();
        Ok(())
    }
}
