use super::*;
use proptest::prelude::*;

const FAMILY: &str = "---
version: 1
tools:
  - {capability: shell, variant: shell, revision: v1}
  - {capability: edit, variant: apply_patch, revision: v1}
reasoning: {mode: effort, effort: medium}
cache: automatic
watermark_tokens: 700
---
Model family instruction.
";
const MODEL: &str = "---\nversion: 1\nkey: test/model\n---\nModel instruction.\n";
const EMPTY: &str = "---\nversion: 1\n---\n";
const WORKER: &str = include_str!("roles/worker.md");
const ORCHESTRATOR: &str = include_str!("roles/orchestrator.md");
const REVIEWER: &str = include_str!("roles/reviewer.md");
const ADVISOR: &str = include_str!("roles/advisor.md");

#[test]
fn executable_bindings_retain_original_tools_and_refuse_stale_requests() -> Result<()> {
    use crate::{
        model::{Model, ModelRequest},
        tool::ToolRegistry,
    };
    let mut registry = ToolRegistry::default();
    for name in ["first", "second"] {
        let mut tool = crate::tool::files::read()?;
        tool.definition.name = name.into();
        tool.definition.revision = "v1".into();
        registry.register((tool, None), &mut None, None)?;
    }
    let family = "---\nversion: 1\ntools:\n  - {capability: read, variant: second, revision: v1}\n  - {capability: edit, variant: first, revision: v1}\nreasoning: {mode: disabled}\ncache: automatic\nwatermark_tokens: 700\n---\n";
    let model = profile(family, Some(200))?;
    let project = ProjectLayer::parse(EMPTY)?;
    let approvals = crate::approvals::ApprovalPolicy::default().bind()?;
    let agent = AgentOverride::parse(EMPTY, &approvals)?;
    let role = RoleFile::parse(
        "---\nversion: 1\nname: worker\nmodel: test/model\ncontext: fork\nmay_spawn: []\nallowed_tools: [read]\n---\n",
    )?;
    let resolved = resolve(&model, &role, &project, &agent)?;
    let bound = resolved.bind_tools(&registry, None)?;
    let declarations = bound.declarations();
    assert!(std::ptr::eq(
        declarations[0].tool(),
        registry.get("second", Some("v1"), None)?.unwrap()
    ));
    assert_eq!(declarations[1].tool().definition.name, "first");
    assert!(declarations[0].available());
    assert!(!declarations[1].available());
    let mut request = ModelRequest {
        model: Model {
            provider: "test".into(),
            name: "model".into(),
            revision: "v1".into(),
            options: serde_json::json!({}),
        },
        messages: Vec::new(),
        tools: declarations
            .iter()
            .map(|binding| binding.tool().definition.clone())
            .collect(),
        max_output_tokens: None,
    };
    bound.validate_request(&request)?;
    request.tools.swap(0, 1);
    assert!(bound.validate_request(&request).is_err());
    request.tools.swap(0, 1);
    request.tools[0].revision = "stale".into();
    assert!(bound.validate_request(&request).is_err());
    request.tools[0].revision = "v1".into();
    request.model.name = "another-model".into();
    assert!(bound.validate_request(&request).is_err());
    let missing = profile(
        &family.replace("revision: v1", "revision: missing"),
        Some(200),
    )?;
    let missing = resolve(&missing, &role, &project, &agent)?;
    assert!(missing.bind_tools(&registry, None).is_err());
    Ok(())
}

fn profile(family: &str, output_tokens: Option<u32>) -> Result<ModelProfile> {
    ModelProfile::parse(
        family,
        MODEL,
        ModelContextCapacity {
            context_tokens: 1_000,
            output_tokens,
        },
        "renderer-v1",
    )
}

#[test]
fn role_changes_append_policy_without_rewriting_the_model_prefix() -> Result<()> {
    let model = profile(FAMILY, Some(200))?;
    let project = ProjectLayer::parse(EMPTY)?;
    let approvals = crate::approvals::ApprovalPolicy::default().bind()?;
    let agent = AgentOverride::parse(EMPTY, &approvals)?;
    let worker = RoleFile::parse(WORKER)?;
    let orchestrator = RoleFile::parse(ORCHESTRATOR)?;
    let advisor = RoleFile::parse(ADVISOR)?;
    let reviewer = RoleFile::parse(REVIEWER)?;
    let base = resolve(&model, &worker, &project, &agent)?;
    let expected = serde_json::to_vec(&base.prefix).expect("serialize prefix");
    for role in [&orchestrator, &advisor, &reviewer] {
        let selected = resolve(&model, role, &project, &agent)?;
        assert_eq!(
            serde_json::to_vec(&selected.prefix).expect("serialize prefix"),
            expected
        );
        assert_eq!(selected.prefix_digest, base.prefix_digest);
        assert_ne!(selected.digest, base.digest, "overlay is still pinned");
        assert!(
            std::ptr::eq(selected.prefix.tools, base.prefix.tools),
            "borrow declarations"
        );
    }
    assert_eq!(
        base.prefix
            .tools
            .iter()
            .map(|tool| tool.capability.as_str())
            .collect::<Vec<_>>(),
        ["shell", "edit"]
    );
    assert_eq!(base.overlay.allowed_tools, None);
    assert_eq!(
        base.overlay.max_output_tokens, None,
        "retain capacity semantics"
    );
    let restricted = resolve(&model, &orchestrator, &project, &agent)?;
    assert!(
        !restricted
            .overlay
            .allowed_tools
            .expect("role restriction")
            .iter()
            .any(|tool| tool == "shell")
    );
    assert_eq!(orchestrator.admit_child(&worker)?, SpawnContext::Fork);
    assert_eq!(worker.admit_child(&reviewer)?, SpawnContext::Fresh);
    assert_eq!(worker.admit_child(&advisor)?, SpawnContext::Fresh);
    assert!(
        advisor.admit_child(&reviewer).is_err(),
        "role-file spawn policy"
    );
    assert!(
        worker.admit_child(&orchestrator).is_err(),
        "top level cannot be spawned"
    );

    let reversed = FAMILY.replace(
        "  - {capability: shell, variant: shell, revision: v1}\n  - {capability: edit, variant: apply_patch, revision: v1}",
        "  - {capability: edit, variant: apply_patch, revision: v1}\n  - {capability: shell, variant: shell, revision: v1}");
    let reordered_model = profile(&reversed, Some(200))?;
    let reordered = resolve(&reordered_model, &worker, &project, &agent)?;
    assert_ne!(
        reordered.prefix_digest, base.prefix_digest,
        "declaration order is semantic"
    );
    assert_ne!(reordered.digest, base.digest);
    let edited_role =
        RoleFile::parse(&WORKER.replace("Own the assigned work", "Check the assigned work"))?;
    let edited = resolve(&model, &edited_role, &project, &agent)?;
    assert_eq!(edited.prefix_digest, base.prefix_digest);
    assert_ne!(
        edited.digest, base.digest,
        "pin the edited directive at the next epoch"
    );
    let other_gate = crate::approvals::ApprovalPolicy::accept_all("owner").bind()?;
    let other_agent = AgentOverride::parse(EMPTY, &other_gate)?;
    let other_policy = resolve(&model, &worker, &project, &other_agent)?;
    assert_eq!(other_policy.prefix_digest, base.prefix_digest);
    assert_ne!(
        other_policy.digest, base.digest,
        "pin the actual owner's approval policy"
    );
    Ok(())
}

proptest! {
    #[test]
    fn profile_capacity_is_bounded_by_the_original_advertisement(context in 8u32..2_000_000, seed in any::<u32>()) {
        let budget = context / 2 + 2;
        let watermark = budget / 2;
        let output = 1 + seed % (budget / 4);
        let family = format!("---\nversion: 1\ntools: []\nreasoning: {{mode: adaptive, effort: medium}}\ncache: automatic\nwatermark_tokens: {watermark}\n---\n");
        let model = ModelProfile::parse(&family, MODEL,
            ModelContextCapacity { context_tokens: context, output_tokens: Some(output) }, "renderer-v1").unwrap();
        let project = ProjectLayer::parse(EMPTY).unwrap();
        let role = RoleFile::parse(WORKER).unwrap();
        let override_source = format!("---\nversion: 1\nbudget_tokens: {budget}\n---\n");
        let approvals = crate::approvals::ApprovalPolicy::default().bind().unwrap();
        let agent = AgentOverride::parse(&override_source, &approvals).unwrap();
        let selected = resolve(&model, &role, &project, &agent).unwrap();
        prop_assert_eq!(selected.prefix.capacity.budget_tokens, budget);
        prop_assert_eq!(selected.prefix.capacity.watermark_tokens, watermark);
        prop_assert_eq!(selected.overlay.max_output_tokens, None);
        let overflow = AgentOverride::parse(&format!("---\nversion: 1\nbudget_tokens: {}\n---\n", u64::from(context)+1), &approvals);
        prop_assert!(overflow.is_err() || resolve(&model, &role, &project, &overflow.unwrap()).is_err());
        let no_watermark = AgentOverride::parse("---\nversion: 1\nwatermark_tokens: 0\n---\n", &approvals).unwrap();
        prop_assert!(resolve(&model, &role, &project, &no_watermark).is_err());
        let oversized_role = RoleFile::parse(&WORKER.replacen("version: 1", &format!("version: 1\nmax_output_tokens: {}", output+1), 1)).unwrap();
        prop_assert!(resolve(&model, &oversized_role, &project, &agent).is_err());
    }
}

#[test]
fn unverified_output_and_untyped_override_paths_refuse() -> Result<()> {
    let role = RoleFile::parse(WORKER)?;
    let project = ProjectLayer::parse(EMPTY)?;
    let approvals = crate::approvals::ApprovalPolicy::default().bind()?;
    let agent = AgentOverride::parse(EMPTY, &approvals)?;
    let unknown = profile(FAMILY, None)?;
    let explicit_role =
        RoleFile::parse(&WORKER.replacen("version: 1", "version: 1\nmax_output_tokens: 100", 1))?;
    assert_eq!(
        resolve(&unknown, &explicit_role, &project, &agent)?
            .overlay
            .max_output_tokens,
        Some(100)
    );
    assert!(
        resolve(&unknown, &role, &project, &agent).is_err(),
        "no guessed output reserve"
    );
    let exhausted = profile(FAMILY, Some(500))?;
    assert!(
        resolve(&exhausted, &role, &project, &agent).is_err(),
        "prepare before the output reservation exhausts input capacity"
    );
    let exhausted_role =
        RoleFile::parse(&WORKER.replacen("version: 1", "version: 1\nmax_output_tokens: 400", 1))?;
    let larger = profile(FAMILY, Some(500))?;
    assert!(resolve(&larger, &exhausted_role, &project, &agent).is_err());
    assert!(
        AgentOverride::parse("---\nversion: 1\noptions: {tools: []}\n---\n", &approvals).is_err()
    );
    assert!(
        AgentOverride::parse(
            "---\nversion: 1\napproval_policy: {approvers: []}\n---\n",
            &approvals
        )
        .is_err(),
        "agent text cannot replace owner approval admission"
    );
    assert!(ProjectLayer::parse("---\nversion: 1\ntools: []\n---\n").is_err());
    assert!(RoleFile::parse(&WORKER.replace("version: 1", "version: 2")).is_err());
    assert!(
        profile(
            &FAMILY.replace("capability: edit", "capability: shell"),
            Some(200)
        )
        .is_err()
    );
    Ok(())
}
