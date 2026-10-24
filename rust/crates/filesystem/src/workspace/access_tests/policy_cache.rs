use super::*;
use crate::access::{
    Capability, CompiledPolicy, Context, Generations, Grant, PathTrie, Policy, Principal,
    PrincipalKind, Resource, ResourcePattern,
};

fn caller(id: String) -> Principal {
    Principal {
        kind: PrincipalKind::Agent,
        id,
        lineage: vec![],
        groups: Default::default(),
        roles: Default::default(),
        tags: Default::default(),
        attributes: Default::default(),
    }
}

fn ceiling() -> Capability {
    Capability {
        layers: vec![vec![Grant {
            actions: Action::ALL.into_iter().collect(),
            resource: ResourcePattern {
                volume: None,
                lineage_root: None,
                generations: Generations::All,
                path: "**".into(),
            },
        }]],
        expires_at: None,
    }
}

fn binding(
    policy: &Arc<CompiledPolicy>,
    id: String,
    maximum: usize,
) -> Result<(PathTrie, usize), Box<dyn std::error::Error>> {
    let mut remaining = maximum;
    let principal = caller(id);
    let view = policy.bind_with_work(
        &principal,
        Resource {
            config: crate::VolumeConfig::portable(crate::Lifecycle::Ephemeral),
            lineage: &[],
            volume: "project",
            generation: 1,
            head: 1,
            path: "",
        },
        Context {
            now: 10,
            approvals: 0,
        },
        &ceiling(),
        &mut remaining,
        |_| Ok(()),
    )?;
    Ok((view, maximum - remaining))
}

#[test]
fn one_hundred_thousand_principals_share_paths_and_have_constant_binding_work()
-> Result<(), Box<dyn std::error::Error>> {
    let policy: Policy = serde_json::from_slice(INITIAL_POLICY)?;
    let config = crate::VolumeConfig::portable(crate::Lifecycle::Ephemeral);
    let mut compile_work = usize::MAX;
    let small = Arc::new(CompiledPolicy::compile_with_work(
        &policy,
        config,
        &mut compile_work,
        |_| Ok(()),
    )?);
    let mut large = policy;
    let layer = large.layers.first_mut().ok_or("missing policy layer")?;
    let rule = layer.first().ok_or("missing policy rule")?.clone();
    for index in 1..128 {
        let mut rule = rule.clone();
        rule.id = format!("rule-{index}");
        layer.push(rule);
    }
    let large = Arc::new(CompiledPolicy::compile_with_work(
        &large,
        config,
        &mut compile_work,
        |_| Ok(()),
    )?);
    let bytes = large.logical_owned_bytes();
    let (small_view, expected) = binding(&small, "agent".into(), 256)?;
    for index in 0..100_000 {
        let (view, work) = binding(&large, format!("agent-{index}"), 256)?;
        assert_eq!(work, expected);
        assert!(view.decide(Action::Write, "dir/file", 10).allowed);
        assert!(view.logical_owned_bytes() < bytes);
    }
    assert_eq!(Arc::strong_count(&large), 1);
    assert_eq!(large.logical_owned_bytes(), bytes);
    assert_eq!(Arc::strong_count(&small), 2);
    drop(small_view);
    Ok(())
}

async fn check_revision_reuse(keep_stale_revision: bool) -> Result<(), Box<dyn std::error::Error>> {
    let owner = workspace().await?;
    let cancel = CancellationToken::new();
    let mut first = owner
        .checkout(
            GenerationSelector::Head,
            CheckoutMode::read_only_pinned(),
            WorkBudget::UNBOUNDED,
            &cancel,
        )
        .await?
        .value;
    let first = first
        .compiled_access_policy(WorkBudget::UNBOUNDED, &cancel)
        .await?;
    let mut maximum_hit = 0;
    let mut head_keyed = std::collections::HashMap::new();
    for index in 0..8 {
        owner.write_text("/file", &index.to_string()).await?;
        let mut checkout = owner
            .checkout(
                GenerationSelector::Head,
                CheckoutMode::read_only_pinned(),
                WorkBudget::UNBOUNDED,
                &cancel,
            )
            .await?
            .value;
        // Negative control: a generation key misses for every data write.
        assert!(head_keyed.insert(checkout.generation_id(), ()).is_none());
        let next = checkout
            .compiled_access_policy(WorkBudget::UNBOUNDED, &cancel)
            .await?;
        maximum_hit = maximum_hit.max(next.work.items_examined);
        assert!(Arc::ptr_eq(&first.value, &next.value));
    }
    assert_eq!(head_keyed.len(), 8);
    let old_view = binding(&first.value, "agent".into(), 256)?.0;
    assert!(old_view.decide(Action::Read, "secret", 10).allowed);
    fixtures::publish_policy(&owner, &fixtures::denied(Action::Read, "secret")).await?;
    let mut checkout = owner
        .checkout(
            GenerationSelector::Head,
            CheckoutMode::read_only_pinned(),
            WorkBudget::UNBOUNDED,
            &cancel,
        )
        .await?
        .value;
    let next = checkout
        .compiled_access_policy(WorkBudget::UNBOUNDED, &cancel)
        .await?;
    assert!(!Arc::ptr_eq(&first.value, &next.value));
    let selected = if keep_stale_revision {
        &first.value
    } else {
        &next.value
    };
    let view = binding(selected, "agent".into(), 8192)?.0;
    assert!(
        !view.decide(Action::Read, "secret", 10).allowed,
        "current policy revision must revoke read"
    );
    // Negative control: omitting the revision check returns the retained old
    // program and permits the read which the current policy denies.
    assert_ne!(
        old_view.decide(Action::Read, "secret", 10).allowed,
        view.decide(Action::Read, "secret", 10).allowed
    );
    eprintln!(
        "policy_cache first_items={} maximum_hit_items={} head_key_compiles=8 revision_compiles=1",
        first.work.items_examined, maximum_hit
    );
    Ok(())
}

#[tokio::test]
async fn data_writes_reuse_policy_revision_and_policy_changes_revoke_fresh_views()
-> Result<(), Box<dyn std::error::Error>> {
    check_revision_reuse(false).await
}

#[tokio::test]
#[should_panic(expected = "current policy revision must revoke read")]
async fn negative_control_missing_revision_check_uses_a_stale_allow() {
    assert!(check_revision_reuse(true).await.is_ok());
}

#[test]
fn caller_facts_capability_attenuation_expiry_and_history_match_the_reference()
-> Result<(), Box<dyn std::error::Error>> {
    let mut policy: Policy = serde_json::from_slice(INITIAL_POLICY)?;
    let mut restricted = policy
        .layers
        .first()
        .and_then(|layer| layer.first())
        .ok_or("missing policy rule")?
        .clone();
    restricted.id = "agent-only-until-20".into();
    restricted.principal.id = Some("permitted".into());
    restricted.conditions.expires_at = Some(20);
    policy.layers = vec![vec![restricted]];
    let config = crate::VolumeConfig::portable(crate::Lifecycle::Ephemeral);
    let mut remaining = usize::MAX;
    let shared = Arc::new(CompiledPolicy::compile_with_work(
        &policy,
        config,
        &mut remaining,
        |_| Ok(()),
    )?);
    for id in ["permitted", "other"] {
        for generation in [1, 2] {
            for now in [10, 20, 30] {
                for history in [false, true] {
                    let caller = caller(id.into());
                    let mut capability = ceiling();
                    if !history {
                        capability
                            .layers
                            .first_mut()
                            .and_then(|layer| layer.first_mut())
                            .ok_or("missing grant")?
                            .actions
                            .remove(&Action::ReadHistory);
                    }
                    capability.expires_at = Some(25);
                    let resource = Resource {
                        config,
                        lineage: &[],
                        volume: "project",
                        generation,
                        head: 2,
                        path: "dir/file",
                    };
                    let context = Context { now, approvals: 0 };
                    let view = shared.bind_with_work(
                        &caller,
                        resource,
                        context,
                        &capability,
                        &mut remaining,
                        |_| Ok(()),
                    )?;
                    for action in Action::ALL {
                        assert_eq!(
                            view.decide(action, resource.path, now),
                            crate::access::decide(
                                &policy,
                                &caller,
                                action,
                                resource,
                                context,
                                &capability
                            )
                        );
                    }
                    if now == 10 {
                        assert_eq!(view.valid_until(), Some(20));
                        assert!(!view.decide(Action::Read, resource.path, 20).allowed);
                    }
                }
            }
        }
    }
    Ok(())
}
