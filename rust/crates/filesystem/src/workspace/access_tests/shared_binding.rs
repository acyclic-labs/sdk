use super::*;
use crate::access::{
    AccessBinding, AuthenticatedFacts, Capability, Context, Policy, Principal, PrincipalKind,
};

fn caller() -> Principal {
    Principal {
        kind: PrincipalKind::Agent,
        id: "fixture".into(),
        lineage: vec![],
        groups: Default::default(),
        roles: Default::default(),
        tags: Default::default(),
        attributes: Default::default(),
    }
}

#[tokio::test]
async fn every_adapter_uses_the_metered_persisted_policy_boundary()
-> Result<(), Box<dyn std::error::Error>> {
    let workspace = workspace().await?;
    let cancel = CancellationToken::new();
    let principal = caller();
    let capability = Capability::whole_volume(None, &Action::ALL);
    for facts in [
        AuthenticatedFacts::Identity,
        AuthenticatedFacts::MetadataStream,
        AuthenticatedFacts::Coarse,
    ] {
        let mut checkout = workspace
            .checkout(
                GenerationSelector::Head,
                CheckoutMode::read_only_pinned(),
                WorkBudget::UNBOUNDED,
                &cancel,
            )
            .await?
            .value;
        let binding = || AccessBinding {
            principal: &principal,
            capability: &capability,
            facts,
            generation: None,
            context: Context {
                now: 10,
                approvals: 0,
            },
        };
        let observed = checkout
            .compile_access(binding(), WorkBudget::UNBOUNDED, &cancel)
            .await?;
        assert!(observed.value.decide(Action::Read, "file", 10).allowed);
        assert!(observed.work.items_examined > 0);
        let mut exhausted = WorkBudget::UNBOUNDED;
        exhausted.items_examined = 0;
        assert!(
            checkout
                .compile_access(binding(), exhausted, &cancel)
                .await
                .is_err()
        );
        // Negative control: a direct unmetered decision ignores this zero work window.
        let policy: Policy = serde_json::from_slice(INITIAL_POLICY)?;
        assert!(
            crate::access::decide(
                &policy,
                &principal,
                Action::Read,
                crate::access::Resource {
                    config: checkout.volume_config(),
                    lineage: &[],
                    volume: "fixture",
                    generation: 1,
                    head: 1,
                    path: "file"
                },
                Context {
                    now: 10,
                    approvals: 0
                },
                &capability
            )
            .allowed
        );
    }
    Ok(())
}

#[tokio::test]
async fn absent_approval_facts_cannot_silently_disable_a_deny_rule()
-> Result<(), Box<dyn std::error::Error>> {
    let workspace = workspace().await?;
    let cancel = CancellationToken::new();
    let principal = caller();
    let capability = Capability::whole_volume(None, &Action::ALL);
    let mut policy = fixtures::denied(Action::MergeInto, "**");
    policy.layers[0][1].conditions.approvals = Some(1);
    fixtures::publish_policy(&workspace, &policy).await?;
    let mut checkout = workspace
        .checkout(
            GenerationSelector::Head,
            CheckoutMode::read_only_pinned(),
            WorkBudget::UNBOUNDED,
            &cancel,
        )
        .await?
        .value;
    for facts in [
        AuthenticatedFacts::Identity,
        AuthenticatedFacts::MetadataStream,
        AuthenticatedFacts::Coarse,
    ] {
        assert!(
            checkout
                .compile_access(
                    AccessBinding {
                        principal: &principal,
                        capability: &capability,
                        facts,
                        generation: None,
                        context: Context {
                            now: 10,
                            approvals: 0
                        }
                    },
                    WorkBudget::UNBOUNDED,
                    &cancel
                )
                .await
                .is_err()
        );
    }
    // Negative control: supplying fabricated zero approvals suppresses the conditional deny.
    let mut remaining = usize::MAX;
    let view = crate::access::PathTrie::compile_with_work(
        &policy,
        &principal,
        crate::access::Resource {
            config: crate::VolumeConfig::portable(crate::Lifecycle::Ephemeral),
            lineage: &[],
            volume: "fixture",
            generation: 1,
            head: 1,
            path: "",
        },
        Context {
            now: 10,
            approvals: 0,
        },
        &capability,
        &mut remaining,
        |_| Ok(()),
    )?;
    assert!(view.decide(Action::MergeInto, "file", 10).allowed);
    Ok(())
}
