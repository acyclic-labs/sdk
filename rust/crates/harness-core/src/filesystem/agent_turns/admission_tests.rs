use super::*;
use crate::{conversation::VolumeRef, core::AuthorityRoot, resources::ProviderRef};

#[test]
fn pure_agent_admission_has_no_mint_and_intersects_the_actual_private_owner() -> Result<()> {
    let path = AgentPath::root(&crate::agent::AgentId::from_bytes([191; 16]));
    let owner = crate::AgentId::from_bytes(path.id().to_bytes());
    let audience = Authority {
        kind: AggregateKind::Task,
        id: "admission-cost".into(),
    };
    let root = AuthorityRoot::new("admission-cost", [192; 32], audience.clone());
    let issuer = RuntimeAuthority::from(root.clone());
    let volume = VolumeRef::new(
        ProviderRef::new(
            "admission-cost".into(),
            "filesystem".into(),
            "2".into(),
            &mut None,
        )?,
        "private".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(owner),
        &mut None,
    )?;
    let volumes = [VolumeBinding {
        name: PRIVATE_VOLUME.into(),
        volume,
    }];
    let ceiling =
        acyclic_fs::access::Capability::whole_volume(None, &acyclic_fs::access::Action::ALL)
            .to_scope_label()
            .map_err(|_| Error::Invalid("fixture filesystem ceiling".into()))?;
    let scope = root.bootstrap_scope("caller", &Capabilities::new([ceiling]), None, &mut None)?;
    let admission = TurnAdmission::Admitted {
        audience: audience.clone(),
        scope: scope.clone(),
    };
    AdmittedAgent::verify(&issuer, &admission, &path, &volumes)?;
    for callers in [1, 100, 1000] {
        let (result, signatures) = acyclic_test_support::differential::observe(
            || (),
            |()| -> Result<()> {
                for _ in 0..callers {
                    AdmittedAgent::verify(&issuer, &admission, &path, &volumes)?;
                }
                Ok(())
            },
        );
        result?;
        let signatures: Vec<acyclic_auth::crypto::SignatureVerification> = signatures;
        assert!(
            signatures.is_empty(),
            "warm admission minted or reauthenticated a credential"
        );
    }
    // The former mint-and-discard path does real cryptographic work; removing
    // it is observed against the same issuer, grant and private binding.
    let (result, signatures) = acyclic_test_support::differential::observe(
        || (),
        |()| {
            futures::executor::block_on(async {
                AdmittedAgent::verify(&issuer, &admission, &path, &volumes)?
                    .authority(&[])
                    .await
                    .map(|_| ())
            })
        },
    );
    result?;
    let signatures: Vec<acyclic_auth::crypto::SignatureVerification> = signatures;
    assert!(!signatures.is_empty());
    let foreign = [VolumeBinding {
        name: PRIVATE_VOLUME.into(),
        volume: VolumeRef::new(
            volumes[0].volume.provider().clone(),
            "private".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(crate::AgentId::from_bytes([193; 16])),
            &mut None,
        )?,
    }];
    assert!(AdmittedAgent::verify(&issuer, &admission, &path, &foreign).is_err());

    // A valid private binding cannot authorize another agent's fresh volume.
    // Use the same owner predicate as signed volume writes, before any IO.
    let local = AdmittedAgent::verify(&issuer, &TurnAdmission::LocalRoot, &path, &volumes)?;
    let (result, signatures) = acyclic_test_support::differential::observe(
        || (),
        |()| -> Result<()> {
            local.authorize_volume(&volumes[0].volume)?;
            let error = local
                .authorize_volume(&foreign[0].volume)
                .expect_err("local root accepted a foreign fresh volume");
            assert!(
                error
                    .to_string()
                    .contains("only the owning agent may write")
            );
            Ok(())
        },
    );
    result?;
    let signatures: Vec<acyclic_auth::crypto::SignatureVerification> = signatures;
    assert!(
        signatures.is_empty(),
        "local ownership admission minted a scope"
    );
    let read_only = root.bootstrap_scope(
        "read-only",
        &Capabilities::new([volumes[0]
            .volume
            .capability(VolumeOperation::Read, &mut None)?]),
        Some(owner),
        &mut None,
    )?;
    assert!(
        AdmittedAgent::verify(
            &issuer,
            &TurnAdmission::Admitted {
                audience,
                scope: read_only
            },
            &path,
            &volumes
        )
        .is_err()
    );
    root.revoke_workload(&scope)?;
    assert!(AdmittedAgent::verify(&issuer, &admission, &path, &volumes).is_err());
    Ok(())
}
