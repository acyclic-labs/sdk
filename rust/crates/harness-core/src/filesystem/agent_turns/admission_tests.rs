use super::*;
use crate::{conversation::VolumeRef, core::AuthorityRoot, resources::ProviderRef};

#[tokio::test]
async fn frozen_notice_ranges_page_without_gaining_later_records() -> Result<()> {
    use crate::agent::{AgentId, InboxRecord, IntentId, NoticePayload, NoticeSelection};
    use crate::conversation::wire::Notice;
    use crate::model::ModelContentPart;
    use acyclic_stream::{
        AppendRequest, IdempotencyKey, MemoryStream, StreamError, StreamProviderExt as _,
    };

    let stream = MemoryStream::default();
    let agent = AgentPath::root(&AgentId::from_bytes([194; 16]));
    let count = acyclic_stream::MAX_ITEMS + 1;
    let mut records = Vec::new();
    for index in 0..=count {
        if index == acyclic_stream::MAX_ITEMS / 2 {
            records.push(InboxRecord::Awaited.encode().map_err(agent_error)?);
        }
        records.push(
            InboxRecord::Notice {
                id: IntentId::try_from(format!("event-{index}")).map_err(agent_error)?,
                notice: NoticePayload::Quiet(Notice {
                    source: "events".into(),
                    kind: "source.changed".into(),
                    body: serde_json::json!({"index": index}),
                    wake: false,
                    coalesce_key: Some("source".into()),
                }),
            }
            .encode()
            .map_err(agent_error)?,
        );
    }
    let mut from = 0;
    for (page, records) in records.chunks(acyclic_stream::MAX_ITEMS).enumerate() {
        stream
            .append(AppendRequest {
                path: agent.inbox().map_err(agent_error)?,
                records: records.iter().cloned().map(Into::into).collect(),
                if_tail: Some(from),
                idempotency_key: IdempotencyKey::new(format!("notices-{page}").into_bytes())?,
            })
            .await?;
        from += records.len() as u64;
    }
    let selected = NoticeSelection {
        source: agent.clone(),
        range: 0..count as u64 + 1,
    };
    let input = ModelContent::Text("prompt".into());
    let (notices, retained) =
        selected_notices(&stream, &agent, Some(&selected), input.clone(), None).await?;
    assert_eq!(retained, Some(input));
    let [ModelContent::Part(ModelContentPart::Notices { notices })] = notices.as_slice() else {
        panic!("missing event boundary");
    };
    assert_eq!(notices.len(), 1);
    assert_eq!(notices[0].body["index"], count - 1);
    let unavailable = NoticeSelection {
        range: 0..from + 1,
        ..selected
    };
    assert!(
        selected_notices(
            &stream,
            &agent,
            Some(&unavailable),
            ModelContent::Text("prompt".into()),
            None,
        )
        .await
        .expect_err("a missing frozen suffix must refuse")
        .uncertain()
    );
    for cause in [StreamError::Unavailable, StreamError::DeadlineElapsed] {
        let error = agent_error(crate::agent::Error::Stream(cause));
        assert!(error.uncertain());
        assert!(!Error::Invalid(error.to_string()).uncertain());
    }
    Ok(())
}

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
