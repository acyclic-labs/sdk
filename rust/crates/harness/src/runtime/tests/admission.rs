use super::*;

#[test]
fn v2_task_admission_and_execution_placement_fixtures_are_canonical() -> Result<()> {
    let admission_fixture = include_str!("../../../fixtures/v2/task-admission.json").trim();
    let admission_value: Value = serde_json::from_str(admission_fixture)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    let admission = TaskAdmissionRecord::from_canonical_value(admission_value, &mut None)?.0;
    assert_eq!(
        crate::contract::canonical_json_bytes(&admission.canonical_value())?,
        admission_fixture.as_bytes()
    );
    let placement_fixture = include_str!("../../../fixtures/v2/execution-placement.json").trim();
    let placement: ExecutionPlacement = serde_json::from_str(placement_fixture)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    placement.validate(&mut None)?;
    let placement_value =
        serde_json::to_value(&placement).map_err(|error| Error::Invalid(error.to_string()))?;
    assert_eq!(
        crate::contract::canonical_json_bytes(&placement_value)?,
        crate::contract::canonical_json_bytes(
            &serde_json::from_str::<Value>(placement_fixture)
                .map_err(|error| Error::Invalid(error.to_string()))?
        )?
    );
    Ok(())
}

#[test]
fn task_children_validation_matches_typescript_boundary() -> Result<()> {
    let parent = TaskId::from_bytes([1; 16]);
    assert!(validate_children_request(TaskId::from_bytes([0; 16]), None, 1).is_err());
    assert!(validate_children_request(parent, None, 0).is_err());
    validate_children_request(parent, None, usize::MAX)?;
    assert!(validate_children_request(parent, Some("\u{7f}"), 1).is_err());
    validate_children_request(parent, Some(&"x".repeat(1_024)), 1)?;
    validate_children_request(
        parent,
        Some(
            "rÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â©sumÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â©",
        ),
        1,
    )?;

    let child = TaskChild {
        slot: "a".into(),
        task_id: TaskId::from_bytes([2; 16]),
    };
    let child_task_id = child.task_id;
    let mut page = TaskChildrenPage {
        revision: 3,
        entries: vec![child],
        next_after: Some("a".into()),
    };
    validate_children_page(&page, Some(3), None, 1)?;
    assert!(validate_children_page(&page, Some(4), None, 1).is_err());
    assert!(validate_children_page(&page, None, Some("a"), 1).is_err());
    assert!(validate_children_page(&page, None, None, 0).is_err());

    page.next_after = Some("b".into());
    assert!(validate_children_page(&page, None, None, 1).is_err());
    page.next_after = Some("a".into());
    page.entries[0].task_id = TaskId::from_bytes([0; 16]);
    assert!(validate_children_page(&page, None, None, 1).is_err());
    page.entries[0].task_id = child_task_id;
    page.entries[0].slot = "".into();
    assert!(validate_children_page(&page, None, None, 1).is_err());
    page = TaskChildrenPage {
        revision: 3,
        entries: vec![
            TaskChild {
                slot: "a".into(),
                task_id: child_task_id,
            },
            TaskChild {
                slot: "a".into(),
                task_id: TaskId::from_bytes([3; 16]),
            },
        ],
        next_after: None,
    };
    assert!(validate_children_page(&page, None, None, 2).is_err());
    page.entries[1].slot = "b".into();
    page.entries[1].task_id = child_task_id;
    assert!(validate_children_page(&page, None, None, 2).is_err());
    Ok(())
}

#[test]
fn created_resource_proofs_are_exact_canonical_child_data() -> Result<()> {
    use crate::resource_preparation::ResourceCreationProofRef;
    use crate::resources::{ProviderRef, ResourceKind, ResourceRef};

    let wire: Value =
        serde_json::from_str(include_str!("../../../fixtures/v2/task-admission.json"))
            .map_err(|error| Error::Invalid(error.to_string()))?;
    let mut admission = TaskAdmissionRecord::from_canonical_value(wire.clone(), &mut None)?.0;
    let provider = ProviderRef::new("test".into(), "creator".into(), "1".into(), &mut None)?;
    let proof = ResourceCreationProofRef {
        provider: provider.clone(),
        preparation: TaskId::from_bytes([71; 16]),
        resource: ResourceRef::new(
            ResourceKind::Sandbox,
            provider.clone(),
            b"child".to_vec().into(),
            None,
            &mut None,
        )?,
        evidence: ResourceRef::new(
            ResourceKind::Stream,
            provider,
            b"allocation".to_vec().into(),
            None,
            &mut None,
        )?,
    };
    assert!(
        admission
            .clone()
            .with_resource_proofs(vec![proof.clone()])
            .is_err()
    );
    admission.parent = Some(TaskId::from_bytes([72; 16]));
    let original_grants = admission.grants.clone();
    let child = admission.with_resource_proofs(vec![proof.clone()])?;
    assert_eq!(child.grants, original_grants);
    let canonical = child.canonical_value();
    assert!(TaskAdmissionRecord::from_canonical_value(canonical.clone(), &mut None)?.0 == child);
    assert!(
        child
            .clone()
            .with_resource_proofs(vec![proof.clone(), proof.clone()])
            .is_err()
    );
    let mut changed = proof;
    changed.preparation = TaskId::from_bytes([73; 16]);
    let changed_child = child.clone().with_resource_proofs(vec![changed])?;
    assert_ne!(
        crate::contract::canonical_json_digest(&canonical)?,
        crate::contract::canonical_json_digest(&changed_child.canonical_value())?
    );
    let mut missing = wire;
    missing
        .as_object_mut()
        .ok_or_else(|| Error::Invalid("fixture is not an object".into()))?
        .remove("resource_proofs");
    assert!(TaskAdmissionRecord::from_canonical_value(missing, &mut None).is_err());
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn rejected_native_admission_releases_conversion_reservation() -> Result<()> {
    use crate::store::history::HistoryIo;
    let mut value: Value =
        serde_json::from_str(include_str!("../../../fixtures/v2/task-admission.json"))
            .map_err(|error| Error::Invalid(error.to_string()))?;
    value["task"]["digest"] = serde_json::to_value([0_u8; 32]).expect("digest serializes");
    let bytes = crate::contract::canonical_json_bytes(&value)?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(1_000_000, 64 * 1024 * 1024),
        &cancellation,
    )
    .map_err(|failure| Error::NativeWork {
        cause: failure.0,
        custody: None,
    })?;
    let mut io =
        crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(&work)?);
    let (value, held) = io.decode_json_value(&bytes)?;
    assert!(matches!(
        TaskAdmissionRecord::from_canonical_value(value, &mut Some(&mut io)),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid,
            ..
        })
    ));
    assert_eq!(io.into_custody()?.bytes(), held);
    Ok(())
}
