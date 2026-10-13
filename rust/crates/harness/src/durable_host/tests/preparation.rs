use super::*;
use crate::resource_preparation::{ResourceCreationProofRef, ResourcePreparationRequest};
use crate::resources::{ResourceKind, ResourceRef};

#[tokio::test]
async fn b24_sdk_preparation_validates_proofs_duplicates_and_finite_bounds() -> Result<()> {
    let f = fixture::fixture().await?;
    let provider = ProviderRef::new("custom".into(), "allocator".into(), "1".into(), &mut None)?;
    let mut child = f.admission(3, json!(0));
    child.parent = Some(TaskId::from_bytes([1; 16]));
    child = child.with_resource_proofs(vec![ResourceCreationProofRef {
        provider: provider.clone(),
        preparation: TaskId::from_bytes([2; 16]),
        resource: ResourceRef::new(
            ResourceKind::Sandbox,
            provider.clone(),
            b"one".to_vec(),
            None,
            &mut None,
        )?,
        evidence: ResourceRef::new(
            ResourceKind::Stream,
            provider.clone(),
            b"allocation".to_vec(),
            None,
            &mut None,
        )?,
    }])?;
    let request = ResourcePreparationRequest {
        provider,
        child,
        creation_grant: "resource:create:custom".into(),
        maximum_resources: 3,
        maximum_work: 7,
        maximum_bytes: 65_536,
        payload: json!({"allocation":"one"}),
        command: None,
        source_checkpoint: None,
        input_files: Vec::new(),
        read_authorization: None,
    };
    request.validate(&mut None)?;
    for control in 0..8 {
        let mut invalid = request.clone();
        match control {
            0 => invalid.child.resource_proofs.clear(),
            1 => {
                invalid.child.resource_proofs[0].provider =
                    ProviderRef::new("foreign".into(), "allocator".into(), "1".into(), &mut None)?
            }
            2 => invalid
                .child
                .resource_proofs
                .push(invalid.child.resource_proofs[0].clone()),
            3 => invalid.maximum_bytes = 1,
            4 => invalid.maximum_work = u64::MAX,
            5 => invalid.maximum_bytes = u64::MAX,
            6 => invalid.maximum_resources = 0,
            _ => invalid.maximum_work = 0,
        }
        assert!(invalid.validate(&mut None).is_err());
    }
    Ok(())
}
