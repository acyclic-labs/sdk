use super::*;
use acyclic_test_support::measure;

fn fixed_subscription() -> Subscription {
    subscription(acyclic_fs::VolumeId::from_bytes([7; 16]), false)
}

fn wire_subscription(sub: &Subscription) -> wire::SessionFsSubscription {
    wire::SessionFsSubscription {
        version: 1,
        volume_id: sub.volume_id.into_bytes().to_vec(),
        workspace: Some(wire::WorkspaceRef {
            workspace_id: sub.volume_id.into_bytes().to_vec(),
            name: sub.workspace_name.as_str().into(),
        }),
        path_prefix: sub.path_prefix.as_str().into(),
        delivery: sub.delivery as i32,
        start_next: sub.start_next,
    }
}

#[test]
fn encoding_allocates_only_wire_fields_and_output() -> Result {
    let sub = fixed_subscription();
    let reference = wire_subscription(&fixed_subscription()).encode_to_vec();
    let mut encoded = None;
    let counts = measure(|| encoded = Some(sub.encode().unwrap()));
    assert_eq!(counts.count_total, 5);
    assert_eq!(encoded.take().unwrap().as_ref(), reference);
    // Restore the redundant deep protobuf clone on the actual encoder path.
    let extra = measure(|| {
        let wire = wire_subscription(&sub);
        std::hint::black_box(wire.clone());
        std::hint::black_box(sub.encode().unwrap());
    });
    assert_ne!(
        extra.count_total, 5,
        "deep-copy control escaped allocation oracle"
    );

    let publication = Publication {
        workspace_name: sub.workspace_name.clone(),
        generation: acyclic_fs::kernel::PublishedGeneration {
            volume_id: sub.volume_id,
            generation_root: acyclic_fs::ObjectId {
                kind: acyclic_fs::ObjectKind::GenerationRoot,
                digest: acyclic_fs::Digest::from_bytes([9; 32]),
            },
        },
        sequence: acyclic_fs::Sequence::new(2),
        commit_id: acyclic_stream::CommitId::from_bytes([3; 32]),
    };
    let expected = wire::SessionFsPublication {
        version: 1,
        volume_id: sub.volume_id.into_bytes().to_vec(),
        generation: Some(wire::GenerationRef {
            workspace: wire_subscription(&fixed_subscription()).workspace,
            generation_id: vec![9; 32],
        }),
        sequence: 2,
        commit_id: vec![3; 32],
    }
    .encode_to_vec();
    let counts = measure(|| encoded = Some(publication.encode().unwrap()));
    assert_eq!(counts.count_total, 6);
    assert_eq!(encoded.take().unwrap().as_ref(), expected);

    let cursor = Cursor {
        subscription: sub,
        next: 3,
        preceding_commit_id: publication.commit_id,
    };
    let expected = wire::SessionFsCursor {
        version: 1,
        subscription: Some(wire_subscription(&fixed_subscription())),
        next: 3,
        preceding_commit_id: vec![3; 32],
    }
    .encode_to_vec();
    let counts = measure(|| encoded = Some(cursor.encode().unwrap()));
    assert_eq!(counts.count_total, 6);
    assert_eq!(encoded.unwrap().as_ref(), expected);
    let copied = measure(|| {
        let copied = cursor.subscription.clone();
        std::hint::black_box(
            Cursor {
                subscription: copied,
                ..cursor.clone()
            }
            .encode()
            .unwrap(),
        );
    });
    assert_ne!(
        copied.count_total, 6,
        "subscription-copy control escaped allocation oracle"
    );
    Ok(())
}
