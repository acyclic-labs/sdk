use super::*;
use acyclic_stream::MemoryStream;

fn check(broken: bool) {
    loom::model(move || {
        let source = loom::future::block_on(TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("shared-observer").unwrap(),
        ))
        .unwrap();
        let observer = loom::future::block_on(source.reopen()).unwrap();
        assert!(Arc::ptr_eq(&source.index, &observer.index));
        let cut = loom::future::block_on(source.head()).unwrap();
        let file = FileValue::new(FileKey::Goal, b"first".to_vec());
        let (request, accepted, next) =
            loom::future::block_on(source.prepare_append(cut, &file, Vec::new(), false, None))
                .unwrap()
                .unwrap();
        assert!(
            loom::future::block_on(observer.snapshot(&Query::Root, 1))
                .unwrap()
                .files
                .is_empty()
        );
        // Partition at provider acceptance: before it, observation is empty.
        // After it, only the shared index acknowledgement races with refresh.
        // A delayed G1 acknowledgement must not overwrite G2's refreshed cut.
        let CommitOutcome::Committed(accepted_receipt) =
            loom::future::block_on(source.provider.commit(request)).unwrap()
        else {
            unreachable!()
        };
        // G1 is visible to observers before its writer acknowledges acceptance.
        // Put that completed refresh in the serial prefix; the conflicting
        // interval is G1's delayed acknowledgement versus the G2 suffix fold.
        let first = loom::future::block_on(observer.snapshot(&Query::Root, 1)).unwrap();
        assert_eq!(first.files, vec![file]);
        assert_eq!(first.generation.position, next);
        assert_eq!(
            first.generation.commit,
            *accepted_receipt.commit_id.as_bytes()
        );
        let latest = FileValue::new(FileKey::Goal, b"second".to_vec());
        let (record, _) = segment::encode(&latest, None).unwrap();
        let outcome = loom::future::block_on(source.provider.append(AppendRequest {
            path: source.path.clone(),
            records: vec![record],
            if_tail: Some(next),
            idempotency_key: None,
        }))
        .unwrap();
        let acyclic_stream::AppendOutcome::Committed(receipt) = outcome else {
            unreachable!()
        };
        let writer = loom::thread::spawn(move || {
            if broken {
                model_checking::set_fault(model_checking::Fault::OverwriteRebuiltIndex);
            }
            // Explore the real production acknowledgement after provider
            // acceptance, against the complete production refresh below.
            loom::future::block_on(source.acknowledge(
                cut,
                accepted,
                next,
                accepted_receipt.commit_id,
            ));
            source
        });
        let snapshot = loom::future::block_on(observer.snapshot(&Query::Root, 1)).unwrap();
        assert_eq!(snapshot.files, vec![latest.clone()]);
        assert_eq!(snapshot.generation.position, next + 1);
        assert_eq!(snapshot.generation.commit, *receipt.commit_id.as_bytes());
        let source = writer.join().unwrap();
        // Lock order: index only. No repairing operation follows the join.
        let index = loom::future::block_on(observer.index.lock());
        assert_eq!(
            source.generation(&index).position,
            next + 1,
            "shared index regressed"
        );
        assert_eq!(index.commit, receipt.commit_id);
        assert_eq!(
            index.files.get(&latest.key).map(|stored| &stored.file),
            Some(&latest)
        );
    });
}

#[test]
fn shared_observer_refresh_and_delayed_acknowledgement_are_atomic() {
    check(false);
}

#[test]
#[should_panic]
fn negative_control_detects_overwriting_the_shared_index() {
    check(true);
}
