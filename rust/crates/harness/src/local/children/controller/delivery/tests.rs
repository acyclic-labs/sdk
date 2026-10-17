use super::*;
use crate::conversation::{VolumeClass, VolumeOwner, VolumeRef};
use crate::{
    AgentId, Capabilities,
    core::{AggregateKind, Authority, AuthorityIssuer},
    resources::ProviderRef,
};
use acyclic_stream::MemoryStream;
use std::cell::RefCell;

async fn files(provider: Arc<MemoryStream>) -> TaskFiles<MemoryStream> {
    let agent = AgentId::from_bytes([1; 16]);
    let volume = VolumeRef::new(
        ProviderRef::new("local".into(), "filesystem".into(), "2".into(), &mut None).unwrap(),
        "delivery".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(agent),
        &mut None,
    )
    .unwrap();
    let issuer = AuthorityIssuer::new(
        "delivery",
        [1; 32],
        Authority {
            kind: AggregateKind::Task,
            id: TaskId::from_bytes([2; 16]).to_string(),
        },
    );
    let grants = [
        crate::conversation::VolumeOperation::Read,
        crate::conversation::VolumeOperation::Write,
    ]
    .map(|op| volume.capability(op, &mut None).unwrap());
    let scope = issuer
        .issue(
            "owner",
            &Capabilities::new(grants),
            Some(agent),
            None,
            &mut None,
        )
        .unwrap();
    TaskFiles::open(
        provider,
        &volume,
        &issuer.verifier(&mut None).unwrap(),
        &scope,
        4096,
    )
    .await
    .unwrap()
}

async fn append(files: &TaskFiles<MemoryStream>, reference: &Component, kind: ChildFile) {
    let file = FileValue::new(
        FileKey::Child {
            reference: reference.clone(),
            file: kind,
        },
        b"message".to_vec(),
    );
    let generation = files.inner.head().await.unwrap();
    files.write(generation, &file).await.unwrap();
}

async fn sequential<const RETAIN: bool>(n: u64) -> (usize, Vec<FileKey>) {
    let provider = Arc::new(MemoryStream::default());
    let mut files = files(provider.clone()).await;
    let reference = Component::new("worker").unwrap();
    let child = TaskId::from_bytes([3; 16]);
    let calls = RefCell::new(Vec::new());
    append(&files, &reference, ChildFile::Request).await;
    for sequence in 0..=n {
        if sequence != 0 {
            // Reverse lexical order must not reverse accepted delivery order.
            append(
                &files,
                &reference,
                ChildFile::Inbox(Component::new(format!("message-{:03}", n - sequence)).unwrap()),
            )
            .await;
        }
        suffix::<RETAIN, _, _, _>(&files, &reference, child, sequence + 1, |file| {
            calls.borrow_mut().push(file.key);
            futures::future::ready(Ok(()))
        })
        .await
        .unwrap();
        // Reopen reconstructs the accepted-order index and authentic frontier.
        drop(files);
        files = self::files(provider.clone()).await;
    }
    let calls = calls.into_inner();
    (calls.len(), calls)
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(16))]
    #[test]
    fn delivery_work_is_linear_in_the_accepted_input_count(n in 1u64..33) {
        futures::executor::block_on(async {
            let (count, _) = sequential::<true>(n).await;
            linear_property(count, n, true);
        });
    }

    #[test]
    fn frontier_cannot_regress_exceed_inputs_or_change_an_established_task(
        count in 1u64..16, prior in 0u64..16, candidate in 0u64..18, same_task in proptest::prelude::any::<bool>(),
    ) {
        futures::executor::block_on(async {
            let files = files(Arc::new(MemoryStream::default())).await;
            let reference = Component::new("worker").unwrap();
            let child = TaskId::from_bytes([3;16]);
            append(&files,&reference,ChildFile::Request).await;
            for n in 1..count { append(&files,&reference,ChildFile::Inbox(Component::new(format!("input-{n}")).unwrap())).await; }
            let prior = prior.min(count);
            if prior > 0 { acknowledge(&files,&reference,child,prior).await.unwrap(); }
            let task = if same_task { child } else { TaskId::from_bytes([4;16]) };
            let file = FileValue::new(FileKey::Child { reference,file: ChildFile::Delivered },serde_json::to_vec(&(task,candidate)).unwrap());
            let generation = files.inner.head().await.unwrap();
            let accepted = files.inner.append(generation,&file).await.is_ok();
            assert_eq!(accepted,candidate>0 && candidate<=count && (prior==0 || same_task && candidate>=prior));
        });
    }
}

fn linear_property(actual: usize, n: u64, assert_property: bool) {
    if assert_property {
        assert_eq!(
            actual,
            n as usize + 1,
            "historical inputs repeated staging/encoding/authority work"
        );
    }
}

#[tokio::test]
async fn delivery_stops_at_the_acknowledged_cut_when_a_new_input_arrives_during_staging() {
    let files = files(Arc::new(MemoryStream::default())).await;
    let reference = Component::new("worker").unwrap();
    let child = TaskId::from_bytes([3; 16]);
    append(&files, &reference, ChildFile::Request).await;
    let calls = RefCell::new(Vec::new());
    suffix::<true, _, _, _>(&files, &reference, child, 1, |file| {
        let files = &files;
        let reference = &reference;
        let calls = &calls;
        async move {
            calls.borrow_mut().push(file.key);
            append(
                files,
                reference,
                ChildFile::Inbox(Component::new("later").unwrap()),
            )
            .await;
            Ok(())
        }
    })
    .await
    .unwrap();
    assert_eq!(calls.borrow().len(), 1);
    assert_eq!(frontier(&files, &reference, child).await.unwrap(), 1);
    assert_eq!(files.inner.child_input_count(&reference).await.unwrap(), 2);
    suffix::<true, _, _, _>(&files, &reference, child, 2, |file| {
        calls.borrow_mut().push(file.key);
        futures::future::ready(Ok(()))
    })
    .await
    .unwrap();
    assert_eq!(calls.borrow().len(), 2);
}

#[tokio::test]
async fn sequential_messages_stage_exactly_one_suffix_item_each_across_reopen() {
    for n in [1, 4, 16] {
        let (count, keys) = sequential::<true>(n).await;
        linear_property(count, n, true);
        let messages: Vec<_> = keys
            .iter()
            .filter_map(|key| match key {
                FileKey::Child {
                    file: ChildFile::Inbox(id),
                    ..
                } => Some(id.as_str()),
                _ => None,
            })
            .collect();
        assert!(messages.windows(2).all(|pair| pair[0] > pair[1]));
    }
    let (broken, _) = sequential::<false>(4).await;
    assert_eq!(broken, 15);
    std::panic::catch_unwind(|| linear_property(broken, 4, true))
        .expect_err("missing delivery frontier was not detected");
    linear_property(broken, 4, false);
}

#[tokio::test]
async fn a_send_before_frontier_crash_retries_only_the_unacknowledged_input() {
    let provider = Arc::new(MemoryStream::default());
    let first = files(provider.clone()).await;
    let reference = Component::new("worker").unwrap();
    let child = TaskId::from_bytes([3; 16]);
    append(&first, &reference, ChildFile::Request).await;
    let received = RefCell::new(BTreeSet::new());
    let result = suffix::<true, _, _, _>(&first, &reference, child, 1, |file| {
        received.borrow_mut().insert(file.key);
        futures::future::ready(Err(Error::Indeterminate(OperationId::from_bytes(
            child.into_bytes(),
        ))))
    })
    .await;
    assert!(result.is_err());
    drop(first);
    let reopened = files(provider).await;
    let retries = RefCell::new(0);
    suffix::<true, _, _, _>(&reopened, &reference, child, 1, |file| {
        *retries.borrow_mut() += 1;
        received.borrow_mut().insert(file.key);
        futures::future::ready(Ok(()))
    })
    .await
    .unwrap();
    assert_eq!(*retries.borrow(), 1);
    assert_eq!(received.borrow().len(), 1);
    assert_eq!(frontier(&reopened, &reference, child).await.unwrap(), 1);
    let generation = reopened.inner.head().await.unwrap();
    let forged = FileValue::new(
        FileKey::Child {
            reference,
            file: ChildFile::Delivered,
        },
        serde_json::to_vec(&(child, 1)).unwrap(),
    );
    assert!(reopened.write(generation, &forged).await.is_err());
}
