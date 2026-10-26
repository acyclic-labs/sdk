#![no_main]
use acyclic_harness::{
    AgentId, Capabilities, TaskId,
    conversation::{VolumeClass, VolumeOperation, VolumeOwner, VolumeRef},
    core::{AggregateKind, Authority, AuthorityRoot},
    resources::ProviderRef,
    task_volume::{FileValue, Query, TaskFiles},
};
use acyclic_stream::{AppendRequest, MemoryStream, Record, StreamPath, StreamProvider};
use std::sync::Arc;

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    if bytes.len() > 65_536 {
        return;
    }
    futures::executor::block_on(async {
        let agent = AgentId::from_bytes([1; 16]);
        let volume = VolumeRef::new(
            ProviderRef::new("local".into(), "filesystem".into(), "2".into(), &mut None).unwrap(),
            "native-task-files-fuzz".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(agent),
            &mut None,
        )
        .unwrap();
        let issuer = AuthorityRoot::new(
            "task-files",
            [7; 32],
            Authority {
                kind: AggregateKind::Task,
                id: TaskId::from_bytes([2; 16]).to_string(),
            },
        );
        let grants = [VolumeOperation::Read, VolumeOperation::Write]
            .map(|op| volume.capability(op, &mut None).unwrap());
        let scope = issuer
            .bootstrap_scope("caller", &Capabilities::new(grants), Some(agent), &mut None)
            .unwrap();
        let verifier = issuer.verifier(&mut None).unwrap();
        let provider = Arc::new(MemoryStream::default());
        let original = TaskFiles::open(provider.clone(), &volume, &verifier, &scope, 65_536)
            .await
            .unwrap();
        let path = StreamPath::new(format!(
            "harness/task-volumes/{}",
            volume.storage_name(&mut None).unwrap()
        ))
        .unwrap();
        provider
            .append(AppendRequest {
                path,
                records: vec![bytes.to_vec().into()],
                if_tail: Some(1),
                idempotency_key: None,
            })
            .await
            .unwrap();
        drop(original);
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::performance::BOUNDARY_BUDGET,
            &acyclic_fs::CancellationToken::new(),
        )
        .unwrap();
        let baseline = work.live_allocation_bytes();
        let result =
            TaskFiles::open_with_native_work(provider, &volume, &verifier, &scope, 65_536, &work)
                .await;
        match result {
            Ok(files) => {
                let record = Record {
                    sequence: 1,
                    headers: Default::default(),
                    value: bytes.to_vec().into(),
                    commit_id: acyclic_stream::CommitId::from_bytes([0; 32]),
                    committed_at_micros: 0,
                };
                let expected = FileValue::from_record(&record).unwrap();
                let snapshot = files.snapshot(&Query::Root, 4).await.unwrap();
                assert_eq!(snapshot.files, expected);
            }
            Err(error) => drop(error),
        }
        assert_eq!(work.live_allocation_bytes(), baseline);
    });
});
