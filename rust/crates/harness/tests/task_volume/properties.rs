use super::*;
use acyclic_stream::{CommitCondition, CommitId, CommitMutation, CommitRequest, IdempotencyKey};
use proptest::prelude::*;

fn key() -> impl Strategy<Value = String> {
    (0..22_u8, "[a-z][a-z0-9_-]{0,7}", any::<u64>(), any::<u64>()).prop_map(
        |(kind, name, turn, sequence)| match kind {
            0 => "GOAL.md".into(),
            1 => format!(".volumes/agents/{name}"),
            2 => format!(".tasks/{name}/requests/r.json"),
            3 => format!(".tasks/{name}/status.json"),
            4 => format!(".tasks/{name}/turns/{turn}/request.json"),
            5 => format!(".tasks/{name}/turns/{turn}/chunks/{sequence}"),
            6 => format!(".tasks/{name}/turns/{turn}/text.md"),
            7 => format!(".tasks/{name}/turns/{turn}/reasoning.md"),
            8 => format!(".tasks/{name}/turns/{turn}/completion.json"),
            9 => format!(".tasks/{name}/turns/{turn}/tools/c/request.json"),
            10 => format!(".tasks/{name}/turns/{turn}/tools/c/arguments/{sequence}"),
            11 => format!(".tasks/{name}/turns/{turn}/tools/c/stdout/{sequence}"),
            12 => format!(".tasks/{name}/turns/{turn}/tools/c/stderr/{sequence}"),
            13 => format!(".tasks/{name}/turns/{turn}/tools/c/result.json"),
            14 => format!(".tasks/{name}/approvals/a/request.json"),
            15 => format!(".tasks/{name}/approvals/a/decisions/p.json"),
            16 => format!("plans/{name}.md"),
            17 => format!(".tasks/{name}/notifications/n.json"),
            19 => format!(".tasks/{name}/requests/r/started.json"),
            20 => format!(".tasks/{name}/requests/r/ready.json"),
            21 => format!(".tasks/{name}/requests/r/result.json"),
            _ => format!(".tasks/{name}/context/r/manifest.json"),
        },
    )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn canonical_paths_and_json_preserve_identity(key in key()) {
        let key=FileKey::parse(&key)?;
        prop_assert_eq!(FileKey::parse(&key.path()).ok(), Some(key.clone()));
        let encoded = serde_json::to_vec(&key)?;
        let decoded: FileKey = serde_json::from_slice(&encoded)?;
        prop_assert_eq!(decoded, key);
    }

    #[test]
    fn every_accepted_path_is_already_canonical(path in ".{0,4200}") {
        if let Ok(key) = FileKey::parse(&path) {
            prop_assert_eq!(key.path(), path);
        }
    }

    #[test]
    fn serialized_events_preserve_identity_channel_and_payload(
        key in key(), bytes in proptest::collection::vec(any::<u8>(),0..128), reasoning in any::<bool>()
    ) {
        let key=FileKey::parse(&key)?;
        let result: Result = futures::executor::block_on(async {
            let (provider, volume) = memory().await?;
            let cut = volume.snapshot(&Query::Root, 0).await?;
            let mut file = Write::new(key, bytes);
            if matches!(file.key, FileKey::Task { file: acyclic_harness::task_volume::TaskFile::Chunk { .. }, .. }) {
                file.channel = Some(if reasoning { Channel::Reasoning } else { Channel::Text });
            }
            let result=volume.append(cut.generation, &file).await;
            if matches!(file.key, FileKey::Task { file: acyclic_harness::task_volume::TaskFile::TextHead { .. }, .. }) {
                assert!(matches!(result,Err(Error::InvalidSegment)));
                return Ok(());
            }
            result?;
            let mut records = provider.read(ReadRequest { path: StreamPath::new("task-volume")?, from: 0, limit: 2 }).await?;
            let header=records.next().await.ok_or("missing header")??;
            assert_eq!(Write::from_record(&header)?,None);
            let record = records.next().await.ok_or("missing event")??;
            let decoded=Write::from_record(&record)?.ok_or("missing decoded file")?;
            assert_eq!(decoded,file);
            if !decoded.bytes.is_empty() {
                let offset=record.value.len().checked_sub(decoded.bytes.len()).ok_or("payload exceeds segment")?;
                assert_eq!(decoded.bytes.as_ptr(),record.value.as_ptr().wrapping_add(offset));
            }
            Ok(())
        });
        prop_assert!(result.is_ok(), "{result:?}");
    }

    #[test]
    fn cursor_and_query_json_round_trip(
        key in key(), position in any::<u64>(), accepted in any::<u64>(), durable in any::<u64>(), identity in any::<[u8;32]>()
    ) {
        let key=FileKey::parse(&key)?;
        for query in [Query::File(key), Query::Plans] {
            prop_assert_eq!(serde_json::from_slice::<Query>(&serde_json::to_vec(&query)?)?, query);
        }
        let cursor = Cursor { volume: identity,epoch: uuid::Uuid::from_bytes([7;16]),schema: 1,query: identity,position,accepted,durable,commit: identity };
        prop_assert_eq!(serde_json::from_slice::<Cursor>(&serde_json::to_vec(&cursor)?)?, cursor);
    }

    #[test]
    fn watermark_updates_are_monotonic_and_failure_is_atomic(
        position in 1..100_u64, accepted in 0..150_u64, durable in 0..150_u64
    ) {
        let mut cursor = Cursor {volume:[0;32],epoch:uuid::Uuid::nil(),schema:1,query:[0;32],position,accepted:position,durable:position/2,commit:[0;32]};
        let before=cursor.clone();
        let result=cursor.advance_watermarks(acyclic_stream::StreamWatermarks {accepted,durable});
        prop_assert_eq!(result.is_ok(), durable<=accepted && accepted>=position && durable>=position/2);
        if result.is_err() {prop_assert_eq!(cursor,before);} else {prop_assert_eq!((cursor.accepted,cursor.durable),(accepted,durable));}
    }

    #[test]
    fn head_condition_wire_round_trip_binds_the_full_identity(position in any::<u64>(), identity in any::<[u8;32]>()) {
        let request=CommitRequest { conditions:vec![CommitCondition::Head {path:StreamPath::new("head")?,expected:position,commit_id:CommitId::from_bytes(identity)}],mutations:vec![CommitMutation::Append {path:StreamPath::new("head")?,records:vec![OwnedBytes::from_static(b"event")]}],idempotency_key:IdempotencyKey::new(OwnedBytes::from_static(b"key"))?};
        let wire=acyclic_stream::wire_codec::commit_to_wire(&request);
        prop_assert_eq!(acyclic_stream::wire_codec::commit_from_wire(wire)?,request);
        let observation=acyclic_stream::IdempotencyObservation {
            idempotency_key:IdempotencyKey::new(OwnedBytes::from_static(b"key"))?,request_digest:identity,
            outcome:acyclic_stream::IdempotencyOutcome::Commit(acyclic_stream::CommitOutcome::Conflict(vec![acyclic_stream::CommitConflict::Head {
                path:StreamPath::new("head")?,expected:CommitId::from_bytes(identity),actual:CommitId::from_bytes(identity.map(|b|b^255))
            }]))
        };
        let bytes=acyclic_stream::persistence::encode_observation(&observation,512)?;
        prop_assert_eq!(acyclic_stream::persistence::decode_observation(&bytes,512)?,observation);
    }

    #[test]
    fn indexed_queries_equal_the_reference_fold_at_every_publication(
        operations in proptest::collection::vec((0..8_u8,0..3_u8,0..3_u64,proptest::collection::vec(any::<u8>(),0..16)),0..16)
    ) {
        let result: Result=futures::executor::block_on(async {
            let (_,volume)=memory().await?;
            let mut generation=volume.snapshot(&Query::Root,0).await?.generation;
            let mut reference=BTreeMap::new();
            for (n,(kind,task,turn,bytes)) in operations.into_iter().enumerate() {
                let task=Component::new(format!("t{task}"))?;
                let path=match kind {
                    0=>"GOAL.md".into(),
                    1=>format!(".tasks/{}/status.json",task.as_str()),
                    2=>format!("plans/{}.md",task.as_str()),
                    3=>format!(".tasks/{}/turns/{turn}/chunks/{n}",task.as_str()),
                    4=>format!(".tasks/{}/notifications/n{n}.json",task.as_str()),
                    5=>format!(".tasks/{}/approvals/a{n}/request.json",task.as_str()),
                    6=>format!(".tasks/{}/turns/{turn}/tools/c/stdout/{n}",task.as_str()),
                    _=>format!(".volumes/agents/a{n}"),
                };
                let mut file=Write::new(FileKey::parse(&path)?,bytes);
                if kind==3 {file.channel=Some(Channel::Text);}
                generation=volume.append(generation,&file).await?;
                reference.insert(file.key.clone(),file.clone());
                let queries=[Query::Root,Query::Plans,Query::Tasks,Query::Agents,Query::Task(task.clone()),Query::Requests(task.clone()),Query::Turns(task.clone()),Query::Turn {task:task.clone(),turn},Query::Chunks {task:task.clone(),turn},Query::Tools {task:task.clone(),turn},Query::Approvals(task.clone()),Query::Notifications(task.clone()),Query::File(file.key)];
                for query in queries {
                    let expected=reference.values().filter(|file|query.matches(&file.key)).cloned().collect::<Vec<_>>();
                    assert_eq!(volume.snapshot(&query,100).await?.files,expected,"{query:?}");
                }
                assert_eq!(Query::folder(&format!(".tasks/{}/turns/{turn}/tools",task.as_str()))?,Query::Tools {task,turn});
            }
            Ok(())
        });
        prop_assert!(result.is_ok(),"{result:?}");
    }
}
