//! A filtered checkpoint cannot impersonate the requested stop/cancel file.
use super::*;
use proptest::prelude::*;

#[tokio::test]
async fn task_stream_drivers_keep_a_parent_wait_and_child_completion_independent() -> Result<()> {
    use crate::external_executor::{ExternalHost, request};
    use serde_json::json;
    let root = tempfile::tempdir().map_err(storage)?;
    let project = root.path().join("project");
    std::fs::create_dir(&project).map_err(storage)?;
    let harness = Arc::new(LocalHarness::open(LocalHarnessConfig::external(&project)).await?);
    let stop = Arc::new(
        TaskVolume::open(
            harness.task_stream(),
            StreamPath::new("test/control").map_err(storage)?,
        )
        .await
        .map_err(storage)?,
    );
    let instance = uuid::Uuid::new_v4();
    let registry = registry::Registry::new(root.path().to_path_buf(), Arc::clone(&stop), instance);
    let draining = registry.drain();
    let size = std::mem::size_of_val(&draining);
    eprintln!("local owner drain future: {size} bytes");
    assert!(size < 16 * 1024, "owner drain future grew to {size} bytes");
    drop(draining);
    let handler = drive_task(
        Arc::clone(&harness),
        crate::TaskId::from_bytes([7; 16]),
        Arc::clone(&stop),
        instance,
    );
    let size = std::mem::size_of_val(&handler);
    eprintln!("per-task owner handler future: {size} bytes");
    assert!(size < 16 * 1024, "task handler future grew to {size} bytes");
    drop(handler);
    let scenario = async {
        for host in [
            ExternalHost::Codex,
            ExternalHost::ClaudeCode,
            ExternalHost::Pi,
        ] {
            let pi = host == ExternalHost::Pi;
            let attach = if pi { "session_start" } else { "SessionStart" };
            let parent = request(host, attach, json!({"sessionId":host.name(),"cwd":project}))?;
            let parent_files = harness.external_volumes(parent.task_id()).await?;
            let folder = Component::new(parent.task_id().to_string()).map_err(storage)?;
            parent
                .send(
                    &parent_files.requests,
                    &parent_files.results,
                    folder.clone(),
                )
                .await
                .map_err(storage)?;
            let fork = request(
                host,
                if pi {
                    "subagent_start"
                } else {
                    "SubagentStart"
                },
                json!({"sessionId":host.name(),"agentId":"child","agentType":"worker"}),
            )?;
            fork.send(
                &parent_files.requests,
                &parent_files.results,
                folder.clone(),
            )
            .await
            .map_err(storage)?;
            let child = request(
                host,
                attach,
                json!({"sessionId":host.name(),"agentId":"child","cwd":project}),
            )?;
            let child_files = harness.external_volumes(child.task_id()).await?;
            let child_folder = Component::new(child.task_id().to_string()).map_err(storage)?;
            child
                .send(
                    &child_files.requests,
                    &child_files.results,
                    child_folder.clone(),
                )
                .await
                .map_err(storage)?;
            let join = request(
                host,
                if pi { "subagent_stop" } else { "SubagentStop" },
                json!({"sessionId":host.name(),"agentId":"child"}),
            )?;
            let joined = join
                .send(
                    &parent_files.requests,
                    &parent_files.results,
                    folder.clone(),
                )
                .await
                .map_err(storage)?;
            assert_eq!(joined, json!({"Succeeded":null}));
            // A delayed SessionEnd is the exact same terminal request.
            let end = request(
                host,
                if pi { "session_shutdown" } else { "SessionEnd" },
                json!({"sessionId":host.name(),"agentId":"child"}),
            )?;
            end.send(&child_files.requests, &child_files.results, child_folder)
                .await
                .map_err(storage)?;
        }
        let file = FileValue::new(stop_key(instance)?, b"{}".as_slice());
        super::super::super::publish_request(&stop, &file, None, None)
            .await
            .map_err(storage)?;
        Ok::<_, Error>(())
    };
    let mut driver = Box::pin(drive(Arc::clone(&harness), Arc::clone(&stop), instance));
    tokio::select! {
        result = &mut driver => panic!("request driver exited before scenario completed: {result:?}"),
        result = scenario => result?,
    }
    driver.await
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]
    // Assert the actual OS option, independent of scheduling or latency oracles.
    #[test]
    fn every_incoming_socket_disables_nagle(peer_options in prop::collection::vec(any::<bool>(), 1..4)) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await.unwrap();
            let address = listener.local_addr().unwrap();
            let mut connections = Box::pin(incoming(listener));
            for option in peer_options {
                let (peer, accepted) = tokio::join!(tokio::net::TcpStream::connect(address), futures::StreamExt::next(&mut connections));
                let peer = peer.unwrap();
                peer.set_nodelay(option).unwrap();
                assert!(accepted.unwrap().unwrap().nodelay().unwrap());
            }
        });
    }

    #[test]
    fn unrelated_publications_never_complete_a_file_wait(identity in any::<[u8;16]>()) {
        let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
        runtime.block_on(async {
            let files = TaskVolume::open(Arc::new(acyclic_stream::MemoryStream::default()),
                StreamPath::new("control").unwrap()).await.unwrap();
            let instance = uuid::Uuid::from_bytes(identity);
            let key = stop_key(instance).unwrap();
            let mut changed = identity;
            changed[0] ^= 1;
            let unrelated = FileValue::new(stop_key(uuid::Uuid::from_bytes(changed)).unwrap(),b"{}".as_slice());
            let mut waiting = Box::pin(stopped(&files,instance));
            assert!(futures::poll!(waiting.as_mut()).is_pending());
            files.append(files.head().await.unwrap(),&unrelated).await.unwrap();
            assert!(futures::poll!(waiting.as_mut()).is_pending());
            let wanted = FileValue::new(key,b"{}".as_slice());
            files.append(files.head().await.unwrap(),&wanted).await.unwrap();
            waiting.await.unwrap();
        });
    }
}
