use super::*;

#[test]
#[ignore = "invoked as a separate process by mount lifecycle tests"]
fn projected_mount_writer_child() {
    if let Some(path) = std::env::var_os("ACYCLIC_TEST_PROJECTED_WRITE_PATH") {
        fs::write(PathBuf::from(path), b"late parent").unwrap();
    }
}

#[test]
fn recursive_publication_is_direct_parent_only() {
    run_large_stack("recursive-publication", || async {
        let fixture = Fixture::new();
        fs::write(fixture.root.join("base.txt"), b"base").unwrap();
        let mut control = ControlPlane::open(fixture.data.clone()).await.unwrap();
        control
            .session_start(json!({"session_id":"session","cwd":fixture.root}))
            .await
            .unwrap();
        control
            .user_prompt(json!({"session_id":"session","turn_id":"root-turn"}))
            .unwrap();
        for (parent, child, turn) in [
            ("root-turn", "child", "child-turn"),
            ("child-turn", "grandchild", "grandchild-turn"),
        ] {
            control
                .pre_tool(json!({"session_id":"session","turn_id":parent,
                "tool_use_id":format!("spawn-{child}"),"tool_name":"spawn_agent","tool_input":{}}))
                .await
                .unwrap();
            control
                .subagent_start(json!({"session_id":"session","turn_id":turn,
                "agent_id":child,"agent_type":"explorer"}))
                .await
                .unwrap();
        }
        let child_path = route_path(&control.state.routes["child"]);
        let grandchild_path = route_path(&control.state.routes["grandchild"]);
        assert_ne!(child_path, grandchild_path);
        for (turn, tool, path) in [
            ("child-turn", "parent-read", &child_path),
            ("grandchild-turn", "descendant-read", &grandchild_path),
        ] {
            control
                .pre_tool(
                    json!({"session_id":"session","turn_id":turn,"tool_use_id":tool,
                "tool_name":"Read","tool_input":{"path":path.join("base.txt")}}),
                )
                .await
                .unwrap();
        }
        control
            .post_tool(json!({"session_id":"session","turn_id":"grandchild-turn",
            "tool_use_id":"descendant-read","tool_name":"Read"}))
            .await
            .unwrap();
        let writer = acyclic_native_runtime::process_output(
            std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "tests::recursive::projected_mount_writer_child",
                    "--ignored",
                ])
                .env(
                    "ACYCLIC_TEST_PROJECTED_WRITE_PATH",
                    child_path.join("base.txt"),
                ),
            std::time::Duration::from_secs(120),
            8 * 1024 * 1024,
        )
        .unwrap();
        assert!(
            writer.status.success(),
            "{}",
            String::from_utf8_lossy(&writer.stderr)
        );
        control
            .post_tool(json!({"session_id":"session","turn_id":"child-turn",
            "tool_use_id":"parent-read","tool_name":"Read"}))
            .await
            .unwrap();
        let grandchild = control
            .workspace(&control.state.routes["grandchild"])
            .await
            .unwrap();
        assert_eq!(
            grandchild.read("/base.txt", 32).await.unwrap().as_ref(),
            b"late parent"
        );
        let mut transaction = grandchild
            .begin_transaction(IdempotencyKey::new())
            .await
            .unwrap();
        transaction
            .write_text("/nested.txt", "nested")
            .await
            .unwrap();
        transaction
            .commit(
                None,
                acyclic_fs::PublicationPermit::Unrestricted,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
                |_| async {
                    Ok::<_, acyclic_fs::OperationFailure<acyclic_fs::WorkspaceError>>(
                        acyclic_fs::OperationReceipt {
                            value: (),
                            work: acyclic_fs::WorkCounters::default(),
                        },
                    )
                },
            )
            .await
            .unwrap();
        drop(transaction);
        assert!(
            control
                .agent_merge(json!({"agent":"grandchild","_caller_turn_id":"root-turn"}))
                .await
                .is_err()
        );
        assert!(!fixture.root.join("nested.txt").exists());
        control
            .agent_merge(json!({"agent":"grandchild","_caller_turn_id":"child-turn"}))
            .await
            .unwrap();
        assert!(!fixture.root.join("nested.txt").exists());
        control
            .agent_merge(json!({"agent":"child","_caller_turn_id":"root-turn"}))
            .await
            .unwrap();
        assert_eq!(
            fs::read(fixture.root.join("nested.txt")).unwrap(),
            b"nested"
        );
        assert_eq!(
            fs::read(fixture.root.join("base.txt")).unwrap(),
            b"late parent"
        );

        control.fail_next_unmount.insert("grandchild".into());
        assert!(
            control
                .agent_discard(json!({"agent":"child","_caller_turn_id":"root-turn"}))
                .await
                .is_err()
        );
        control.fail_after_discard_delete = true;
        assert!(
            control
                .agent_discard(json!({"agent":"child","_caller_turn_id":"root-turn"}))
                .await
                .is_err()
        );
        drop(grandchild);
        drop(control);
        let recovered = ControlPlane::open(fixture.data.clone()).await.unwrap();
        assert!(recovered.state.routes.is_empty());
        assert!(recovered.state.pending_discards.is_empty());
        assert!(recovered.state.turns.is_empty());
        recovered.shutdown().await.unwrap();
    });
}

#[test]
fn multi_root_publication_reopens_the_same_routes() {
    run_large_stack("multi-root", || async {
        let fixture = Fixture::new();
        let second = fixture.directory("second");
        for (root, contents) in [
            (&fixture.root, b"first".as_slice()),
            (&second, b"second".as_slice()),
        ] {
            fs::write(root.join("base.txt"), contents).unwrap();
        }
        let mut control = ControlPlane::open(fixture.data.clone()).await.unwrap();
        for root in [&fixture.root, &second] {
            control
                .session_start(json!({"session_id":"session","cwd":root}))
                .await
                .unwrap();
        }
        let second_root = WorkspaceRootId::from_bytes(
            control
                .state
                .roots
                .values()
                .find(|root| root.root_id != control.state.root_id)
                .unwrap()
                .root_id,
        );
        control
            .user_prompt(json!({"session_id":"session","turn_id":"root-turn"}))
            .unwrap();
        control.pre_tool(json!({"session_id":"session","turn_id":"root-turn","tool_use_id":"spawn",
            "tool_name":"spawn_agent","tool_input":{},"_caller_root_id":hex::encode(second_root.into_bytes())})).await.unwrap();
        control
            .subagent_start(
                json!({"session_id":"session","turn_id":"child-turn","agent_id":"child"}),
            )
            .await
            .unwrap();
        let route = control.state.routes["child"].clone();
        control
            .pre_tool(
                json!({"session_id":"session","turn_id":"child-turn","tool_use_id":"write",
            "tool_name":"Write","tool_input":{"path":route_path(&route).join("in-flight.txt")}}),
            )
            .await
            .unwrap();
        for input in [
            json!({"agent":"child","_caller_agent_id":control.state.root_agent_id}),
            json!({"agent":"child","_caller_turn_id":"root-turn"}),
        ] {
            assert!(control.agent_changes(input.clone()).await.is_err());
            assert!(control.agent_merge(input).await.is_err());
        }
        assert!(
            control
                .agent_discard(
                    json!({"agent":"child","_caller_agent_id":control.state.root_agent_id})
                )
                .await
                .is_err()
        );
        control.post_tool(json!({"session_id":"session","turn_id":"child-turn","tool_use_id":"write","tool_name":"Write"})).await.unwrap();
        for root in route.roots.values() {
            let workspace = control
                .workspace_root(&route, WorkspaceRootId::from_bytes(root.root_id))
                .await
                .unwrap();
            let mut transaction = workspace
                .begin_transaction(IdempotencyKey::new())
                .await
                .unwrap();
            transaction
                .write_text("/child.txt", &root.mount_name())
                .await
                .unwrap();
            transaction
                .commit(
                    None,
                    acyclic_fs::PublicationPermit::Unrestricted,
                    WorkBudget::UNBOUNDED,
                    &CancellationToken::new(),
                    |_| async {
                        Ok::<_, acyclic_fs::OperationFailure<acyclic_fs::WorkspaceError>>(
                            acyclic_fs::OperationReceipt {
                                value: (),
                                work: acyclic_fs::WorkCounters::default(),
                            },
                        )
                    },
                )
                .await
                .unwrap();
        }
        let changes = control
            .agent_changes(
                json!({"agent":"child","path":"child.txt","_caller_turn_id":"root-turn"}),
            )
            .await
            .unwrap();
        assert!(
            changes["roots"]
                .as_array()
                .unwrap()
                .iter()
                .all(|root| root["paths"] == json!(["/child.txt"]))
        );
        control.mounts["child"].advance_to_head().await.unwrap();
        control
            .agent_merge(json!({"agent":"child","_caller_turn_id":"root-turn"}))
            .await
            .unwrap();
        assert!(fixture.root.join("child.txt").is_file());
        assert!(second.join("child.txt").is_file());
        control.shutdown().await.unwrap();
        let reopened = ControlPlane::open(fixture.data.clone()).await.unwrap();
        let (_, routed, root_id) = reopened
            .route_root_from_cwd(&route_path(&reopened.state.routes["child"]))
            .unwrap();
        assert_eq!(routed.unwrap().agent_id, "child");
        assert_eq!(root_id, second_root);
        assert_eq!(reopened.state.routes["child"].roots.len(), 2);
        reopened.shutdown().await.unwrap();
    });
}

#[test]
fn root_registration_recovers_intent_and_fences_identity_changes() {
    run_large_stack("root-recovery", || async {
        let fixture = Fixture::new();
        let mut control = ControlPlane::open(fixture.data.clone()).await.unwrap();
        control.fail_after_root_intent = true;
        assert!(
            control
                .session_start(json!({"session_id":"session","cwd":fixture.root}))
                .await
                .is_err()
        );
        let context = control.state.root_context_id;
        drop(control);
        let mut recovered = ControlPlane::open(fixture.data.clone()).await.unwrap();
        assert_eq!(recovered.state.root_context_id, context);
        assert!(!recovered.state.pending_root_registration);
        let second = fixture.directory("second");
        recovered.fail_after_root_intent = true;
        assert!(
            recovered
                .session_start(json!({"session_id":"session","cwd":second}))
                .await
                .is_err()
        );
        drop(recovered);
        let mut recovered = ControlPlane::open(fixture.data.clone()).await.unwrap();
        assert_eq!(recovered.state.roots.len(), 2);
        assert!(recovered.state.pending_root_adoptions.is_empty());
        let third = fixture.directory("third");
        recovered.fail_after_root_intent = true;
        assert!(
            recovered
                .session_start(json!({"session_id":"session","cwd":third}))
                .await
                .is_err()
        );
        let pending = recovered
            .state
            .pending_root_adoptions
            .first()
            .unwrap()
            .clone();
        recovered
            .state
            .roots
            .get_mut(&pending)
            .unwrap()
            .created_mut()
            .unwrap()
            .native_root_identity = [u8::MAX; 16];
        recovered.persist().unwrap();
        drop(recovered);
        assert!(ControlPlane::open(fixture.data.clone()).await.is_err());
    });
}
