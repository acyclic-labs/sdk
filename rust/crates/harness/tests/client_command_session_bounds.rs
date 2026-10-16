use super::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    #[test]
    fn command_queries_and_ui_paths_round_trip(seed in any::<u128>(), recent in any::<u32>(), directory in "[a-zA-Z0-9_-]{1,40}") {
        let query = Query::CommandSession { session: Component::new(uuid::Uuid::from_u128(seed).to_string()).unwrap(), recent };
        prop_assert_eq!(serde_json::from_slice::<Query>(&serde_json::to_vec(&query).unwrap()).unwrap(), query);
        let path = format!(".ui/{directory}");
        prop_assert_eq!(Query::folder(&path).unwrap(), Query::UiDirectory(Component::new(directory).unwrap()));
    }

    // Key/query module splits preserve config inputs and every valid plan name.
    #[test]
    fn config_and_plan_queries_preserve_canonical_file_names(
        stem in prop_oneof!["[a-z]{1,16}", Just("release notes".to_owned()), Just("\u{8abf}\u{67fb}".to_owned()), Just("\u{10ffff}tail".to_owned())],
        value in any::<u32>(),
    ) {
        futures::executor::block_on(async {
            let volume = volume().await.unwrap();
            let plan = FileKey::parse(&format!("plans/{stem}.md")).unwrap();
            let config = FileKey::parse("config/settings.json").unwrap();
            let mut cut = volume.snapshot(&Query::Root, 0).await.unwrap().generation;
            for key in [config.clone(), plan.clone(), FileKey::Goal] {
                cut = volume.append(cut, &FileValue::new(key, value.to_string())).await.unwrap();
            }
            for (query, expected) in [(Query::folder("config").unwrap(), config), (Query::Plans, plan)] {
                prop_assert_eq!(serde_json::from_slice::<Query>(&serde_json::to_vec(&query).unwrap()).unwrap(), query.clone());
                let files = volume.snapshot(&query, 1).await.unwrap().files;
                prop_assert_eq!(files.len(), 1);
                prop_assert_eq!(&files[0].key, &expected);
            }
            Ok(())
        })?;
    }

    // Followed command and skill inputs keep their exact canonical path through
    // the production typed and JSON codecs.
    #[test]
    fn command_source_paths_round_trip(name in "[a-z]{1,24}", root in 0_usize..3) {
        let path = match root {
            0 => format!(".claude/commands/{name}.md"),
            1 => format!(".codex/prompts/{name}.md"),
            _ => format!("skills/{name}/SKILL.md"),
        };
        let key = FileKey::parse(&path).unwrap();
        prop_assert!(Query::Inputs.matches(&key));
        prop_assert_eq!(key.path(), path);
        prop_assert_eq!(serde_json::from_slice::<FileKey>(&serde_json::to_vec(&key).unwrap()).unwrap(), key);
    }

    // The active bound is independent of completed history length. Historical
    // effects/results remain addressable, and reopening never resends an effect.
    #[test]
    fn completed_history_does_not_fill_the_active_view(count in 8_usize..64) {
        futures::executor::block_on(async {
            let volume = volume().await.unwrap();
            let mut client = CommandSession::open(volume.clone(), session_id(), 32_768, 16).await.unwrap();
            let mut first = None;
            for n in 0..count {
                client.submit("/shell hi", &DeniedReader).await.unwrap();
                let request = client.next_request().await.unwrap();
                if first.is_none() { first = Some(request.clone()); }
                client.consume(&request, |_, _, _| async move { Ok(json!({"index":n})) }).await.unwrap();
                prop_assert!(client.take_request().is_none());
            }
            let query = Query::CommandSession { session:Component::new(session_id().to_string()).unwrap(), recent:4 };
            let selected = volume.snapshot(&query, 16).await.unwrap();
            prop_assert!(selected.files.len() <= 16);
            let all = volume.snapshot(&Query::Root, count*3+4).await.unwrap();
            prop_assert_eq!(all.files.len(), count*3+4);
            let mut reopened = CommandSession::open(volume, session_id(), 32_768, 16).await.unwrap();
            prop_assert!(reopened.take_request().is_none());
            prop_assert_eq!(reopened.output_page(0).unwrap().unwrap()["ok"]["index"].as_u64(), Some(count as u64 - 1));
            let replay = reopened.consume(&first.unwrap(), |_, _, _| async { panic!("archived effect resent") }).await.unwrap();
            prop_assert!(replay.is_none());
            Ok(())
        })?;
    }

    // The encoded bound includes wrapper fields and worst-case JSON escaping.
    // Oversize output still has a durable terminal record and cannot replay.
    #[test]
    fn completed_results_always_have_a_bounded_terminal_record(count in 0_usize..65_536, ch in prop_oneof![Just('\0'), Just('"'), Just('x')]) {
        futures::executor::block_on(async {
            let volume = volume().await.unwrap();
            let mut client = CommandSession::open(volume.clone(), session_id(), 8192, 16).await.unwrap();
            client.submit("/shell hi", &DeniedReader).await.unwrap();
            let request = client.next_request().await.unwrap();
            client.consume(&request, |_, _, _| async move { Ok(json!({"stdout":ch.to_string().repeat(count)})) }).await.unwrap();
            let snapshot = volume.snapshot(&Query::Root, 16).await.unwrap();
            let result = snapshot.files.iter().find(|file| file.key.path().ends_with("/result.json")).unwrap();
            prop_assert!(result.bytes.len() <= 8192);
            let saved: Value = serde_json::from_slice(&result.bytes).unwrap();
            prop_assert!(saved.get("ok").is_some() || saved["completed"] == true && saved["output_omitted"] == true);
            let mut reopened = CommandSession::open(volume, session_id(), 8192, 16).await.unwrap();
            let replay = reopened.consume(&request, |_, _, _| async { panic!("completed output overflow resent effect") }).await.unwrap();
            prop_assert!(replay.is_none());
            Ok(())
        })?;
    }

    #[test]
    fn shared_settings_are_inherited_and_task_overrides_stay_bound(index in 0_usize..3, override_workspace in any::<bool>(), value in any::<u32>()) {
        futures::executor::block_on(async {
            let volume = volume().await.unwrap();
            let mut client = CommandSession::open(volume, session_id(), 32_768, 32).await.unwrap();
            client.write_state(CommandFileRoot::Config, "settings.json", &json!({"workspace":"/shared","search_endpoint":"https://search.invalid","retained":value})).await.unwrap();
            client.write_state(CommandFileRoot::Session, "layout.json", &json!({"selected_task":TaskId::from_bytes([3;16])})).await.unwrap();
            let task = if override_workspace {json!({"workspace":"/task","agent_mode":"code"})} else {json!({"agent_mode":"code"})};
            client.write_state(CommandFileRoot::Task, "settings.json", &task).await.unwrap();
            client.submit(["/shell hi","/fetch https://example.invalid","/search hi"][index], &DeniedReader).await.unwrap();
            let request = client.next_request().await.unwrap();
            client.write_state(CommandFileRoot::Session, "layout.json", &json!({"selected_task":null})).await.unwrap();
            let settings = client.request_settings(&request).await.unwrap();
            prop_assert_eq!(settings["workspace"].as_str(), Some(if override_workspace {"/task"} else {"/shared"}));
            prop_assert_eq!(settings["search_endpoint"].as_str(), Some("https://search.invalid"));
            prop_assert_eq!(settings["retained"].as_u64(), Some(value as u64));
            prop_assert_eq!(settings["agent_mode"].as_str(), Some("code"));
            Ok(())
        })?;
    }

    // UUID-selected UI files remain separate even in one shared task volume.
    #[test]
    fn independent_ui_sessions_keep_layout_and_exit_separate(seed in 1_u128..u128::MAX) {
        futures::executor::block_on(async {
            let volume = volume().await.unwrap();
            let mut left = CommandSession::open(volume.clone(), uuid::Uuid::from_u128(seed), 32_768, 32).await.unwrap();
            let right = CommandSession::open(volume.clone(), uuid::Uuid::from_u128(seed ^ 1), 32_768, 32).await.unwrap();
            left.submit("/quit", &DeniedReader).await.unwrap_err(); // stale cut follows right's bootstrap before retry
            left.submit("/quit", &DeniedReader).await.unwrap();
            prop_assert!(left.exit_requested().unwrap());
            prop_assert!(!right.exit_requested().unwrap());
            let reopened = CommandSession::open(volume, uuid::Uuid::from_u128(seed ^ 1), 32_768, 32).await.unwrap();
            prop_assert!(!reopened.exit_requested().unwrap());
            Ok(())
        })?;
    }
}
