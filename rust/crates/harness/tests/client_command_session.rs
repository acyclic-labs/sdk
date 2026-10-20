use super::*;
use acyclic_harness::{
    OperationId, TaskId,
    client::{CommandFileRoot, CommandSession, CommandTool},
    task_volume::{Component, FileKey, FileValue, Query, TaskVolume},
};
use acyclic_stream::{MemoryStream, StreamPath};
use futures::FutureExt as _;
use proptest::prelude::*;
use std::sync::Arc;
#[path = "client_command_activity.rs"]
mod activity;
#[path = "client_command_session_bounds.rs"]
mod bounds;

fn session_id() -> uuid::Uuid {
    uuid::Uuid::from_bytes([7; 16])
}

async fn volume() -> Result<Arc<TaskVolume<MemoryStream>>> {
    Ok(Arc::new(
        TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("command-session").unwrap(),
        )
        .await
        .map_err(|error| Error::Storage(error.to_string()))?,
    ))
}

async fn publish(volume: &TaskVolume<MemoryStream>, path: &str, bytes: Vec<u8>) -> Result<()> {
    let cut = volume
        .snapshot(&Query::Root, 128)
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
    let key = FileKey::parse(path).map_err(|error| Error::Invalid(error.to_string()))?;
    volume
        .append(cut.generation, &FileValue::new(key, bytes))
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
    Ok(())
}

#[cfg(all(
    feature = "filesystem-local",
    feature = "native-execution",
    feature = "test-support",
    unix
))]
#[tokio::test]
async fn published_shell_request_causes_a_real_tool_effect_and_does_not_replay() -> Result<()> {
    use acyclic_harness::{LocalHarness, model::Model, test_support::ScriptedModelProvider};
    let directory = crate::client_commands::client_local::Directory::new()
        .map_err(|error| Error::Storage(error.to_string()))?;
    std::fs::create_dir(directory.path().join(".git")).unwrap();
    let model = Model::new("scripted".into(), "test".into(), "1".into(), Value::Null)?;
    let provider = Arc::new(ScriptedModelProvider::new([]).with_context_capacity(
        acyclic_harness::context::ModelContextCapacity {
            context_tokens: 32_768,
            output_tokens: Some(4096),
        },
    ));
    let local = LocalHarness::open(crate::client_commands::client_local::config(
        directory.path(),
        (model, provider.clone()),
    )?)
    .await?;
    let mut client = CommandSession::open_local(&local, session_id(), 32_768, 128).await?;
    client
        .submit("/SH printf hi >> effect.txt; printf hi", &DeniedReader)
        .await?;
    let request = client.next_request().await?;
    let result = acyclic_harness::client::consume_local_command(&mut client, &local, &request)
        .await?
        .unwrap();
    assert_eq!(result["stdout"], "hi");
    assert_eq!(
        std::fs::read(directory.path().join("effect.txt")).unwrap(),
        b"hi"
    );
    assert!(provider.requests().is_empty());
    assert!(
        acyclic_harness::client::consume_local_command(&mut client, &local, &request)
            .await?
            .is_none()
    );
    drop(client);
    let mut reopened = CommandSession::open_local(&local, session_id(), 32_768, 128).await?;
    assert!(
        acyclic_harness::client::consume_local_command(&mut reopened, &local, &request)
            .await?
            .is_none()
    );
    assert_eq!(
        std::fs::read(directory.path().join("effect.txt")).unwrap(),
        b"hi"
    );
    Ok(())
}

#[tokio::test]
async fn catalog_and_external_requests_are_observed_through_the_cursor_follow() -> Result<()> {
    let volume = volume().await?;
    let mut client = CommandSession::open(volume.clone(), session_id(), 32_768, 128).await?;
    let mut definitions: Value =
        serde_json::from_slice(acyclic_harness::client::DEFAULT_COMMANDS_JSON).unwrap();
    let shell = definitions
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|spec| spec["name"] == "shell")
        .unwrap();
    shell["aliases"]
        .as_array_mut()
        .unwrap()
        .push(json!("external"));
    publish(
        &volume,
        "commands/catalog.json",
        serde_json::to_vec(&definitions).unwrap(),
    )
    .await?;
    client.observe_change().await?;
    assert_eq!(
        client.catalog().complete("EXTERNAL").next().unwrap().name,
        "shell"
    );
    publish(
        &volume,
        "commands/themes.json",
        br#"["Solarized","Light"]"#.to_vec(),
    )
    .await?;
    client.observe_change().await?;
    assert_eq!(
        client.catalog().complete_input("/THEME Sol")[0].replacement,
        "/theme Solarized"
    );
    client.submit("/THEME Solarized", &DeniedReader).await?;
    assert_eq!(
        client
            .state(CommandFileRoot::Config, "theme.json")?
            .unwrap()["name"],
        "Solarized"
    );
    let operation = OperationId::new();
    let resolution = CommandResolution::Tool {
        tool: CommandTool::Bash,
        input: json!({"command":"echo hi"}),
    };
    publish(
        &volume,
        &format!(".tasks/{}/requests/{operation}.json", session_id()),
        serde_json::to_vec(&resolution).unwrap(),
    )
    .await?;
    let request = client.next_request().await?;
    let result = client
        .consume(&request, |actual, task, intent| async move {
            assert_eq!(actual, operation);
            assert_eq!(task, Some(TaskId::from_bytes(session_id().into_bytes())));
            assert_eq!(intent, resolution);
            Ok(json!({"called":true}))
        })
        .await?;
    assert_eq!(result, Some(json!({"called":true})));
    Ok(())
}

#[cfg(feature = "tui")]
#[tokio::test]
async fn dispatch_and_input_follow_the_same_catalog_and_theme_files() -> Result<()> {
    use acyclic_harness::{
        tui::{Editor, PromptInput},
        tui_files::CompletionFiles,
    };
    let volume = volume().await?;
    let mut client = CommandSession::open(volume.clone(), session_id(), 32_768, 128).await?;
    let mut input = PromptInput::new(Editor::new("/SH"));
    let mut completions = CompletionFiles::observe(&volume, &mut input, None, 128)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;
    let candidate = input.completion.items().next().unwrap();
    assert_eq!(candidate.value, "shell");
    assert!(candidate.tool);
    assert!(input.completion.accept(&mut input.editor, 0));
    assert_eq!(input.editor.text(), "/shell ");
    publish(
        &volume,
        "commands/themes.json",
        br#"["Solarized Light"]"#.to_vec(),
    )
    .await?;
    client.observe_change().await?;
    completions
        .next(&mut input)
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;
    input.editor.set_text("/THEME sol", 1);
    input.refresh_completions();
    assert!(input.completion.accept(&mut input.editor, 2));
    assert_eq!(input.editor.text(), "/THEME Solarized Light");
    client.submit(input.editor.text(), &DeniedReader).await?;
    assert_eq!(
        client
            .state(CommandFileRoot::Config, "theme.json")?
            .unwrap()["name"],
        "Solarized Light"
    );
    Ok(())
}

#[tokio::test]
async fn cancelling_a_claimed_dispatch_keeps_it_uncertain_without_resending() -> Result<()> {
    let volume = volume().await?;
    let mut client = CommandSession::open(volume.clone(), session_id(), 32_768, 128).await?;
    client.submit("/shell echo hi", &DeniedReader).await?;
    let request = client.next_request().await?;
    let dispatched = std::cell::Cell::new(false);
    assert!(
        client
            .consume(&request, |_, _, _| async {
                dispatched.set(true);
                std::future::pending::<Result<Value>>().await
            })
            .now_or_never()
            .is_none()
    );
    assert!(
        dispatched.get(),
        "negative test must reach the real claimed dispatch"
    );
    drop(client);
    let mut reopened = CommandSession::open(volume, session_id(), 32_768, 128).await?;
    let request = reopened.next_request().await?;
    assert!(matches!(
        reopened
            .consume(&request, |_, _, _| async {
                panic!("uncertain dispatch was resent")
            })
            .await,
        Err(Error::Indeterminate(_))
    ));
    Ok(())
}

#[tokio::test]
async fn a_stale_publisher_never_uses_old_catalog_or_settings() -> Result<()> {
    let volume = volume().await?;
    let mut client = CommandSession::open(volume.clone(), session_id(), 32_768, 128).await?;
    publish(
        &volume,
        ".ui/config/settings.json",
        b"{\"reasoning\":true}".to_vec(),
    )
    .await?;
    assert!(matches!(
        client.submit("/reasoning", &DeniedReader).await,
        Err(Error::Conflict(_))
    ));
    assert_eq!(
        client
            .state(CommandFileRoot::Config, "settings.json")?
            .unwrap()["reasoning"],
        true
    );
    client.submit("/reasoning", &DeniedReader).await?;
    assert_eq!(
        client
            .state(CommandFileRoot::Config, "settings.json")?
            .unwrap()["reasoning"],
        false
    );
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    // Loading absent settings must never move a previously resolved action to
    // a newer catalog cut, for any selected task or command argument.
    #[test]
    fn loading_missing_task_settings_cannot_publish_an_outdated_action(
        task_byte in 1u8..=254, arguments in "[a-zA-Z0-9][a-zA-Z0-9 ]{0,39}"
    ) {
    futures::executor::block_on(async {
    let volume = volume().await?;
    let mut client = CommandSession::open(volume.clone(), session_id(), 32_768, 128).await?;
    client
        .write_state(
            CommandFileRoot::Session,
            "layout.json",
            &json!({"selected_task":TaskId::from_bytes([task_byte;16])}),
        )
        .await?;
    let mut definitions: Vec<Value> =
        serde_json::from_slice(acyclic_harness::client::DEFAULT_COMMANDS_JSON).unwrap();
    definitions
        .iter_mut()
        .find(|spec| spec["name"] == "shell")
        .unwrap()["action"] = json!({"kind":"tool","tool":"web_search"});
    publish(
        &volume,
        "commands/catalog.json",
        serde_json::to_vec(&definitions).unwrap(),
    )
    .await?;
    let input = format!("/shell {arguments}");
    prop_assert!(matches!(client.submit(&input, &DeniedReader).await,
        Err(Error::Conflict(_))));
    prop_assert!(client.take_request().is_none(), "stale action was published");
    client.submit(&input, &DeniedReader).await?;
    let request = client.next_request().await?;
    let resolution: Value = serde_json::from_slice(&request.bytes).unwrap();
    prop_assert_eq!(resolution["tool"].as_str(), Some("web_search"));
    Ok(())
    })?;
    }
}

#[tokio::test]
async fn archive_selection_and_recovery_fold_the_outcome_files() -> Result<()> {
    let volume = volume().await?;
    let mut client = CommandSession::open(volume.clone(), session_id(), 32_768, 128).await?;
    let mut definitions: Vec<Value> =
        serde_json::from_slice(acyclic_harness::client::DEFAULT_COMMANDS_JSON).unwrap();
    definitions.push(json!({"name":"back","description":"Archive","arguments":"text","action":{"kind":"sdk","command":"back"}}));
    client
        .install_catalog(serde_json::to_vec(&definitions).unwrap())
        .await?;
    for index in 0..2 {
        client.submit("/shell echo hi", &DeniedReader).await?;
        let request = client.next_request().await?;
        client
            .consume(&request, |_, _, _| async move {
                Ok(json!({"rendered":format!("page {index}")}))
            })
            .await?;
    }
    client.submit("/back 1", &DeniedReader).await?;
    assert_eq!(
        client
            .state(CommandFileRoot::Session, "layout.json")?
            .unwrap()["history_page"],
        1
    );
    assert_eq!(client.output_page(1)?.unwrap()["ok"]["rendered"], "page 0");
    assert_eq!(client.render_view(&Default::default())?, "page 0");
    client.submit("/back 99", &DeniedReader).await?;
    assert_eq!(
        client.render_view(&Default::default())?,
        "History page 99 is unavailable.\n"
    );
    assert!(client.output_page(usize::MAX)?.is_none());
    drop(client);
    let reopened = CommandSession::open(volume.clone(), session_id(), 32_768, 128).await?;
    assert_eq!(
        reopened.output_page(0)?.unwrap()["ok"]["rendered"],
        "page 1"
    );
    let query = Query::folder(&format!(".sessions/{}", session_id())).unwrap();
    let executions = volume.snapshot(&query, 128).await.unwrap();
    assert_eq!(executions.files.len(), 6);
    assert!(executions.files.iter().all(|file| query.matches(&file.key)));
    Ok(())
}

#[tokio::test]
async fn invalid_catalog_publication_is_retained_until_explicit_snapshot_recovery() -> Result<()> {
    let volume = volume().await?;
    let mut client = CommandSession::open(volume.clone(), session_id(), 32_768, 128).await?;
    publish(&volume, "commands/catalog.json", b"not JSON".to_vec()).await?;
    publish(
        &volume,
        "commands/catalog.json",
        acyclic_harness::client::DEFAULT_COMMANDS_JSON.to_vec(),
    )
    .await?;
    assert!(client.observe_change().await.is_err());
    assert!(
        client.observe_change().await.is_err(),
        "a later publication cannot hide a rejected query cut"
    );
    drop(client);
    let mut reopened = CommandSession::open(volume, session_id(), 32_768, 128).await?;
    reopened.submit("/shell echo hi", &DeniedReader).await?;
    Ok(())
}

#[tokio::test]
async fn unrelated_malformed_ui_file_does_not_block_a_direct_request() -> Result<()> {
    let volume = volume().await?;
    let mut client = CommandSession::open(volume.clone(), session_id(), 32_768, 128).await?;
    publish(&volume, ".ui/config/theme.json", b"not JSON".to_vec()).await?;
    client.observe_change().await?;
    client.submit("/shell echo hi", &DeniedReader).await?;
    assert!(client.take_request().is_some());
    assert!(
        client.submit("/theme light", &DeniedReader).await.is_ok(),
        "unknown theme only opens the picker"
    );
    Ok(())
}

#[tokio::test]
async fn request_target_and_settings_survive_later_navigation() -> Result<()> {
    let volume = volume().await?;
    let mut client = CommandSession::open(volume, session_id(), 32_768, 128).await?;
    let original = TaskId::from_bytes([3; 16]);
    let other = TaskId::from_bytes([4; 16]);
    client
        .write_state(
            CommandFileRoot::Session,
            "layout.json",
            &json!({"selected_task":original}),
        )
        .await?;
    client
        .write_state(
            CommandFileRoot::Task,
            "settings.json",
            &json!({"workspace":"original workspace"}),
        )
        .await?;
    client.submit("/shell echo hi", &DeniedReader).await?;
    let request = client.next_request().await?;
    client
        .write_state(
            CommandFileRoot::Session,
            "layout.json",
            &json!({"selected_task":other}),
        )
        .await?;
    client
        .write_state(
            CommandFileRoot::Task,
            "settings.json",
            &json!({"workspace":"other workspace"}),
        )
        .await?;
    assert_eq!(
        client.request_settings(&request).await?["workspace"],
        "original workspace"
    );
    client
        .consume(&request, |_, target, _| async move {
            assert_eq!(target, Some(original));
            Ok(Value::Null)
        })
        .await?;
    Ok(())
}

#[tokio::test]
async fn session_and_task_requests_cannot_alias_even_with_equal_identifiers() -> Result<()> {
    let volume = volume().await?;
    let mut client = CommandSession::open(volume, session_id(), 32_768, 128).await?;
    client.submit("/shell echo root", &DeniedReader).await?;
    let root = client.next_request().await?;
    assert!(matches!(root.key, FileKey::Session { .. }));
    client
        .consume(&root, |_, target, _| async move {
            assert_eq!(target, None);
            Ok(Value::Null)
        })
        .await?;
    let task = TaskId::from_bytes(session_id().into_bytes());
    client
        .write_state(
            CommandFileRoot::Session,
            "layout.json",
            &json!({"selected_task":task}),
        )
        .await?;
    client.submit("/shell echo task", &DeniedReader).await?;
    let selected = client.next_request().await?;
    assert!(matches!(selected.key, FileKey::Task { .. }));
    client
        .consume(&selected, |_, target, _| async move {
            assert_eq!(target, Some(task));
            Ok(Value::Null)
        })
        .await?;
    Ok(())
}

#[tokio::test]
async fn activity_selection_recovers_from_sdk_files_and_ignores_tool_output_fields() -> Result<()> {
    let volume = volume().await?;
    let mut client = CommandSession::open(volume.clone(), session_id(), 32_768, 128).await?;
    let mut definitions: Vec<Value> =
        serde_json::from_slice(acyclic_harness::client::DEFAULT_COMMANDS_JSON).unwrap();
    definitions.push(json!({"name":"activity","description":"Activity","arguments":"text","action":{"kind":"sdk","command":"next"}}));
    client
        .install_catalog(serde_json::to_vec(&definitions).unwrap())
        .await?;
    client.submit("/activity", &DeniedReader).await?;
    let request = client.next_request().await?;
    client
        .consume(&request, |_, _, _| async {
            Ok(json!({"activity_cursor":"retained SDK cursor"}))
        })
        .await?;
    client.submit("/shell echo hi", &DeniedReader).await?;
    let request = client.next_request().await?;
    client
        .consume(&request, |_, _, _| async {
            Ok(json!({"activity_cursor":"tool-controlled text"}))
        })
        .await?;
    assert_eq!(client.activity_cursor(), Some("retained SDK cursor"));
    drop(client);
    let reopened = CommandSession::open(volume, session_id(), 32_768, 128).await?;
    assert_eq!(reopened.activity_cursor(), Some("retained SDK cursor"));
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    // Every intervening publication may force an outcome retry, but the actual
    // effect is FnOnce and its immutable claim is never executed a second time.
    #[test]
    fn competing_publications_only_retry_the_outcome(count in 0_usize..8) {
        futures::executor::block_on(async {
            let volume = volume().await.unwrap();
            let mut client = CommandSession::open(volume.clone(), session_id(), 32_768, 128).await.unwrap();
            client.submit("/shell echo hi", &DeniedReader).await.unwrap();
            let request = client.next_request().await.unwrap();
            let dispatches = std::cell::Cell::new(0);
            let result = client.consume(&request, |_, _, _| async {
                dispatches.set(dispatches.get()+1);
                for index in 0..count { publish(&volume, ".ui/config/other.json", serde_json::to_vec(&index).unwrap()).await?; }
                Ok(json!({"value":count}))
            }).await.unwrap().unwrap();
            prop_assert_eq!(dispatches.get(), 1);
            prop_assert_eq!(&client.output_page(0).unwrap().unwrap()["ok"], &result);
            let cut = volume.snapshot(&Query::Root, 128).await.unwrap();
            prop_assert_eq!(client.output_page(0).unwrap().unwrap()["sequence"].as_u64(), Some(cut.generation.position - 1));
            Ok(())
        })?;
    }

    // Dispatch authority is the exact observed immutable file, including its
    // bytes and metadata. A caller cannot replace it with a fabricated request.
    #[test]
    fn fabricated_request_cannot_reach_dispatch(text in "[a-zA-Z0-9]{1,40}") {
        futures::executor::block_on(async {
            let volume = volume().await.unwrap();
            let mut client = CommandSession::open(volume, session_id(), 32_768, 128).await.unwrap();
            client.submit("/shell echo original", &DeniedReader).await.unwrap();
            let mut fabricated = client.next_request().await.unwrap();
            fabricated.bytes = serde_json::to_vec(&json!({"kind":"tool","tool":"bash","input":{"command":text}})).unwrap().into();
            let result = client.consume(&fabricated, |_, _, _| async { panic!("fabricated input reached dispatch") }).await;
            prop_assert!(matches!(result, Err(Error::Unauthorized(_))));
            Ok(())
        })?;
    }
    // The new ordinary UI file namespace has one canonical spelling, retains
    // its caller owner through JSON, and rejects path aliases/traversal.
    #[test]
    fn ui_file_keys_round_trip(directory in "[a-zA-Z0-9_-]{1,48}", name in "[a-zA-Z0-9_-]{1,48}") {
        let key = FileKey::Ui { directory: Component::new(&directory).unwrap(), name: Component::new(format!("{name}.json")).unwrap() };
        prop_assert_eq!(FileKey::parse(&key.path()).unwrap(), key.clone());
        prop_assert_eq!(serde_json::from_slice::<FileKey>(&serde_json::to_vec(&key).unwrap()).unwrap(), key.clone());
        prop_assert_eq!(key.owner(), acyclic_harness::task_volume::Owner::Caller);
        for file in [acyclic_harness::task_volume::TaskFile::ExecutionClaim(Component::new(&name).unwrap()), acyclic_harness::task_volume::TaskFile::ExecutionResult(Component::new(&name).unwrap())] {
            let key = FileKey::Task { task:Component::new(&directory).unwrap(),file };
            prop_assert_eq!(FileKey::parse(&key.path()).unwrap(), key.clone());
            prop_assert_eq!(serde_json::from_slice::<FileKey>(&serde_json::to_vec(&key).unwrap()).unwrap(), key.clone());
            prop_assert_eq!(key.owner(), acyclic_harness::task_volume::Owner::Harness);
            let folder = format!(".tasks/{directory}/executions");
            prop_assert!(Query::folder(&folder).unwrap().matches(&key));
        }
        for bad in [format!(".ui/../{name}"), format!(".ui/{directory}//{name}"), format!(".ui/{directory}/{name}/.."), format!(".ui/{directory}/.")] {
            prop_assert!(FileKey::parse(&bad).is_err());
        }
    }
}

#[tokio::test]
async fn ordinary_search_requests_never_enter_the_command_inbox() -> Result<()> {
    let volume = volume().await?;
    let file = FileValue::new(
        FileKey::parse(".tasks/00000000-0000-0000-0000-000000000001/requests/search-native.json")
            .map_err(volume_error)?,
        b"{\"query\":\"native search\"}".to_vec(),
    );
    let cut = volume
        .snapshot(&Query::File(file.key.clone()), 1)
        .await
        .map_err(volume_error)?
        .generation;
    volume.append(cut, &file).await.map_err(volume_error)?;
    let mut client = CommandSession::open(volume.clone(), session_id(), 32_768, 128).await?;
    assert!(client.take_request().is_none());
    let file = FileValue::new(
        FileKey::parse(".tasks/00000000-0000-0000-0000-000000000001/requests/search-followed.json")
            .map_err(volume_error)?,
        b"{}".to_vec(),
    );
    let cut = volume
        .snapshot(&Query::File(file.key.clone()), 1)
        .await
        .map_err(volume_error)?
        .generation;
    volume.append(cut, &file).await.map_err(volume_error)?;
    client.refresh_files().await?;
    assert!(client.take_request().is_none());
    Ok(())
}

proptest! {
    #[test]
    fn submitting_session_owns_the_request_and_its_selection(bytes in any::<[u8;16]>()) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let volume=volume().await.unwrap();
            let session=uuid::Uuid::from_bytes(bytes);
            let mut other_bytes=bytes; other_bytes[0] ^= 1;
            let other=uuid::Uuid::from_bytes(other_bytes);
            let mut author=CommandSession::open(volume.clone(),session,32_768,128).await.unwrap();
            let mut observer=CommandSession::open(volume.clone(),other,32_768,128).await.unwrap();
            let parent=TaskId::from_bytes([3;16]); let child=TaskId::from_bytes([4;16]);
            author.refresh_files().await.unwrap();
            author.write_state(CommandFileRoot::Session,"layout.json",&json!({"selected_task":parent})).await.unwrap();
            observer.refresh_files().await.unwrap();
            observer.write_state(CommandFileRoot::Session,"layout.json",&json!({"selected_task":parent})).await.unwrap();
            author.refresh_files().await.unwrap();
            let operation=author.submit("child prompt",&DeniedReader).await.unwrap();
            let request=author.take_request().unwrap();
            prop_assert_eq!(FileKey::parse(&request.key.path()).unwrap(),request.key.clone());
            observer.refresh_files().await.unwrap();
            prop_assert!(observer.take_request().is_none());
            prop_assert!(observer.consume(&request,|_,_,_| async { panic!("foreign request dispatched") }).await.is_err(), "foreign command requests must be rejected");
            author.refresh_files().await.unwrap();
            author.consume(&request,|id,target,_| async move {
                assert_eq!(id,operation); assert_eq!(target,Some(parent)); Ok(json!({"selected_task":child}))
            }).await.unwrap();
            observer.refresh_files().await.unwrap();
            prop_assert_eq!(author.selected_task().unwrap(),Some(child));
            prop_assert_eq!(observer.selected_task().unwrap(),Some(parent));
            drop(author); drop(observer);
            prop_assert_eq!(CommandSession::open(volume.clone(),session,32_768,128).await.unwrap().selected_task().unwrap(),Some(child));
            prop_assert_eq!(CommandSession::open(volume,other,32_768,128).await.unwrap().selected_task().unwrap(),Some(parent));
            Ok(())
        })?;
    }
}
