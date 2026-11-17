use super::*;

#[tokio::test]
async fn stop_checks_writes_from_settlement_continuations() -> Result<()> {
    for (repair, candidate_reply) in [(true, true), (false, true), (false, false)] {
        let error = |error: std::io::Error| Error::Storage(error.to_string());
        let directory = tempfile::tempdir().map_err(error)?;
        let home = directory.path().join("home");
        let workspace = directory.path().join("workspace");
        std::fs::create_dir_all(home.join(".claude")).map_err(error)?;
        std::fs::create_dir_all(&workspace).map_err(error)?;
        #[cfg(windows)]
        let (guard, shell) = (
            r#"more >nul & echo stop>>stop-observations.txt & if exist needs-review.txt if not exist reviewed.txt (echo Review the settlement write 1>&2 & exit /b 2)"#,
            "cmd",
        );
        #[cfg(not(windows))]
        let (guard, shell) = (
            r#"cat >/dev/null; printf 'stop\n' >>stop-observations.txt; if test -f needs-review.txt && ! test -f reviewed.txt; then printf 'Review the settlement write\n' >&2; exit 2; fi"#,
            "sh",
        );
        std::fs::write(
            home.join(".claude/settings.json"),
            serde_json::to_vec(&json!({"hooks":{"Stop":[{"hooks":[{
                "type":"command", "command":guard, "shell":shell
            }]}]}}))
            .map_err(|error| Error::Storage(error.to_string()))?,
        )
        .map_err(error)?;
        let write = |id: &str, path: &str| {
            ScriptedTurn::Events(vec![
                crate::test_support::ScriptedEvent::Event(crate::model::ModelEvent::ToolCall(
                    crate::model::ToolCall::new(
                        id.into(),
                        "write".into(),
                        json!({"path":path,"content":"written"}),
                    ),
                )),
                crate::test_support::ScriptedEvent::Event(crate::model::ModelEvent::Completed {
                    metadata: json!({}),
                }),
            ])
        };
        let mut turns = vec![
            ScriptedTurn::Text("task complete".into()),
            write("settlement-write", "needs-review.txt"),
        ];
        if candidate_reply {
            turns.push(ScriptedTurn::Text("settlement complete".into()));
        }
        if repair {
            turns.extend([
                write("review-write", "reviewed.txt"),
                ScriptedTurn::Text("review complete".into()),
            ]);
        } else {
            turns.push(ScriptedTurn::Events(vec![
                crate::test_support::ScriptedEvent::Interrupted {
                    error: "response unavailable".into(),
                },
            ]));
        }
        let provider = scripted(turns);
        let harness = Arc::new(
            LocalHarness::open(
                checkout_config(&workspace, (model()?, provider.clone()))?
                    .workspace_mounting(crate::local::WorkspaceMounting::Physical)
                    .settings(
                        crate::native_settings::SettingsConfig::new(&workspace, &home)
                            .codex_home(home.join(".codex")),
                    ),
            )
            .await?,
        );
        let mut observed = ObservedConversation::open(&harness).await?;
        let entry = observed.send("task", "Finish the task").await?;
        if !repair {
            assert!(matches!(&entry.outcome,
                crate::agent::TurnOutcome::Failed { message }
                    if message.contains("response unavailable")
            ));
            assert_eq!(
                provider.request_count(),
                if candidate_reply { 4 } else { 3 }
            );
            continue;
        }
        assert_eq!(entry.outcome, crate::agent::TurnOutcome::Completed);
        assert_eq!(
            observed
                .conversation
                .output_text_at(observed.observer.seen() - 1, &entry)
                .await?,
            "task complete"
        );
        assert_eq!(provider.request_count(), 5);
        assert!(
            request_text(&harness, &provider.requests()[3])
                .await?
                .contains("Review the settlement write")
        );
        let checkout = &harness.workspace.as_ref().expect("physical workspace").path;
        assert_eq!(
            std::fs::read_to_string(checkout.join("stop-observations.txt"))
                .map_err(error)?
                .lines()
                .collect::<Vec<_>>(),
            vec!["stop", "stop", "stop"]
        );
        assert_eq!(
            std::fs::read_to_string(checkout.join("reviewed.txt")).map_err(error)?,
            "written"
        );
    }
    Ok(())
}
