use super::*;
use crate::client::CommandResolution;

#[tokio::test]
async fn user_templates_are_captured_from_declared_roots_and_project_overrides() -> Result<()> {
    let checkout = tempfile::tempdir().map_err(super::super::storage)?;
    let home = tempfile::tempdir().map_err(super::super::storage)?;
    let codex = tempfile::tempdir().map_err(super::super::storage)?;
    std::fs::create_dir_all(home.path().join(".claude/commands")).map_err(super::super::storage)?;
    std::fs::create_dir_all(codex.path().join("prompts")).map_err(super::super::storage)?;
    std::fs::create_dir_all(checkout.path().join(".claude/commands"))
        .map_err(super::super::storage)?;
    for (path, body) in [
        (
            home.path().join(".claude/commands/user.md"),
            "User $ARGUMENTS",
        ),
        (codex.path().join("prompts/codex.md"), "Codex $ARGUMENTS"),
        (
            home.path().join(".claude/commands/shared.md"),
            "User shared",
        ),
        (
            checkout.path().join(".claude/commands/shared.md"),
            "Project shared",
        ),
    ] {
        std::fs::write(path, body).map_err(super::super::storage)?;
    }
    // A non-prompt sibling is deliberately malformed and must never be read.
    std::fs::write(codex.path().join("auth.json"), b"not configuration")
        .map_err(super::super::storage)?;
    let provider = text();
    let harness = LocalHarness::open(
        checkout_config(checkout.path(), (model()?, provider.clone()))?.settings(
            crate::native_settings::SettingsConfig::new(checkout.path(), home.path())
                .codex_home(codex.path()),
        ),
    )
    .await?;
    let operation = OperationId::new();
    let first = harness.resolve_input(operation, "/user value").await?;
    assert!(matches!(&first, CommandResolution::Prompt { text, .. } if text == "User value"));
    std::fs::write(home.path().join(".claude/commands/user.md"), "Changed")
        .map_err(super::super::storage)?;
    assert_eq!(
        harness.resolve_input(operation, "/user value").await?,
        first
    );
    for (input, expected) in [
        ("/codex value", "Codex value"),
        ("/shared", "Project shared"),
    ] {
        assert!(
            matches!(harness.resolve_input(OperationId::new(), input).await?, CommandResolution::Prompt { text, .. } if text == expected)
        );
    }
    std::fs::write(codex.path().join("prompts/user.md"), "Ambiguous")
        .map_err(super::super::storage)?;
    assert!(matches!(
        harness
            .resolve_input(OperationId::new(), "/user value")
            .await,
        Err(Error::Conflict(_))
    ));
    assert!(provider.requests().is_empty());
    harness.disconnect().await
}
