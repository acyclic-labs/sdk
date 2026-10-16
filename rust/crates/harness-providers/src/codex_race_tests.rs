use super::*;
use std::collections::VecDeque;

#[test]
fn cli_write_after_staging_before_final_comparison_is_adopted() -> Result<()> {
    for account_switch in [false, true] {
        let (root, login, _) = fixture(&jwt(0), 200)?;
        let path = root.path().join("auth.json");
        let (original, _) = load(&path, &mut CredentialIo::new(None)?)?;
        let mut cli = auth("cli-access");
        cli["tokens"]["refresh_token"] = json!("cli-rotated");
        if account_switch {
            cli["tokens"]["account_id"] = json!("other-account");
        }
        let cli_bytes = serde_json::to_vec(&cli).expect("fixture encoding");
        login.check_account(&auth("original"))?;
        let adopted = persist_with(
            &path,
            &original,
            auth("provider-access"),
            || {
                // The initial comparison has succeeded, but the final re-read is still ahead.
                let staged: Vec<_> = std::fs::read_dir(root.path())
                    .expect("fixture directory")
                    .map(|entry| entry.expect("fixture entry").path())
                    .filter(|entry| entry != &path)
                    .collect();
                assert_eq!(staged.len(), 1, "stage inside the auth directory");
                assert_eq!(staged[0].parent(), path.parent());
                std::fs::write(&path, &cli_bytes).map_err(|_| invalid("fixture write"))
            },
            &mut CredentialIo::new(None)?,
        )?;
        assert_eq!(adopted, cli);
        assert_eq!(std::fs::read(&path).expect("fixture read"), cli_bytes);
        assert_eq!(login.check_account(&adopted).is_err(), account_switch);
        assert_eq!(
            std::fs::read_dir(root.path())
                .expect("fixture directory")
                .count(),
            1
        );
    }
    Ok(())
}

struct RacingCli {
    path: PathBuf,
    cli_bytes: Vec<u8>,
    replies: Mutex<VecDeque<(u16, Value)>>,
    requests: Mutex<Vec<HttpRequest>>,
}

impl HttpTransport for RacingCli {
    fn send(&self, request: HttpRequest) -> BoxProviderFuture<'_, Result<HttpResponse>> {
        Box::pin(async move {
            let first = {
                let mut requests = self.requests.lock().map_err(|_| invalid("fixture lock"))?;
                requests.push(request);
                requests.len() == 1
            };
            if first {
                // The real CLI does not participate in this provider's in-memory mutex.
                // Publish a complete credential document while independent readers run.
                let mut update = tempfile::NamedTempFile::new_in(self.path.parent().unwrap())
                    .map_err(|_| invalid("fixture staging"))?;
                update
                    .write_all(&self.cli_bytes)
                    .map_err(|_| invalid("fixture write"))?;
                update
                    .persist(&self.path)
                    .map_err(|_| invalid("fixture replace"))?;
            }
            let (status, value) = self
                .replies
                .lock()
                .map_err(|_| invalid("fixture lock"))?
                .pop_front()
                .ok_or_else(|| invalid("unexpected refresh"))?;
            let bytes = Bytes::from(value.to_string());
            Ok(HttpResponse {
                status,
                body: Box::pin(futures::stream::once(async { Ok(bytes) })),
            })
        })
    }
}

fn race(path: &Path, cli: Value, replies: Vec<(u16, Value)>) -> RacingCli {
    RacingCli {
        path: path.to_owned(),
        cli_bytes: format!("{cli}\n").into_bytes(),
        replies: Mutex::new(replies.into()),
        requests: Mutex::new(Vec::new()),
    }
}

#[tokio::test]
async fn cli_write_during_successful_refresh_wins_without_extra_auth_directory_files() -> Result<()>
{
    let (root, login, _) = fixture(&jwt(0), 200)?;
    let cli_access = jwt(crate::codex::fixed_now().unix_timestamp() + 7200);
    let mut cli = auth(&cli_access);
    cli["tokens"]["refresh_token"] = json!("cli-refresh");
    cli["unrelated_cli_field"] = json!([1, 2, 3]);
    let transport = race(
        &root.path().join("auth.json"),
        cli,
        vec![(
            200,
            json!({"access_token":"discard-provider-access", "refresh_token":"discard-provider-refresh"}),
        )],
    );
    assert_eq!(login.bearer(&transport, None, None).await?.0, cli_access);
    assert_eq!(
        std::fs::read(&transport.path).map_err(|_| invalid("fixture read"))?,
        transport.cli_bytes
    );
    assert_eq!(
        std::fs::read_dir(root.path())
            .map_err(|_| invalid("fixture directory"))?
            .count(),
        1
    );
    Ok(())
}

#[tokio::test]
async fn rejected_refresh_reloads_a_fresh_cli_bearer_once() -> Result<()> {
    for status in [400, 401] {
        let (root, login, _) = fixture(&jwt(0), 200)?;
        let cli_access = jwt(crate::codex::fixed_now().unix_timestamp() + 7200);
        let transport = race(
            &root.path().join("auth.json"),
            auth(&cli_access),
            vec![(status, json!({"error":"invalid_grant"}))],
        );
        assert_eq!(login.bearer(&transport, None, None).await?.0, cli_access);
        assert_eq!(
            transport
                .requests
                .lock()
                .map_err(|_| invalid("fixture lock"))?
                .len(),
            1
        );
        assert_eq!(
            std::fs::read(&transport.path).map_err(|_| invalid("fixture read"))?,
            transport.cli_bytes
        );
    }
    Ok(())
}

#[tokio::test]
async fn rejected_refresh_retries_the_changed_on_disk_refresh_token() -> Result<()> {
    for status in [400, 401] {
        let (root, login, _) = fixture(&jwt(0), 200)?;
        let mut cli = auth(&jwt(0));
        cli["tokens"]["refresh_token"] = json!("rotated-by-cli");
        let transport = race(
            &root.path().join("auth.json"),
            cli,
            vec![
                (status, json!({"error":"invalid_grant"})),
                (
                    200,
                    json!({"access_token":"updated", "refresh_token":"rotated-again"}),
                ),
            ],
        );
        assert_eq!(login.bearer(&transport, None, None).await?.0, "updated");
        let requests = transport
            .requests
            .lock()
            .map_err(|_| invalid("fixture lock"))?;
        assert_eq!(requests.len(), 2);
        assert_eq!(
            crate::json(&requests[1].body)?["refresh_token"],
            "rotated-by-cli"
        );
    }
    Ok(())
}

#[tokio::test]
async fn cli_account_changes_during_refresh_are_rejected_without_overwriting_the_cli() -> Result<()>
{
    for status in [200, 400, 401] {
        let (root, login, _) = fixture(&jwt(0), 200)?;
        let mut cli = auth(&jwt(crate::codex::fixed_now().unix_timestamp() + 7200));
        cli["tokens"]["account_id"] = json!("another-account");
        let transport = race(
            &root.path().join("auth.json"),
            cli,
            vec![(status, json!({"access_token":"provider-token"}))],
        );
        assert!(matches!(
            login.bearer(&transport, None, None).await,
            Err(acyclic_harness::Error::Unauthorized(_))
        ));
        assert_eq!(
            std::fs::read(&transport.path).map_err(|_| invalid("fixture read"))?,
            transport.cli_bytes
        );
    }
    Ok(())
}

#[tokio::test]
async fn independent_login_instances_adopt_cli_credentials_published_during_refresh() -> Result<()>
{
    let (root, first, _) = fixture(&jwt(0), 200)?;
    let second =
        CodexLogin::from_path(root.path().join("auth.json")).with_clock(crate::codex::fixed_now);
    let cli_access = jwt(crate::codex::fixed_now().unix_timestamp() + 7200);
    let transport = race(
        &root.path().join("auth.json"),
        auth(&cli_access),
        vec![
            (200, json!({"access_token":"discard-first"})),
            (200, json!({"access_token":"discard-second"})),
        ],
    );
    let (left, right) = futures::join!(
        first.bearer(&transport, None, None),
        second.bearer(&transport, None, None)
    );
    assert_eq!(left?.0, cli_access);
    assert_eq!(right?.0, cli_access);
    assert_eq!(
        std::fs::read(&transport.path).map_err(|_| invalid("fixture read"))?,
        transport.cli_bytes
    );
    Ok(())
}

#[tokio::test]
async fn oauth_transient_statuses_keep_the_retryable_classification() -> Result<()> {
    for status in [429, 500, 503] {
        let (_root, login, transport) = fixture(&jwt(0), status)?;
        let error = login
            .bearer(&transport, None, None)
            .await
            .expect_err("transient failure");
        assert!(error.to_string().contains(&status.to_string()));
        assert!(matches!(error, acyclic_harness::Error::Storage(_)));
    }
    Ok(())
}

#[tokio::test]
async fn metadata_only_cli_edit_keeps_rotated_tokens_and_unrelated_fields() -> Result<()> {
    let (root, login, _) = fixture(&jwt(0), 200)?;
    let mut cli = auth(&jwt(0));
    cli["agent_identity"] = json!({"new":"metadata"});
    cli["tokens"]["cli_metadata"] = json!(["keep", "λ"]);
    let transport = race(
        &root.path().join("auth.json"),
        cli,
        vec![(
            200,
            json!({"access_token":"new-access", "refresh_token":"single-use-rotated", "id_token":"new-id"}),
        )],
    );
    assert_eq!(login.bearer(&transport, None, None).await?.0, "new-access");
    let stored = decode_auth(&std::fs::read(&transport.path).unwrap())?;
    assert_eq!(stored["tokens"]["refresh_token"], "single-use-rotated");
    assert_eq!(stored["tokens"]["id_token"], "new-id");
    assert_eq!(stored["tokens"]["cli_metadata"], json!(["keep", "λ"]));
    assert_eq!(stored["agent_identity"], json!({"new":"metadata"}));
    Ok(())
}

#[test]
fn metadata_only_edit_after_staging_is_merged_into_rotated_credentials() -> Result<()> {
    let (root, _, _) = fixture(&jwt(0), 200)?;
    let path = root.path().join("auth.json");
    let original = std::fs::read(&path).unwrap();
    let mut refreshed = auth("rotated-access");
    refreshed["tokens"]["refresh_token"] = json!("rotated-refresh");
    let mut cli = auth(&jwt(0));
    cli["agent_identity"] = json!({"metadata":"after-staging"});
    let merged = persist_with(
        &path,
        &original,
        refreshed,
        || {
            std::fs::write(&path, serde_json::to_vec_pretty(&cli).unwrap())
                .map_err(|_| invalid("fixture write"))
        },
        &mut CredentialIo::new(None)?,
    )?;
    assert_eq!(merged["tokens"]["access_token"], "rotated-access");
    assert_eq!(merged["tokens"]["refresh_token"], "rotated-refresh");
    assert_eq!(merged["agent_identity"], cli["agent_identity"]);
    assert_eq!(decode_auth(&std::fs::read(path).unwrap())?, merged);
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn metadata_merges_preserve_new_tokens_but_credential_changes_take_ownership(
        metadata in ".{0,96}", access in ".{0,96}", refresh in ".{0,96}",
        changed in prop::option::of(0usize..6),
    ) {
        let original = auth("original");
        let mut current = original.clone();
        current["agent_identity"] = json!({"arbitrary":metadata});
        current["tokens"]["extra"] = json!(metadata);
        if let Some(field) = changed {
            let path = [
                vec!["tokens", "account_id"], vec!["tokens", "access_token"],
                vec!["tokens", "refresh_token"], vec!["tokens", "id_token"],
                vec!["auth_mode"], vec!["OPENAI_API_KEY"],
            ].into_iter().nth(field).unwrap();
            if path.len() == 2 { current[path[0]][path[1]] = json!("different"); }
            else { current[path[0]] = json!("different"); }
        }
        let mut refreshed = original.clone();
        refreshed["tokens"]["access_token"] = json!(access);
        refreshed["tokens"]["refresh_token"] = json!(refresh);
        let merged = merge_refresh(&original, &serde_json::to_vec(&current).unwrap(), &current, &refreshed, &mut CredentialIo::new(None).unwrap()).unwrap();
        prop_assert_eq!(merged.is_none(), changed.is_some());
        if let Some(merged) = merged {
            prop_assert_eq!(&merged["agent_identity"], &current["agent_identity"]);
            prop_assert_eq!(&merged["tokens"]["extra"], &current["tokens"]["extra"]);
            prop_assert_eq!(&merged["tokens"]["access_token"], &refreshed["tokens"]["access_token"]);
            prop_assert_eq!(&merged["tokens"]["refresh_token"], &refreshed["tokens"]["refresh_token"]);
        }
    }
}

#[test]
fn explicit_chatgpt_mode_is_metadata_when_the_original_mode_was_implicit() -> Result<()> {
    let mut original = auth("original");
    original.as_object_mut().unwrap().remove("auth_mode");
    let current = auth("original");
    let mut refreshed = original.clone();
    refreshed["tokens"]["refresh_token"] = json!("rotated-single-use");
    let merged = merge_refresh(
        &original,
        &serde_json::to_vec(&current).unwrap(),
        &current,
        &refreshed,
        &mut CredentialIo::new(None)?,
    )?
    .expect("same credential identity");
    assert_eq!(merged["tokens"]["refresh_token"], "rotated-single-use");
    assert_eq!(merged["auth_mode"], "chatgpt");
    Ok(())
}

#[cfg(unix)]
fn owner_only_permissions(path: &Path) -> (u32, u32, u32) {
    use std::os::unix::fs::MetadataExt as _;
    let metadata = std::fs::metadata(path).unwrap();
    assert_eq!(
        metadata.mode() & 0o777,
        0o600,
        "credentials must remain owner-only"
    );
    (metadata.mode(), metadata.uid(), metadata.gid())
}

#[cfg(windows)]
fn owner_only_permissions(path: &Path) -> bool {
    let file = File::open(path).unwrap();
    // This checks a protected DACL with exactly one full-access ACE for the
    // current owner: equality to true denotes the same permission semantics.
    let private = acyclic_fs::has_owner_only_permissions(&file).unwrap();
    assert!(private, "credentials must remain owner-only");
    private
}

fn oversized_merge_preserves_original(
    metadata: usize,
    access: usize,
    remove_size_bound: bool,
) -> proptest::test_runner::TestCaseResult {
    let (root, login, mut transport) = fixture(&jwt(0), 200).unwrap();
    let path = root.path().join("auth.json");
    let mut original = auth(&jwt(0));
    original["agent_identity"] = json!("m".repeat(metadata));
    let original = serde_json::to_vec_pretty(&original).unwrap();
    transport.response = json!({"access_token":"r".repeat(access)});
    prop_assert!(original.len() as u64 <= MAX_AUTH_BYTES);
    prop_assert!(serde_json::to_vec(&transport.response).unwrap().len() as u64 <= MAX_AUTH_BYTES);
    let mut private = crate::credential_file::create(root.path()).unwrap();
    private.write_all(&original).unwrap();
    private.persist(&path).unwrap();
    let permissions = owner_only_permissions(&path);
    let outcome = if remove_size_bound {
        // Mutation of stage: use its real writer and atomic publication but
        // omit only the encoded-size guard. The same oracle must reject it.
        let mut merged = crate::json(&original).unwrap();
        merged["tokens"]["access_token"] = transport.response["access_token"].clone();
        let encoded = crate::projection::encoded_len(&merged).unwrap() as u64 + 1;
        let mut staged = crate::credential_file::create(root.path()).unwrap();
        write_staged(
            &mut staged,
            &merged,
            encoded,
            &mut CredentialIo::new(None).unwrap(),
        )
        .unwrap();
        staged.persist(&path).unwrap();
        Ok(())
    } else {
        futures::executor::block_on(login.bearer(&transport, None, None)).map(|_| ())
    };
    prop_assert!(
        std::fs::read(&path).unwrap() == original,
        "original credential bytes changed"
    );
    prop_assert_eq!(owner_only_permissions(&path), permissions);
    prop_assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 1);
    prop_assert!(
        outcome.is_err_and(|error| {
            error
                .to_string()
                .contains("merged credentials exceed limit")
        }),
        "oversized merge was not rejected"
    );
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    #[test]
    fn oversized_refresh_merges_preserve_original_bytes_and_owner_only_permissions(
        metadata in 524_288usize..720_896,
        access in 524_288usize..720_896,
    ) {
        oversized_merge_preserves_original(metadata, access, false)?;
    }
}

#[test]
#[should_panic(expected = "original credential bytes changed")]
fn negative_control_removing_oversized_merge_bound_fails_the_same_property() {
    oversized_merge_preserves_original(524_288, 524_288, true).unwrap();
}

#[tokio::test]
async fn account_binding_errors_are_not_shared_with_a_newly_resolved_model() -> Result<()> {
    let (root, old, transport) = fixture(&jwt(0), 200)?;
    old.check_account(&auth(&jwt(0)))?;
    let fresh = old.fresh();
    let access = jwt(crate::codex::fixed_now().unix_timestamp() + 3600);
    let mut switched = auth(&access);
    switched["tokens"]["account_id"] = json!("new-account");
    std::fs::write(root.path().join("auth.json"), switched.to_string()).unwrap();

    // Queue both model bindings behind one guard so their sampled generation
    // is identical. The old model's account rejection must not poison the new one.
    let (release, wait) = futures::channel::oneshot::channel::<()>();
    let holder = crate::refresh::serialized(&old.refresh, async {
        wait.await.unwrap();
        ((), None)
    });
    let old_request = old.bearer(&transport, None, None);
    let new_request = fresh.bearer(&transport, None, None);
    futures::pin_mut!(holder, old_request, new_request);
    assert!(futures::poll!(&mut holder).is_pending());
    assert!(futures::poll!(&mut old_request).is_pending());
    assert!(futures::poll!(&mut new_request).is_pending());
    release.send(()).unwrap();
    let (_, rejected, accepted) = futures::join!(holder, old_request, new_request);
    assert!(matches!(
        rejected,
        Err(acyclic_harness::Error::Unauthorized(_))
    ));
    assert_eq!(accepted?, (access, Some("new-account".into())));
    assert!(transport.sent.lock().unwrap().is_empty());
    Ok(())
}
