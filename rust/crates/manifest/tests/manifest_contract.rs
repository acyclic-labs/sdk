//! Downstream admission, authorization, bounded-work and generated contract properties.
use acyclic_manifest::*;
use proptest::prelude::*;
use serde_json::{Value, json};
#[path = "support/admission.rs"]
mod admission;

fn fixture() -> Result<DeploymentManifest, ManifestError> {
    parse(include_bytes!("../fuzz/corpus/manifest_json/valid.json"))
}

fn missing() -> ManifestError {
    ManifestError::invalid("fixture", "missing-element")
}

#[test]
#[should_panic(expected = "inbox-authority")]
fn bypassed_inbox_authority_is_detected_by_independent_oracle() {
    let mut candidate = fixture().expect("valid seed");
    candidate
        .environments
        .first_mut()
        .unwrap()
        .grants
        .first_mut()
        .unwrap()
        .grant
        .0
        .actions = vec!["streams/read".into()];
    // Model admission with the authority check deleted: pass the candidate directly.
    assert_eq!(admission::check(&candidate), Ok(()));
}

#[test]
fn native_oversize_is_rejected_before_json_materialization()
-> Result<(), Box<dyn std::error::Error>> {
    let mut manifest = scaled(12)?;
    for grant in &mut manifest
        .environments
        .first_mut()
        .ok_or_else(missing)?
        .grants
    {
        grant.grant.0.actions = vec![format!("streams/{}", "r".repeat(1980)); 64];
    }
    let bytes = serde_json::to_vec(&manifest)?;
    assert!(bytes.len() > MAX_JSON_BYTES);
    assert_eq!(
        validate(&manifest).expect_err("native budget").rule,
        "json-size"
    );
    assert_eq!(parse(&bytes).expect_err("wire budget").rule, "json-size");
    Ok(())
}
#[test]
fn native_json_admission_agrees_at_exact_byte_boundary() -> Result<(), Box<dyn std::error::Error>> {
    let base = fixture()?.environments.remove(0);
    let mut manifest = DeploymentManifest {
        schema_version: 1,
        environments: (0..8)
            .map(|index| {
                let mut environment = base.clone();
                environment.name = format!("env{index}");
                environment.routes = (0..64)
                    .map(|route| RouteSpec {
                        host: format!("e{index}.example.com"),
                        path_prefix: format!("/p{route}/a"),
                        entry: "agent".into(),
                    })
                    .collect();
                environment
            })
            .collect(),
    };
    let mut remaining = MAX_JSON_BYTES - serde_json::to_vec(&manifest)?.len();
    for route in manifest
        .environments
        .iter_mut()
        .flat_map(|env| &mut env.routes)
    {
        let count = remaining.min(MAX_STRING_BYTES - route.path_prefix.len());
        route.path_prefix.push_str(&"a".repeat(count));
        remaining -= count;
    }
    assert_eq!(remaining, 0);
    let bytes = serde_json::to_vec(&manifest)?;
    assert_eq!(bytes.len(), MAX_JSON_BYTES);
    validate(&manifest)?;
    assert_eq!(parse(&bytes)?, manifest);
    manifest
        .environments
        .iter_mut()
        .flat_map(|env| &mut env.routes)
        .find(|route| route.path_prefix.len() < MAX_STRING_BYTES)
        .ok_or("padding capacity")?
        .path_prefix
        .push('a');
    let bytes = serde_json::to_vec(&manifest)?;
    assert_eq!(bytes.len(), MAX_JSON_BYTES + 1);
    assert_eq!(
        validate(&manifest).expect_err("native budget").rule,
        "json-size"
    );
    assert_eq!(parse(&bytes).expect_err("wire budget").rule, "json-size");
    Ok(())
}

fn scaled(count: usize) -> Result<DeploymentManifest, Box<dyn std::error::Error>> {
    let mut manifest = fixture()?;
    let env = manifest.environments.first_mut().ok_or_else(missing)?;
    env.crons.clear();
    if let EntryRuntime::Actor {
        subscriptions,
        timer_inbox,
        ..
    } = &mut env.entries.first_mut().ok_or_else(missing)?.runtime
    {
        subscriptions.clear();
        *timer_inbox = None;
    }
    env.bindings.clear();
    env.grants.clear();
    env.entries
        .first_mut()
        .ok_or_else(missing)?
        .bindings
        .clear();
    for index in 0..count {
        let name = format!("B{index}");
        let resource = format!("streams:/inboxes/{index}");
        env.bindings
            .push(Binding::new(name.clone(), name.clone(), resource.clone())?.into());
        env.grants.push(BindingGrant {
            name: name.clone(),
            grant: ResourceGrant {
                resource: ResourcePattern {
                    name: resource,
                    prefix: false,
                },
                actions: vec!["streams/read".into()],
            }
            .into(),
            limits: None,
        });
        env.entries
            .first_mut()
            .ok_or_else(missing)?
            .bindings
            .push(name);
    }
    Ok(manifest)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// Canonical Rust values survive JSON admission with identical grants and selections.
    #[test]
    fn admitted_rust_json_round_trip(count in 1usize..=64, expiry in any::<u64>(), worker in any::<bool>(), quota in 1u32..=u32::MAX) {
        let mut manifest = scaled(count).map_err(|e| TestCaseError::fail(e.to_string()))?;
        manifest.environments.first_mut().ok_or_else(missing)?.grants.first_mut().ok_or_else(missing)?.limits = Some(Limits {
            rps: Some(expiry), burst: None, bytes_per_s: None, channels: None,
            concurrency: None, bytes: None, spend_micros: None, tokens: None,
        }.into());
        if worker {
            manifest.environments.first_mut().ok_or_else(missing)?.entries.first_mut().ok_or_else(missing)?.runtime = EntryRuntime::Worker { limits: WorkerLimits { cpu_millis:quota, memory_bytes:quota, timeout_millis:quota } };
        }
        validate(&manifest)?;
        prop_assert_eq!(admission::check(&manifest), Ok(()));
        prop_assert_eq!(&parse(&serde_json::to_vec(&manifest)?)?, &manifest);
        if worker {
            manifest.environments.first_mut().ok_or_else(missing)?.entries.first_mut().ok_or_else(missing)?.runtime = EntryRuntime::Worker { limits: WorkerLimits { cpu_millis:0, memory_bytes:quota, timeout_millis:quota } };
            prop_assert!(validate(&manifest).is_err());
        }
    }

    /// The real validator's visited-byte/node/edge count grows at most linearly.
    #[test]
    fn validation_work_is_linear(count in 1usize..=16) {
        let small = scaled(count).map_err(|e| TestCaseError::fail(e.to_string()))?;
        let large = scaled(count * 4).map_err(|e| TestCaseError::fail(e.to_string()))?;
        let small_work = validate_work(&small)?;
        let large_work = validate_work(&large)?;
        prop_assert!(large_work <= small_work * 5);
        prop_assert!(environment_typescript(&large, "production", "agent")?.contains("B0: Streams"));
    }

    /// Native and JSON admission share the aggregate byte ceiling, including wire overhead.
    #[test]
    fn native_and_json_budgets_agree(count in 1usize..=16, width in 1usize..=2040) {
        let mut manifest = scaled(count).map_err(|e| TestCaseError::fail(e.to_string()))?;
        for grant in &mut manifest.environments.first_mut().ok_or_else(missing)?.grants {
            grant.grant.0.actions = vec![format!("streams/{}", "r".repeat(width)); 64];
        }
        let bytes = serde_json::to_vec(&manifest)?;
        let native = validate(&manifest);
        prop_assert_eq!(native.is_ok(), parse(&bytes).is_ok());
        if bytes.len() > MAX_JSON_BYTES { prop_assert_eq!(native.expect_err("oversized" ).rule, "json-size"); }
    }

    /// Shared grants are indexed once even when both action and cron counts grow.
    #[test]
    fn shared_grant_work_is_linear(count in 1usize..=16) {
        let make = |size: usize| -> Result<DeploymentManifest, ManifestError> {
            let mut manifest = fixture()?;
            let env = manifest.environments.first_mut().ok_or_else(missing)?;
            env.grants.first_mut().ok_or_else(missing)?.grant.0.actions = vec!["streams/append".into(); size];
            let cron = env.crons.first_mut().ok_or_else(missing)?.clone();
            env.crons = (0..size).map(|i| CronSpec { name:format!("cron{i}"), ..cron.clone() }).collect();
            Ok(manifest)
        };
        prop_assert!(validate_work(&make(count * 4)?)? <= validate_work(&make(count)?)? * 5);
    }
    /// Plain API keys, bearer credentials and credential-bearing URLs never become refs.
    #[test]
    fn plaintext_credentials_are_never_secret_references(payload in "[A-Za-z0-9_-]{1,120}") {
        for value in [format!("sk-ant-api03-{payload}"), format!("Bearer {payload}"),
            format!("https://user:{payload}@provider.example/"), format!("secrets:/user:{payload}"),
            format!("secrets:/{payload}@provider"), payload.to_owned(), format!("secrets:/{payload}?token=key")] {
            prop_assert!(SecretReference::new(value.clone()).is_err());
            let mut json: Value = serde_json::from_slice(include_bytes!("../fuzz/corpus/manifest_json/valid.json"))?;
            *json.pointer_mut("/environments/0/secrets/0/reference").ok_or_else(missing)? = value.into();
            prop_assert!(parse(&serde_json::to_vec(&json)?).is_err());
        }
        let reference = SecretReference::new(format!("secrets:/models/{payload}"));
        prop_assert_eq!(reference.is_ok(), payload.len() <= 63);
        if let Ok(reference) = reference {
            prop_assert_eq!(serde_json::from_str::<SecretReference>(&serde_json::to_string(&reference)?)?, reference);
        }
    }

    #[test]
    fn accepted_untrusted_json_round_trips(bytes in prop::collection::vec(any::<u8>(), 0..4096)) {
        if let Ok(manifest) = parse(&bytes) { prop_assert_eq!(parse(&serde_json::to_vec(&manifest)?)?, manifest); }
    }
}

#[test]
fn review_regressions_fail_before_publication() -> Result<(), Box<dyn std::error::Error>> {
    let base: Value =
        serde_json::from_slice(include_bytes!("../fuzz/corpus/manifest_json/valid.json"))?;
    let cases = [
        (
            "/environments/0/bindings/0/resource",
            json!("objects:/billing/ledger"),
        ),
        (
            "/environments/0/grants/0/grant/actions",
            json!(["objects/get"]),
        ),
        (
            "/environments/0/grants/0/grant/resource/name",
            json!("streams:/agents/a/*"),
        ),
        ("/environments/0/entries/0/runtime/subscriptions", json!([])),
        (
            "/environments/0/entries/0/runtime/limits/memoryBytes",
            json!("0"),
        ),
        ("/environments/0/entries/0/runtime/homeRegion", json!("")),
        (
            "/environments/0/entries/0/runtime/subscriptions/0/start",
            json!({"currentHead":false}),
        ),
        (
            "/environments/0/entries/0/runtime",
            json!({"kind":"worker","limits":{"cpuMillis":1,"memoryBytes":1024,"timeoutMillis":10}}),
        ),
        (
            "/environments/0/crons/0/inbox/name",
            json!("streams:/billing/ledger"),
        ),
        ("/environments/0/crons/0/inbox/prefix", json!(true)),
        (
            "/environments/0/entries/0/runtime/timerInbox/stream/name",
            json!("streams:/unwatched"),
        ),
        ("/environments/0/name", json!("Prod_Main")),
        ("/environments/0/name", json!("-production")),
        ("/environments/0/crons/0/schedule", json!("0 0 31 2 *")),
        ("/environments/0/crons/0/schedule", json!("*/0 * * * *")),
        ("/environments/0/entries/0/bindings", json!(["missing"])),
        ("/environments/0/entries/0/secrets", json!(["missing"])),
        ("/environments/0/bindings/0/name", json!("1binding")),
        (
            "/environments/0/secrets/0/reference",
            json!("sk-ant-api03-secret"),
        ),
        (
            "/environments/0/routes",
            json!([{"host":"app.run.acyclic.dev","pathPrefix":"/","entry":"agent"}]),
        ),
        (
            "/environments/0/routes",
            json!([{"host":"app.example.com","pathPrefix":"/a/../b","entry":"agent"}]),
        ),
        (
            "/environments/0/routes",
            json!([{"host":"app.example.com","pathPrefix":"/api/","entry":"agent"}]),
        ),
        (
            "/environments/0/assets",
            json!([{"entry":"agent","binding":"INBOX","bundleSha256":"11".repeat(32),"spaFallback":true,"cache":"immutable"}]),
        ),
        (
            "/environments/0/entries/0/bindings",
            json!(["INBOX", "INBOX"]),
        ),
    ];
    for (pointer, invalid) in cases {
        let mut candidate = base.clone();
        if let Some(field) = candidate.pointer_mut(pointer) {
            *field = invalid;
        } else {
            let (parent, field) = pointer.rsplit_once('/').ok_or("invalid pointer")?;
            candidate
                .pointer_mut(parent)
                .and_then(Value::as_object_mut)
                .ok_or("missing regression object")?
                .insert(field.into(), invalid);
        }
        assert!(
            parse(&serde_json::to_vec(&candidate)?).is_err(),
            "admitted {pointer}"
        );
    }
    for host in [
        "127.1",
        "169.254.169.0xfe",
        "0x7f.1",
        "127.0.0.01",
        "2130706433",
        "LOCALHOST",
        "example.com.",
    ] {
        let mut candidate = base.clone();
        *candidate
            .pointer_mut("/environments/0/entries/0/fetch")
            .ok_or_else(missing)? = json!({"name":"HTTP","egress":[{"host":host,"port":443}]});
        assert!(
            parse(&serde_json::to_vec(&candidate)?).is_err(),
            "admitted {host}"
        );
    }
    let mut routes = fixture()?;
    routes
        .environments
        .first_mut()
        .ok_or_else(missing)?
        .routes
        .push(RouteSpec {
            host: "app.example.com".into(),
            path_prefix: "/api".into(),
            entry: "agent".into(),
        });
    let mut staging = routes.environments.first_mut().ok_or_else(missing)?.clone();
    staging.name = "staging".into();
    routes.environments.push(staging);
    assert_eq!(
        validate(&routes).err().map(|e| e.rule),
        Some("duplicate-route-across-environments".into())
    );
    Ok(())
}

#[test]
fn all_binding_products_and_fetch_are_scoped_to_selected_entry()
-> Result<(), Box<dyn std::error::Error>> {
    let mut manifest = fixture()?;
    let env = manifest.environments.first_mut().ok_or_else(missing)?;
    for product in [
        "fs",
        "objects",
        "actors",
        "workers",
        "machines",
        "execution",
        "infer",
    ] {
        let name = product.to_ascii_uppercase();
        let resource = format!("{product}:/sandbox");
        env.bindings
            .push(Binding::new(name.clone(), name.clone(), resource.clone())?.into());
        env.grants.push(BindingGrant {
            name: name.clone(),
            grant: ResourceGrant {
                resource: ResourcePattern {
                    name: resource,
                    prefix: false,
                },
                actions: vec![format!(
                    "{product}/{}",
                    if product == "fs" { "read" } else { "get" }
                )],
            }
            .into(),
            limits: None,
        });
        env.entries
            .first_mut()
            .ok_or_else(missing)?
            .bindings
            .push(name);
    }
    env.entries.first_mut().ok_or_else(missing)?.fetch = Some(FetchBinding {
        name: "HTTP".into(),
        egress: vec![EgressRule {
            host: "api.example.com".into(),
            port: 8443,
        }],
    });
    let mut worker = env.entries.first_mut().ok_or_else(missing)?.clone();
    worker.name = "web".into();
    worker.bindings.clear();
    worker.secrets.clear();
    worker.fetch = None;
    worker.runtime = EntryRuntime::Worker {
        limits: WorkerLimits {
            cpu_millis: 1,
            memory_bytes: 1024,
            timeout_millis: 10,
        },
    };
    env.entries.push(worker);
    let actor = environment_typescript(&manifest, "production", "agent")?;
    let worker = environment_typescript(&manifest, "production", "web")?;
    assert!(
        actor.contains("INFER: Infer")
            && actor.contains("EXECUTION: Execution")
            && actor.contains("HTTP: Fetch")
    );
    assert!(!worker.contains("HTTP:") && !worker.contains("MODEL_KEY:"));
    Ok(())
}

#[tokio::test]
async fn cron_inbox_append_reaches_lowered_actor_wake_subscription()
-> Result<(), Box<dyn std::error::Error>> {
    use acyclic_stream::{AppendRequest, IdempotencyKey, MemoryStream, StreamPath, StreamProvider};
    use futures::StreamExt;
    let manifest = fixture()?;
    let env = manifest.environments.first().ok_or_else(missing)?;
    let request = lower_actor(
        env,
        env.entries.first().ok_or_else(missing)?,
        "project-production-agent",
    )?;
    assert_eq!(request.home_region(), "eu");
    assert_eq!(request.limits().handler_timeout_millis(), 1000);
    assert_eq!(
        request.bindings().first().ok_or_else(missing)?.resource(),
        "streams:/agents/a/inbox"
    );
    let subscription = request.subscriptions().first().ok_or_else(missing)?;
    let path = StreamPath::new(subscription.stream_path())?;
    let stream = MemoryStream::default();
    stream
        .append(AppendRequest {
            path: path.clone(),
            records: vec![bytes::Bytes::from_static(b"deployed").into()],
            if_tail: None,
            idempotency_key: None,
        })
        .await?;
    let mut wake = stream
        .follow(
            path.clone(),
            subscription.start().cursor_value().ok_or("cursor")?,
            acyclic_stream::FollowFilter::default(),
        )
        .await?;
    let initialized = wake.next().await.ok_or("missing initialization")??;
    assert_eq!(initialized.sequence, 0);
    let pending = wake.next();
    futures::pin_mut!(pending);
    assert!(futures::poll!(pending.as_mut()).is_pending());
    // The actor subscription is waiting before the cron appends. No Invoke or polling.
    let record = bytes::Bytes::from_static(b"cron-fire:tick:123");
    let append = AppendRequest {
        path,
        records: vec![record.clone().into()],
        if_tail: None,
        idempotency_key: Some(IdempotencyKey::new(bytes::Bytes::from_static(
            b"cron:tick:123",
        ))?),
    };
    stream.append(append.clone()).await?;
    stream.append(append).await?;
    let delivered = tokio::time::timeout(std::time::Duration::from_secs(1), pending)
        .await?
        .ok_or("no wake")??;
    assert_eq!(delivered.value, record.into());
    Ok(())
}

#[test]
fn version_bounds_and_error_location_are_stable() -> Result<(), Box<dyn std::error::Error>> {
    let valid = std::str::from_utf8(include_bytes!("../fuzz/corpus/manifest_json/valid.json"))?;
    assert!(parse(valid.as_bytes()).is_ok());
    assert!(
        parse(
            valid
                .replace(
                    "\"schemaVersion\":1",
                    "\"schemaVersion\":1,\"schemaVersion\":1"
                )
                .as_bytes()
        )
        .is_err()
    );
    let error = parse(br#"{"schemaVersion":2,"future":true}"#)
        .err()
        .ok_or("accepted v2")?;
    assert_eq!(
        (error.path.as_str(), error.rule.as_str()),
        ("$.schemaVersion", "unsupported-version")
    );
    assert!(parse(&vec![b' '; MAX_JSON_BYTES + 1]).is_err());
    let mut manifest = fixture()?;
    manifest
        .environments
        .first_mut()
        .ok_or_else(missing)?
        .entries
        .first_mut()
        .ok_or_else(missing)?
        .module = "x".repeat(MAX_STRING_BYTES + 1);
    assert!(validate(&manifest).is_err());
    manifest = fixture()?;
    manifest
        .environments
        .first_mut()
        .ok_or_else(missing)?
        .bindings = vec![
        manifest
            .environments
            .first_mut()
            .ok_or_else(missing)?
            .bindings
            .first_mut()
            .ok_or_else(missing)?
            .clone();
        MAX_ITEMS + 1
    ];
    assert!(validate(&manifest).is_err());
    let mut value: Value =
        serde_json::from_slice(include_bytes!("../fuzz/corpus/manifest_json/valid.json"))?;
    value
        .pointer_mut("/environments/0/grants/0/grant")
        .and_then(Value::as_object_mut)
        .ok_or_else(missing)?
        .insert("future".into(), json!(true));
    assert!(parse(&serde_json::to_vec(&value)?).is_err());
    Ok(())
}

#[test]
fn asset_bundle_requires_immutable_digest_and_matching_objects_grant()
-> Result<(), Box<dyn std::error::Error>> {
    let mut manifest = fixture()?;
    let env = manifest.environments.first_mut().ok_or_else(missing)?;
    env.bindings.push(
        Binding::new(
            "ASSETS".into(),
            "assetsGrant".into(),
            "objects:/static/".into(),
        )?
        .into(),
    );
    env.grants.push(BindingGrant {
        name: "assetsGrant".into(),
        grant: ResourceGrant {
            resource: ResourcePattern {
                name: "objects:/static/".into(),
                prefix: true,
            },
            actions: vec!["objects/get".into()],
        }
        .into(),
        limits: None,
    });
    env.entries
        .first_mut()
        .ok_or_else(missing)?
        .bindings
        .push("ASSETS".into());
    env.assets.push(AssetSpec {
        entry: "agent".into(),
        binding: "ASSETS".into(),
        bundle_sha256: "ab".repeat(32),
        spa_fallback: true,
        cache: AssetCache::Immutable,
    });
    validate(&manifest)?;
    assert_eq!(admission::check(&manifest), Ok(()));
    let directory = acyclic_auth::objects_resource(
        b"static",
        format!("assets/{}/", "ab".repeat(32)).as_bytes(),
    );
    manifest
        .environments
        .first_mut()
        .ok_or_else(missing)?
        .grants
        .get_mut(1)
        .ok_or_else(missing)?
        .grant
        .0
        .resource
        .name = directory.clone();
    *manifest
        .environments
        .first_mut()
        .ok_or_else(missing)?
        .bindings
        .get_mut(1)
        .ok_or_else(missing)? =
        Binding::new("ASSETS".into(), "assetsGrant".into(), directory)?.into();
    assert_eq!(admission::check(&manifest), Err("asset-object-authority"));
    assert!(validate(&manifest).is_err());
    manifest
        .environments
        .first_mut()
        .ok_or_else(missing)?
        .grants
        .get_mut(1)
        .ok_or_else(missing)?
        .grant
        .0
        .resource
        .name = "objects:/static/".into();
    *manifest
        .environments
        .first_mut()
        .ok_or_else(missing)?
        .bindings
        .get_mut(1)
        .ok_or_else(missing)? = Binding::new(
        "ASSETS".into(),
        "assetsGrant".into(),
        "objects:/static/".into(),
    )?
    .into();
    manifest
        .environments
        .first_mut()
        .ok_or_else(missing)?
        .grants
        .get_mut(1)
        .ok_or_else(missing)?
        .grant
        .0
        .resource
        .prefix = false;
    assert!(validate(&manifest).is_err());
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    /// Every opaque bundle object is covered; an encoded directory is insufficient.
    #[test]
    fn asset_grants_cover_actual_opaque_keys(key in prop::collection::vec(any::<u8>(), 0..64), directory_only in any::<bool>()) {
        let mut manifest = fixture()?;
        let env = manifest.environments.first_mut().ok_or_else(missing)?;
        env.crons.clear();
        if let EntryRuntime::Actor { timer_inbox, .. } = &mut env.entries.first_mut().ok_or_else(missing)?.runtime { *timer_inbox = None; }
        let digest = "ab".repeat(32);
        let name = if directory_only {
            acyclic_auth::objects_resource(b"static", format!("assets/{digest}/").as_bytes())
        } else { "objects:/static/".into() };
        *env.bindings.first_mut().ok_or_else(missing)? = Binding::new("INBOX".into(), "inboxGrant".into(), name.clone())?.into();
        env.grants.first_mut().ok_or_else(missing)?.grant.0 = ResourceGrant { resource:ResourcePattern { name, prefix:true }, actions:vec!["objects/get".into()] };
        env.assets.push(AssetSpec { entry:"agent".into(), binding:"INBOX".into(), bundle_sha256:digest.clone(), spa_fallback:true, cache:AssetCache::Immutable });
        prop_assert_eq!(validate(&manifest).is_ok(), !directory_only);
        if !directory_only {
            let mut object = format!("assets/{digest}/").into_bytes();
            object.extend_from_slice(&key);
            let resource = acyclic_auth::objects_resource(b"static", &object);
            prop_assert!(acyclic_auth::authority::resource_matches(&manifest.environments.first().ok_or_else(missing)?.grants.first().ok_or_else(missing)?.grant.resource, &resource));
            prop_assert_eq!(admission::check(&manifest), Ok(()));
        }
    }

    /// Whole-segment grants cover descendants but never neighboring names.
    #[test]
    fn inbox_scope_uses_s1_whole_segments(parent in "[a-z]{1,12}", suffix in "[a-z]{1,8}") {
        let mut manifest = fixture()?;
        let prefix = format!("streams:/agents/{parent}");
        let env = manifest.environments.first_mut().ok_or_else(missing)?;
        *env.bindings.first_mut().ok_or_else(missing)? = Binding::new("INBOX".into(), "inboxGrant".into(), prefix.clone())?.into();
        env.grants.first_mut().ok_or_else(missing)?.grant.0.resource = ResourcePattern { name: prefix, prefix: true };
        for (path, admitted) in [(format!("agents/{parent}/inbox"), true), (format!("agents/{parent}{suffix}/inbox"), false)] {
            let env = manifest.environments.first_mut().ok_or_else(missing)?;
            let resource = format!("streams:/{path}");
            env.crons.first_mut().ok_or_else(missing)?.inbox.0.name = resource.clone();
            if let EntryRuntime::Actor { subscriptions, timer_inbox, .. } = &mut env.entries.first_mut().ok_or_else(missing)?.runtime {
                *subscriptions.first_mut().ok_or_else(missing)? = SubscriptionSpec::new("inbox".into(), path, acyclic_actors::domain::SubscriptionStart::cursor(0), true)?.into();
                if let Some(inbox) = timer_inbox { inbox.stream.0.name = resource; }
            }
            prop_assert_eq!(validate(&manifest).is_ok(), admitted);
        }
    }

    /// URL-style integer and mixed hexadecimal IP spellings never become DNS grants.
    #[test]
    fn numeric_ip_spellings_are_not_egress_hosts(address in any::<u32>()) {
        let [a, b, c, d] = address.to_be_bytes();
        for host in [address.to_string(), format!("0x{address:x}"),
            format!("{a}.{b}.{c}.0x{d:x}"),
            format!("{a}.{}", address & 0x00ff_ffff)] {
            let mut manifest = fixture()?;
            manifest.environments.first_mut().ok_or_else(missing)?.entries.first_mut().ok_or_else(missing)?.fetch = Some(FetchBinding { name:"HTTP".into(), egress:vec![EgressRule { host, port:443 }] });
            prop_assert!(validate(&manifest).is_err());
        }
    }
}
#[test]
fn generated_artifacts_preserve_the_admitted_manifest_surface()
-> Result<(), Box<dyn std::error::Error>> {
    let declarations = typescript()?;
    let schema: Value = serde_json::from_str(&json_schema()?)?;
    let environment = environment_typescript(&fixture()?, "production", "agent")?;
    assert!(declarations.contains("export type DeploymentManifest"));
    assert!(
        schema
            .pointer("/properties/environments")
            .is_some_and(Value::is_object)
    );
    assert!(environment.contains("readonly INBOX: Streams;"));
    assert!(environment.contains("readonly MODEL_KEY: Secret;"));
    assert_eq!(declarations, typescript()?);
    assert_eq!(schema, serde_json::from_str::<Value>(&json_schema()?)?);
    assert_eq!(
        environment,
        environment_typescript(&fixture()?, "production", "agent")?
    );
    Ok(())
}
