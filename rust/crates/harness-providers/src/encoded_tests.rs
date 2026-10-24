use super::*;
use crate::{Adapter, tests as fixtures};
use acyclic_harness_core::model::ModelProvider as _;
use futures::TryStreamExt as _;

#[tokio::test]
async fn exact_verified_total_is_single_flight_independent_of_planning_weights() -> Result<()> {
    let (provider, _) = fixtures::provider(Adapter::Responses, 200);
    let (request, _) = fixtures::request(&provider)?;
    let names = crate::tool_names::Names::new(request.request());
    let origin = provider.reasoning_origin(provider.key.as_deref(), None);
    let encoded = bodies(&provider, &request, &names, &origin, None).await?;
    crate::counting::VERIFIED_EXCHANGES.with(|count| count.set(0));
    let exchange = || crate::HttpRequest {
        url: crate::counting::wire::endpoint(
            &provider.config.base_url,
            &provider.model.name,
            provider.config.token_counter.as_ref().unwrap(),
        )
        .unwrap(),
        headers: crate::dispatch::headers(&provider, provider.key.as_deref(), None, false),
        body: encoded.count.clone(),
    };
    let count = || projection::plan(&provider, &request, 0).unwrap();
    let (first, _) = crate::counting::send_count(
        &provider,
        exchange(),
        count(),
        None,
        None,
        &encoded.verified,
    )
    .await?;
    provider
        .estimator
        .observe(1, &serde_json::json!({"usage":{"input_tokens":10_000}}));
    let (next, _) = crate::counting::send_count(
        &provider,
        exchange(),
        count(),
        None,
        None,
        &encoded.verified,
    )
    .await?;
    assert_eq!(first.input_tokens, next.input_tokens);
    assert!(next.fixed_tokens > first.fixed_tokens);
    assert_eq!(
        crate::counting::VERIFIED_EXCHANGES.with(std::cell::Cell::get),
        1
    );
    Ok(())
}

#[tokio::test]
async fn counting_and_generation_share_one_verified_projection() -> Result<()> {
    for adapter in [
        Adapter::Responses,
        Adapter::Chat,
        Adapter::Anthropic,
        Adapter::Gemini,
    ] {
        let (provider, _) = fixtures::provider(adapter, 200);
        let (request, dispatch) = fixtures::request(&provider)?;
        PROJECTIONS.with(|count| count.set(0));
        crate::counting::VERIFIED_EXCHANGES.with(|count| count.set(0));
        let start = std::time::Instant::now();
        provider.count_tokens(&request).await?;
        provider.count_tokens(&request).await?;
        provider
            .generate(request, dispatch)
            .try_collect::<Vec<_>>()
            .await?;
        let projections = PROJECTIONS.with(std::cell::Cell::get);
        println!(
            "ENCODED_AFTER adapter={adapter:?} projections={projections} wall_us={}",
            start.elapsed().as_micros()
        );
        assert_eq!(
            projections, 1,
            "count and generation must share their projection"
        );
        assert_eq!(
            crate::counting::VERIFIED_EXCHANGES.with(std::cell::Cell::get),
            1
        );
    }
    Ok(())
}

#[test]
fn encoded_count_envelope_is_identical_to_the_typed_protocol() -> Result<()> {
    for adapter in [
        Adapter::Responses,
        Adapter::Chat,
        Adapter::Anthropic,
        Adapter::Gemini,
    ] {
        let (provider, _) = fixtures::provider(adapter, 200);
        let (request, _) = fixtures::request(&provider)?;
        let names = crate::tool_names::Names::new(request.request());
        let origin = provider.reasoning_origin(provider.key.as_deref(), None);
        let body = projection::body_named(
            &provider,
            request.request(),
            &names,
            &origin,
            &mut crate::files::FileText::default(),
        )?;
        let generation =
            serde_json::to_vec(&body).map_err(|error| crate::invalid(error.to_string()))?;
        let expected = serde_json::to_vec(&crate::counting::wire::body(
            &body,
            &provider.model.name,
            adapter,
        ))
        .map_err(|error| crate::invalid(error.to_string()))?;
        let actual =
            crate::counting::wire::encoded_body(&generation, &provider.model.name, adapter)?;
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&actual)
                .map_err(|error| crate::invalid(error.to_string()))?,
            serde_json::from_slice::<serde_json::Value>(&expected)
                .map_err(|error| crate::invalid(error.to_string()))?
        );
        assert_eq!(actual.len(), expected.len());
    }
    Ok(())
}

#[tokio::test]
async fn memo_never_crosses_reader_configuration_or_request_ownership() -> Result<()> {
    let (provider, _) = fixtures::provider(Adapter::Responses, 200);
    let (request, _) = fixtures::request(&provider)?;
    let origin = provider.reasoning_origin(provider.key.as_deref(), None);
    let names = crate::tool_names::Names::new(request.request());
    PROJECTIONS.with(|count| count.set(0));
    let (first, same) = futures::try_join!(
        bodies(&provider, &request, &names, &origin, None),
        bodies(&provider, &request, &names, &origin, None),
    )?;
    assert_eq!(PROJECTIONS.with(std::cell::Cell::get), 1);
    assert_eq!(first.generation.as_ptr(), same.generation.as_ptr());
    let mut foreign = provider.clone();
    foreign.files.maximum_bytes -= 1;
    bodies(&foreign, &request, &names, &origin, None).await?;
    assert_eq!(PROJECTIONS.with(std::cell::Cell::get), 2);
    let cloned = request.clone();
    let cloned_names = crate::tool_names::Names::new(cloned.request());
    bodies(&provider, &cloned, &cloned_names, &origin, None).await?;
    assert_eq!(PROJECTIONS.with(std::cell::Cell::get), 3);
    let mut foreign = provider.clone();
    Arc::make_mut(&mut foreign.config).account_id = Some("different-account".into());
    let foreign_origin = foreign.reasoning_origin(foreign.key.as_deref(), None);
    bodies(&foreign, &request, &names, &foreign_origin, None).await?;
    assert_eq!(PROJECTIONS.with(std::cell::Cell::get), 4);
    Ok(())
}

#[tokio::test]
async fn native_memo_retains_original_custody_and_rejects_foreign_ledgers() -> Result<()> {
    let (provider, _) = fixtures::provider(Adapter::Responses, 200);
    let (request, _) = fixtures::request(&provider)?;
    let origin = provider.reasoning_origin(provider.key.as_deref(), None);
    let names = crate::tool_names::Names::new(request.request());
    let cancel = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(1_000_000, 4_000_000),
        &cancel,
    )
    .unwrap();
    let other = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(1_000_000, 4_000_000),
        &cancel,
    )
    .unwrap();
    let baseline = work.live_allocation_bytes();
    let other_baseline = other.live_allocation_bytes();
    PROJECTIONS.with(|count| count.set(0));
    let first = bodies(&provider, &request, &names, &origin, Some(&work)).await?;
    assert!(work.live_allocation_bytes() > 0);
    let before = work.receipt();
    let same = bodies(&provider, &request, &names, &origin, Some(&work)).await?;
    assert_eq!(PROJECTIONS.with(std::cell::Cell::get), 1);
    assert_eq!(first.generation.as_ptr(), same.generation.as_ptr());
    assert_eq!(work.receipt().bytes_copied, before.bytes_copied);
    let recheck = work.recheck_admitted();
    assert!(work.shares_ledger(&recheck));
    assert!(!work.shares_materialization_scope(&recheck));
    let scoped = bodies(&provider, &request, &names, &origin, Some(&recheck)).await?;
    assert_eq!(PROJECTIONS.with(std::cell::Cell::get), 2);
    let foreign = bodies(&provider, &request, &names, &origin, Some(&other)).await?;
    assert_eq!(PROJECTIONS.with(std::cell::Cell::get), 3);
    assert_ne!(first.generation.as_ptr(), foreign.generation.as_ptr());
    cancel.cancel();
    assert!(
        bodies(&provider, &request, &names, &origin, Some(&work))
            .await
            .is_err()
    );
    drop((first, same, foreign, scoped));
    drop(names);
    drop(request);
    drop(provider);
    assert_eq!(work.live_allocation_bytes(), baseline);
    assert_eq!(other.live_allocation_bytes(), other_baseline);
    Ok(())
}

#[tokio::test]
async fn encoded_static_prefix_is_reused_across_advancing_steps() -> Result<()> {
    for adapter in [
        Adapter::Responses,
        Adapter::Chat,
        Adapter::Anthropic,
        Adapter::Gemini,
    ] {
        let (provider, _) = fixtures::provider(adapter, 200);
        let (prepared, _) = fixtures::request(&provider)?;
        let mut input = prepared.request().clone();
        input.messages.insert(
            1,
            acyclic_harness_core::model::ModelMessage {
                role: acyclic_harness_core::model::ModelRole::System,
                content: acyclic_harness_core::model::ModelContent::Text(
                    "second static instruction".into(),
                ),
            },
        );
        crate::encoded_prefix::PREFIX_ENCODINGS.with(|count| count.set(0));
        for step in 0..32 {
            input
                .messages
                .push(acyclic_harness_core::model::ModelMessage {
                    role: acyclic_harness_core::model::ModelRole::User,
                    content: acyclic_harness_core::model::ModelContent::Text(format!(
                        "late fact {step}"
                    )),
                });
            let request = PreparedModelRequest::prepare(
                input.clone(),
                acyclic_harness_core::conversation::Limits::default(),
                None,
            )?;
            let names = crate::tool_names::Names::new(request.request());
            let origin = provider.reasoning_origin(provider.key.as_deref(), None);
            let encoded = bodies(&provider, &request, &names, &origin, None).await?;
            let original =
                crate::dispatch::encode(&provider, &request, &names, None, &origin).await?;
            assert_eq!(encoded.generation.as_ref(), original.as_slice());
        }
        assert_eq!(
            crate::encoded_prefix::PREFIX_ENCODINGS.with(std::cell::Cell::get),
            1
        );
        println!("ENCODED_PREFIX adapter={adapter:?} advancing_steps=32 prefix_encodings=1");
        input.messages.get_mut(0).unwrap().content =
            acyclic_harness_core::model::ModelContent::Text("changed static system".into());
        let request = PreparedModelRequest::prepare(
            input.clone(),
            acyclic_harness_core::conversation::Limits::default(),
            None,
        )?;
        let names = crate::tool_names::Names::new(request.request());
        let origin = provider.reasoning_origin(provider.key.as_deref(), None);
        let encoded = bodies(&provider, &request, &names, &origin, None).await?;
        let original = crate::dispatch::encode(&provider, &request, &names, None, &origin).await?;
        assert_eq!(encoded.generation.as_ref(), original.as_slice());
        assert_eq!(
            crate::encoded_prefix::PREFIX_ENCODINGS.with(std::cell::Cell::get),
            2
        );
        input.messages.get_mut(1).unwrap().content =
            acyclic_harness_core::model::ModelContent::Text(
                "changed second static instruction".into(),
            );
        let request = PreparedModelRequest::prepare(
            input,
            acyclic_harness_core::conversation::Limits::default(),
            None,
        )?;
        let names = crate::tool_names::Names::new(request.request());
        let encoded = bodies(&provider, &request, &names, &origin, None).await?;
        let original = crate::dispatch::encode(&provider, &request, &names, None, &origin).await?;
        assert_eq!(encoded.generation.as_ref(), original.as_slice());
        assert_eq!(
            crate::encoded_prefix::PREFIX_ENCODINGS.with(std::cell::Cell::get),
            3
        );
    }
    Ok(())
}

#[tokio::test]
async fn static_prefix_eviction_releases_the_original_work_ledger() -> Result<()> {
    let (provider, _) = fixtures::provider(Adapter::Responses, 200);
    let cancel = acyclic_fs::CancellationToken::new();
    let old = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(1_000_000, 4_000_000),
        &cancel,
    )
    .unwrap();
    let new = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(1_000_000, 4_000_000),
        &cancel,
    )
    .unwrap();
    let old_baseline = old.live_allocation_bytes();
    let new_baseline = new.live_allocation_bytes();
    for (index, work) in [&old, &new].into_iter().enumerate() {
        let (original, _) = fixtures::request(&provider)?;
        let mut input = original.request().clone();
        input.messages.get_mut(0).unwrap().content =
            acyclic_harness_core::model::ModelContent::Text(format!(
                "different verified static prefix {index}"
            ));
        let request = PreparedModelRequest::prepare(
            input,
            acyclic_harness_core::conversation::Limits::default(),
            None,
        )?;
        let names = crate::tool_names::Names::new(request.request());
        let origin = provider.reasoning_origin(provider.key.as_deref(), None);
        bodies(&provider, &request, &names, &origin, Some(work)).await?;
    }
    assert_eq!(old.live_allocation_bytes(), old_baseline);
    assert!(new.live_allocation_bytes() > new_baseline);
    drop(provider);
    assert_eq!(new.live_allocation_bytes(), new_baseline);
    Ok(())
}

#[tokio::test]
async fn native_advancing_steps_share_only_verified_static_bytes() -> Result<()> {
    let mut measurements = Vec::new();
    for adapter in [
        Adapter::Responses,
        Adapter::Chat,
        Adapter::Anthropic,
        Adapter::Gemini,
    ] {
        let (provider, _) = fixtures::provider(adapter, 200);
        let cancellation = acyclic_fs::CancellationToken::new();
        let mut ledgers = Vec::new();
        crate::encoded_prefix::PREFIX_ENCODINGS.with(|count| count.set(0));
        for step in 0..32 {
            let work = acyclic_fs::NativeMaterializationWork::new(
                acyclic_fs::performance::work_budget(1_000_000, 4_000_000),
                &cancellation,
            )
            .unwrap();
            let baseline = work.live_allocation_bytes();
            let (original, _) = fixtures::request(&provider)?;
            let mut input = original.request().clone();
            input
                .messages
                .push(acyclic_harness_core::model::ModelMessage {
                    role: acyclic_harness_core::model::ModelRole::User,
                    content: acyclic_harness_core::model::ModelContent::Text(format!(
                        "native step {step}"
                    )),
                });
            let request = PreparedModelRequest::prepare(
                input,
                acyclic_harness_core::conversation::Limits::default(),
                None,
            )?;
            let names = crate::tool_names::Names::new(request.request());
            let origin = provider.reasoning_origin(provider.key.as_deref(), None);
            let encoded = bodies(&provider, &request, &names, &origin, Some(&work)).await?;
            let uncached =
                crate::dispatch::encode(&provider, &request, &names, None, &origin).await?;
            assert_eq!(encoded.generation.as_ref(), uncached.as_slice());
            drop(encoded);
            drop(names);
            drop(request);
            ledgers.push((work, baseline));
        }
        let encodings = crate::encoded_prefix::PREFIX_ENCODINGS.with(std::cell::Cell::get);
        println!("NATIVE_PREFIX adapter={adapter:?} steps=32 prefix_encodings={encodings}");
        assert_eq!(
            ledgers
                .iter()
                .filter(|(work, baseline)| work.live_allocation_bytes() > *baseline)
                .count(),
            1
        );
        measurements.push((
            encodings,
            ledgers.first().unwrap().0.live_allocation_bytes() > ledgers.first().unwrap().1,
        ));
        drop(provider);
        for (work, baseline) in ledgers {
            assert_eq!(work.live_allocation_bytes(), baseline);
        }
    }
    for (encodings, original_custody) in measurements {
        assert_eq!(
            encodings, 1,
            "new native accounting must fund copies without re-encoding verified static bytes"
        );
        assert!(original_custody);
    }
    Ok(())
}
