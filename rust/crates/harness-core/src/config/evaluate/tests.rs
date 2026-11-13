use super::*;
use serde_json::json;

struct Missing;

struct Metered<'a>(&'a crate::filesystem::PreparationIo);

impl Tree for Metered<'_> {
    fn preparation_io(&self) -> Option<&crate::filesystem::PreparationIo> {
        Some(self.0)
    }
    fn read<'a>(&'a self, _: &'a str) -> BoxFuture<'a, Result<Option<acyclic_stream::OwnedBytes>>> {
        Box::pin(async { Ok(None) })
    }
}

impl Tree for Missing {
    fn read<'a>(&'a self, _: &'a str) -> BoxFuture<'a, Result<Option<acyclic_stream::OwnedBytes>>> {
        Box::pin(std::future::ready(Ok(None)))
    }
}

#[tokio::test]
async fn native_evaluation_budget_refusals_keep_their_cause_and_do_not_poison_the_cache()
-> Result<()> {
    let source = b"export default {model: 'example/' + 'a'.repeat(65536)};";
    let base = Value::Null;
    let cache = Mutex::new(None);
    let cancel = acyclic_fs::CancellationToken::new();
    let make_work = |bytes| {
        acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::WorkBudget {
                peak_allocation_bytes: bytes,
                ..acyclic_fs::WorkBudget::UNBOUNDED
            },
            &cancel,
        )
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })
    };
    let tight = make_work(acyclic_fs::NativeMaterializationWork::accounting_state_bytes() + 4096)?;
    let io = crate::filesystem::PreparationIo::from_native_work(&tight)?;
    let baseline = tight.live_allocation_bytes();
    let error = match evaluate_cached(
        "settings.ts",
        source,
        &Metered(&io),
        &base,
        Surface::Data,
        &cache,
    )
    .await
    {
        Err(error) => error,
        Ok(_) => panic!("the VM must use its caller's allocation allowance"),
    };
    assert!(matches!(
        error,
        Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Work(
                acyclic_fs::WorkError::BudgetExceeded {
                    counter: "peak_allocation_bytes",
                    ..
                }
            ),
            ..
        }
    ));
    assert_eq!(tight.live_allocation_bytes(), baseline);
    let roomy = make_work(16 * 1024 * 1024)?;
    let io = crate::filesystem::PreparationIo::from_native_work(&roomy)?;
    let admitted = evaluate_cached(
        "settings.ts",
        source,
        &Metered(&io),
        &base,
        Surface::Data,
        &cache,
    )
    .await?;
    assert_eq!(admitted.value["model"].as_str().unwrap().len(), 65544);
    let before_decode = roomy.live_allocation_bytes();
    let typed = admitted.value.decode::<crate::config::AgentConfig>()?;
    assert_eq!(typed.model.len(), 65544);
    assert!(roomy.live_allocation_bytes() >= before_decode + 65544);
    drop(typed);
    assert_eq!(roomy.live_allocation_bytes(), before_decode);
    drop(admitted);
    let retained = roomy.live_allocation_bytes();
    assert!(
        retained >= 65536,
        "the cached result retains its original owner"
    );
    let borrowed = evaluate_cached(
        "settings.ts",
        source,
        &Metered(&crate::filesystem::PreparationIo::from_native_work(&tight)?),
        &base,
        Surface::Data,
        &cache,
    )
    .await?;
    let error = borrowed
        .value
        .decode::<crate::config::AgentConfig>()
        .expect_err("an owned decoded copy must use the receiving allowance");
    assert!(matches!(error, Error::NativeWork { .. }));
    assert_eq!(tight.live_allocation_bytes(), baseline);
    let stopped = acyclic_fs::CancellationToken::new();
    let stopped_work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(1_000_000, 16 * 1024 * 1024),
        &stopped,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let stopped_io = crate::filesystem::PreparationIo::from_native_work(&stopped_work)?;
    stopped.cancel();
    let cancelled = evaluate_cached(
        "settings.ts",
        source,
        &Metered(&stopped_io),
        &base,
        Surface::Data,
        &cache,
    )
    .await
    .expect_err("a cached value must not bypass cancellation");
    assert_eq!(cancelled.code(), crate::wire::ErrorCode::Cancelled);
    *cache.lock().unwrap() = None;
    assert_eq!(
        roomy.live_allocation_bytes(),
        retained,
        "the returned view still owns the original cached storage"
    );
    drop(borrowed);
    assert!(
        roomy.live_allocation_bytes() < retained,
        "eviction retires cached ownership"
    );
    Ok(())
}

#[tokio::test]
async fn composition_retains_its_value_after_cache_eviction() -> Result<()> {
    let cancel = acyclic_fs::CancellationToken::new();
    let budget = acyclic_fs::performance::work_budget(1_000_000, 16 * 1024 * 1024);
    let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancel)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let baseline = work.live_allocation_bytes();
    let io = crate::filesystem::PreparationIo::from_native_work(&work)?;
    let tree = Metered(&io);
    let cache = Mutex::new(None);
    let layers = [
        Layer {
            name: "seed",
            generation: "one",
            bytes: Some(b"export default {text: 'x'.repeat(65536)};"),
        },
        Layer {
            name: "home",
            generation: "two",
            bytes: Some(b"export default base => ({...base, applied:true});"),
        },
    ];
    let (value, _) =
        composition::fold("settings.ts", &layers, &tree, Value::Null, None, &cache).await??;
    *cache.lock().unwrap() = None;
    assert_eq!(value["text"].as_str().unwrap().len(), 65536);
    assert!(work.live_allocation_bytes() >= baseline + 65536);
    assert!(
        work.claim(
            budget.peak_allocation_bytes - baseline - 65536 + 1,
            1,
            acyclic_fs::WorkCounters::UNCHARGED
        )
        .is_err()
    );
    // A consuming API cannot detach a bounded payload from its owner.
    assert!(matches!(
        value.into_unbounded(),
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            ..
        })
    ));
    assert_eq!(work.live_allocation_bytes(), baseline);
    Ok(())
}

#[tokio::test]
async fn invalid_native_result_retains_its_cached_decision() -> Result<()> {
    let cancel = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(1_000_000, 16 * 1024 * 1024),
        &cancel,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = crate::filesystem::PreparationIo::from_native_work(&work)?;
    let tree = Metered(&io);
    let source = format!(
        "let value=0; for(let i=0;i<{};i++) value=[value]; export default value;",
        crate::executor::json::RECORD_MAXIMUM_DEPTH + 2
    );
    for native_first in [false, true] {
        let cache = Mutex::new(None);
        let before = model::entries();
        let first_tree: &dyn Tree = if native_first { &tree } else { &Missing };
        let second_tree: &dyn Tree = if native_first { &Missing } else { &tree };
        let first = evaluate_decision_cached(
            "settings.ts",
            source.as_bytes(),
            first_tree,
            &Value::Null,
            Surface::Data,
            &cache,
        )
        .await?;
        assert!(matches!(
            first.result(),
            Err(Error::NativeJsonAdmission {
                kind: crate::executor::json::JsonInputError::Depth,
                ..
            })
        ));
        let second = evaluate_decision_cached(
            "settings.ts",
            source.as_bytes(),
            second_tree,
            &Value::Null,
            Surface::Data,
            &cache,
        )
        .await?;
        assert_eq!(first.identity(), second.identity());
        assert!(matches!(
            second.result(),
            Err(Error::NativeJsonAdmission {
                kind: crate::executor::json::JsonInputError::Depth,
                ..
            })
        ));
        assert_eq!(model::entries(), before + 1);
    }
    Ok(())
}

async fn error(source: &str, surface: Surface) -> String {
    match evaluate("agent.ts", source.as_bytes(), &Missing, &json!({}), surface).await {
        Err(Error::Invalid(message)) => message,
        other => panic!("{source}: {other:?}"),
    }
}

#[tokio::test]
async fn every_unusable_module_is_invalid_with_its_reason() {
    for (source, reason) in [
        ("throw new Error('boom');", "boom"),
        ("while (true) {}\nexport default 1;", "instruction budget"),
        (
            "export default { a: [1, Symbol()] };",
            "the default export.a[1] is a symbol",
        ),
        ("export const x = 1;", "export default"),
        (
            "export default await new Promise(() => {});",
            "never settles",
        ),
        ("export default import('./models.ts');", "import()"),
        (
            r"import '\uD800'; export default 0;",
            "module paths must be valid Unicode",
        ),
        (
            "import x from 'node:fs'; export default x;",
            "imports node:fs",
        ),
        (
            "import x from '../up.ts'; export default x;",
            "imports ../up.ts",
        ),
        (
            "import x from './missing.ts'; export default x;",
            "no layer holds",
        ),
        ("export default {", "agent.ts"),
    ] {
        let message = error(source, Surface::Compose).await;
        assert!(message.contains(reason), "{source}: {message}");
    }
    // policy.ts is plain data: a function export is refused before it runs.
    match crate::config::admit_policy(b"export default (base) => base;", &Missing).await {
        Err(Error::Invalid(message)) => assert!(
            message.starts_with("policy.ts: ") && message.contains("plain data, not a function"),
            "{message}"
        ),
        other => panic!("{other:?}"),
    }
    // Plain data means no function at any depth.
    let message = error("export default { deny: [], x: () => 1 };", Surface::Data).await;
    assert!(
        message.contains("the default export.x must be plain data"),
        "{message}"
    );
    // Negative control: the same data is accepted on both surfaces.
    for surface in [Surface::Compose, Surface::Data] {
        let ok = evaluate(
            "policy.ts",
            b"export default { deny: [] };",
            &Missing,
            &json!({}),
            surface,
        )
        .await
        .unwrap();
        assert_eq!(ok.value, json!({"deny": []}));
    }
}

#[test]
fn json_module_exports_preserve_json_property_semantics() -> Result<()> {
    let source = br#"{"__proto__":{"x":1},"escaped":"\\\""}"#;
    let module = compile_module("data.json", source).map_err(Error::Invalid)?;
    let modules = vec![("data.json".to_owned(), module)];
    let (value, _) = run(
        &modules,
        b"{}",
        Surface::Data,
        crate::javascript_allocator::EngineAllocator::new(None),
    )
    .map_err(Error::Invalid)?;
    assert_eq!(value.value["__proto__"]["x"], 1);
    Ok(())
}
