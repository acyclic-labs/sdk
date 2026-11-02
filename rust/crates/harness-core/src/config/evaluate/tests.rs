use super::*;
use serde_json::json;

struct Files(&'static [(&'static str, &'static str)]);

impl Tree for Files {
    fn read<'a>(&'a self, path: &'a str) -> BoxFuture<'a, Result<Option<Vec<u8>>>> {
        Box::pin(async move {
            Ok(self
                .0
                .iter()
                .find(|(name, _)| *name == path)
                .map(|(_, text)| text.as_bytes().to_vec()))
        })
    }
}

const TREE: Files = Files(&[
    ("models.ts", "export const fast: string = 'p/fast';"),
    (
        "reexports.ts",
        "export { fast } from './models.ts';\n\
         export * from './models.ts';\n\
         export type { Unused } from './type-only-missing.ts';",
    ),
    ("sys/defaults.json", r#"{"temperature": 0.5}"#),
    ("loop/a.ts", "import './b.ts'; export default 1;"),
    ("loop/b.ts", "import './a.ts'; export default 2;"),
]);

async fn error(source: &str, surface: Surface) -> String {
    match evaluate("agent.ts", source.as_bytes(), &TREE, &json!({}), surface).await {
        Err(Error::Invalid(message)) => message,
        other => panic!("{source}: {other:?}"),
    }
}

#[tokio::test]
async fn layers_fold_values_and_functions_with_static_imports() -> Result<()> {
    let seed = "import type { AgentConfig } from 'sys/types/config';\n\
                export default (base: AgentConfig): AgentConfig => base;";
    let home = "import { fast } from './reexports.ts';\n\
                import defaults from 'sys/defaults.json';\n\
                interface Unused { x: number }\n\
                export default (base: { model: string }) => ({ ...base, model: fast, sampling: defaults });";
    let layers = [
        Layer {
            name: "seed",
            generation: "s1",
            bytes: Some(seed.as_bytes()),
        },
        Layer {
            name: "home",
            generation: "h1",
            bytes: Some(home.as_bytes()),
        },
    ];
    let (value, chain) = compose("agent.ts", &layers, &TREE, json!({"model": "p/base"})).await?;
    assert_eq!(
        value,
        json!({"model": "p/fast", "sampling": {"temperature": 0.5}})
    );
    assert_eq!(
        chain
            .iter()
            .map(|link| (link.layer.as_str(), link.form))
            .collect::<Vec<_>>(),
        [("seed", Form::Function), ("home", Form::Function)]
    );
    // A value export replaces whatever is below it.
    let replaced = evaluate(
        "agent.ts",
        b"export default { model: 'p/x' } satisfies { model: string };",
        &TREE,
        &json!({"model": "p/base", "effort": "high"}),
        Surface::Compose,
    )
    .await?;
    assert_eq!(
        (replaced.value, replaced.form),
        (json!({"model": "p/x"}), Form::Value)
    );
    Ok(())
}

#[tokio::test]
async fn every_unusable_module_is_invalid_with_its_reason() {
    for (source, reason) in [
        ("throw new Error('boom');", "boom"),
        ("export default Date.now();", "Date"),
        ("export default Math.random();", "not a function"),
        ("while (true) {}\nexport default 1;", "instruction budget"),
        (
            "export default () => () => 1;",
            "returned a function, not plain data",
        ),
        (
            "export default { a: [1, Symbol()] };",
            "the default export.a[1] is a symbol",
        ),
        ("export default new Map();", "not a plain object"),
        ("export default NaN;", "finite"),
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
        ("import x from './loop/a.ts'; export default x;", "cycle"),
        ("export default {", "agent.ts"),
    ] {
        let message = error(source, Surface::Compose).await;
        assert!(message.contains(reason), "{source}: {message}");
    }
    // policy.ts is plain data: a function export is refused before it runs.
    match crate::config::admit_policy(b"export default (base) => base;", &TREE).await {
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
            &TREE,
            &json!({}),
            surface,
        )
        .await
        .unwrap();
        assert_eq!(ok.value, json!({"deny": []}));
    }
}

#[test]
fn imports_resolve_like_the_engine_and_stay_in_the_tree() {
    assert_eq!(resolve("agent.ts", "./a.ts").unwrap(), "a.ts");
    assert_eq!(
        resolve("plugins/p/provider.ts", "../../x.ts").unwrap(),
        "x.ts"
    );
    assert_eq!(resolve("a/b.ts", "./c/d.ts").unwrap(), "a/c/d.ts");
    assert_eq!(resolve("a.ts", "sys/types/x.ts").unwrap(), "sys/types/x.ts");
    for specifier in ["../a.ts", "x.ts", "/etc/passwd", "./a/../../b.ts", "sys//x"] {
        assert!(resolve("agent.ts", specifier).is_err(), "{specifier}");
    }
}

// plugins.ts: an entry may be a function of the entry below it, and its
// `config` a function of the plugin's defaults; both fold to data.
#[tokio::test]
async fn nested_functions_fold_over_the_base_at_their_path() -> Result<()> {
    let base = json!({
        "compaction": {"enabled": true, "config": {"notify": 0.75, "reuse": "ancestors"}},
        "goal": {"enabled": true, "config": {"maxLoops": 3}},
    });
    let source = "export default (base) => ({
        ...base,
        compaction: { ...base.compaction, config: (c) => ({ ...c, notify: 0.7 }) },
        goal: (entry) => ({ ...entry, enabled: false }),
        extra: { config: (c) => ({ seen: c === undefined }) },
    });";
    let evaluated = evaluate(
        "plugins.ts",
        source.as_bytes(),
        &TREE,
        &base,
        Surface::Compose,
    )
    .await?;
    assert_eq!(
        evaluated.value,
        json!({
            "compaction": {"enabled": true, "config": {"notify": 0.7, "reuse": "ancestors"}},
            "goal": {"enabled": false, "config": {"maxLoops": 3}},
            "extra": {"config": {"seen": true}},
        })
    );
    Ok(())
}

#[test]
fn json_module_exports_preserve_json_property_semantics() -> Result<()> {
    let source = br#"{"__proto__":{"x":1},"escaped":"\\\""}"#;
    let module = compile_module("data.json", source).map_err(Error::Invalid)?;
    let modules = vec![("data.json".to_owned(), module)];
    let (value, _) = run(&modules, b"{}", Surface::Data).map_err(Error::Invalid)?;
    assert_eq!(value["__proto__"]["x"], 1);
    Ok(())
}
