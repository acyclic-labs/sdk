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
    let home = "import { fast } from './models.ts';\n\
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
