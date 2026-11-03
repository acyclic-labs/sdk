use super::*;
use serde_json::json;

struct Missing;

impl Tree for Missing {
    fn read<'a>(&'a self, _: &'a str) -> BoxFuture<'a, Result<Option<Vec<u8>>>> {
        Box::pin(std::future::ready(Ok(None)))
    }
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
