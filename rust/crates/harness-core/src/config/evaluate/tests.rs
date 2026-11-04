use super::*;
use serde_json::json;

struct Missing;

impl Tree for Missing {
    fn read<'a>(&'a self, _: &'a str) -> BoxFuture<'a, Result<Option<acyclic_stream::OwnedBytes>>> {
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
        ("while (true) {}\nexport default 1;", "instruction budget"),
        (
            "export default { a: [1, Symbol()] };",
            "the default export.a[1] is a symbol",
        ),
        ("export const x = 1;", "export default"),
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
}
