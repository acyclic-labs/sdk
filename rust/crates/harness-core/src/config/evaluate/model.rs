//! Generated settings exercise the original evaluator and its original cache.
use super::*;
use serde_json::json;

struct Files(Vec<u8>);

impl Tree for Files {
    fn read<'a>(&'a self, path: &'a str) -> BoxFuture<'a, Result<Option<Vec<u8>>>> {
        Box::pin(async move { Ok((path == "sys/input.json").then(|| self.0.clone())) })
    }
}

fn observed(result: Result<Evaluated>) -> std::result::Result<Evaluated, String> {
    result.map_err(|error| error.to_string())
}

fn agrees(
    actual: &std::result::Result<Evaluated, String>,
    expected: Option<&(Value, Form)>,
) -> std::result::Result<(), String> {
    match (actual, expected) {
        (Ok(actual), Some((value, form))) if actual.value == *value && actual.form == *form => {
            Ok(())
        }
        (Err(_), None) => Ok(()),
        _ => Err(format!(
            "plain-data oracle: {actual:?}, expected {expected:?}"
        )),
    }
}

/// No process-global cache growth: each input owns one cache, shared across
/// changes of source, imported content, base and surface at the same path.
pub async fn check(bytes: &[u8]) -> std::result::Result<(), String> {
    let cache = Mutex::new(HashMap::new());
    let mode = bytes.first().copied().unwrap_or_default() % 10;
    let literal = json!({
        "text": String::from_utf8_lossy(bytes),
        "bytes": bytes,
    });
    for revision in 0..2 {
        // Change one cache-key dimension at a time. Otherwise a changed
        // base could hide a missing source or imported-content dependency.
        let base_revision = if mode == 1 || mode == 2 { revision } else { 0 };
        let base = json!({"base": base_revision, "nested": {"base": base_revision}});
        let imported = json!({"imported": revision, "value": literal});
        let tree = Files(serde_json::to_vec(&imported).map_err(|error| error.to_string())?);
        let encoded = serde_json::to_string(&literal).map_err(|error| error.to_string())?;
        let (source, value, form, functions) = match mode {
            0 => (
                format!("export default ({encoded});"),
                literal.clone(),
                Form::Value,
                false,
            ),
            1 => (
                format!("export default (base) => ({{...base, value: {encoded}}});"),
                json!({"base": revision, "nested": {"base": revision}, "value": literal}),
                Form::Function,
                true,
            ),
            2 => (
                "export default {nested: (base) => ({...base, added: true})};".to_owned(),
                json!({"nested": {"base": revision, "added": true}}),
                Form::Value,
                true,
            ),
            3 => (
                "import input from 'sys/input.json'; export default input;".to_owned(),
                imported,
                Form::Value,
                false,
            ),
            4 => (
                [
                    "export default new Map();",
                    "export default Symbol();",
                    "export default NaN;",
                    "export default () => () => 1;",
                ][bytes.get(1).copied().unwrap_or_default() as usize % 4]
                    .to_owned(),
                Value::Null,
                Form::Value,
                false,
            ),
            5 => (
                "export default Date.now();".to_owned(),
                Value::Null,
                Form::Value,
                false,
            ),
            6 => (
                "export default Math.random();".to_owned(),
                Value::Null,
                Form::Value,
                false,
            ),
            7 => (
                format!("export default {{revision: {revision}, value: {encoded}}};"),
                json!({"revision": revision, "value": literal}),
                Form::Value,
                false,
            ),
            8 => (
                [
                    "throw {code: 1};",
                    "throw [1];",
                    "throw Symbol('x');",
                    "throw (() => 1);",
                ][bytes.get(1).copied().unwrap_or_default() as usize % 4]
                    .to_owned(),
                Value::Null,
                Form::Value,
                false,
            ),
            _ => (
                "throw 'settings refused';".to_owned(),
                Value::Null,
                Form::Value,
                false,
            ),
        };
        for surface in [Surface::Compose, Surface::Data] {
            let expected = (mode < 4 || mode == 7)
                .then(|| (value.clone(), form))
                .filter(|_| !(functions && surface == Surface::Data));
            let cold = observed(
                evaluate_cached("agent.ts", source.as_bytes(), &tree, &base, surface, &cache).await,
            );
            agrees(&cold, expected.as_ref())?;
            if mode == 8
                && cold
                    .as_ref()
                    .err()
                    .is_some_and(|error| error.contains("0x"))
            {
                return Err("a non-scalar throw retained a native address".into());
            }
            let warm = observed(
                evaluate_cached("agent.ts", source.as_bytes(), &tree, &base, surface, &cache).await,
            );
            let fresh = observed(
                evaluate_cached(
                    "agent.ts",
                    source.as_bytes(),
                    &tree,
                    &base,
                    surface,
                    &Mutex::new(HashMap::new()),
                )
                .await,
            );
            if cold != warm || warm != fresh {
                return Err(format!(
                    "cache/fresh determinism: cold={cold:?}, warm={warm:?}, fresh={fresh:?}"
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn generated_settings_match_fresh_evaluation_and_reject_corrupt_cache() {
        for mode in 0..10 {
            for variant in 0..4 {
                check(&[mode, variant, b'\"', b'\\', 0, 255]).await.unwrap();
            }
        }
        // Corrupt the actual retained cache entry, then use the same oracle
        // as the positive schedules; no alternate evaluator is involved.
        let cache = Mutex::new(HashMap::new());
        let tree = Files(Vec::new());
        let base = Value::Null;
        let source = b"export default {admitted: true};";
        let original = evaluate_cached("agent.ts", source, &tree, &base, Surface::Data, &cache)
            .await
            .unwrap();
        for outcome in cache.lock().unwrap().values_mut() {
            *outcome = Ok(Arc::new((json!({"admitted": false}), Form::Value)));
        }
        let corrupt = observed(
            evaluate_cached("agent.ts", source, &tree, &base, Surface::Data, &cache).await,
        );
        assert!(agrees(&corrupt, Some(&(original.value, original.form))).is_err());
    }
}
