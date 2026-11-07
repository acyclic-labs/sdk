//! The existing generated oracle also retains the production loader's cut.
use super::*;

pub(super) async fn check(bytes: &[u8]) -> std::result::Result<(), String> {
    let source = b"import input from 'sys/input.json'; export default input;";
    let value = json!({"original": bytes});
    let encoded = serde_json::to_vec(&value).map_err(|error| error.to_string())?;
    let mut identities = Vec::new();
    // The same unchanged root observes absence, malformed data and admitted
    // data. Both refusals need the actual read cut, not just a root version.
    for input in [None, Some(Vec::new()), Some(encoded)] {
        let tree = Files {
            input,
            cycle: false,
        };
        let prepared = prepare("agent.ts", source, &tree)
            .await
            .map_err(|error| error.to_string())?;
        let expected = [
            ModuleRead {
                path: "agent.ts".into(),
                digest: Some(blake3::hash(source).into()),
            },
            ModuleRead {
                path: "sys/input.json".into(),
                digest: tree.input.as_ref().map(|bytes| blake3::hash(bytes).into()),
            },
        ];
        if prepared.reads() != expected {
            return Err("original dependency observation was discarded".into());
        }
        let cache = Cache::default();
        let cold = prepared
            .evaluate_cached(&Value::Null, Surface::Compose, &cache)
            .map_err(|error| error.to_string())?;
        let identity = cold.identity();
        if identities.contains(&identity) {
            return Err("settings input identity aliased a different dependency cut".into());
        }
        identities.push(identity);
        let expected = tree
            .input
            .as_ref()
            .filter(|bytes| !bytes.is_empty())
            .map(|_| (&value, Form::Value));
        match (cold.result(), expected) {
            (Ok(actual), Some((value, form))) if actual.value == *value && actual.form == form => {}
            (Err(Error::Invalid(_)), None) => {}
            other => return Err(format!("retained dependency decision changed: {other:?}")),
        }
        let entries = VM_ENTRIES.get();
        let warm = prepared
            .evaluate_cached(&Value::Null, Surface::Compose, &cache)
            .map_err(|error| error.to_string())?;
        if warm.identity() != identity
            || observed(warm.into_result()) != observed(cold.into_result())
            || VM_ENTRIES.get() != entries
        {
            return Err("retained cut lost its original decision or re-entered QuickJS".into());
        }
        for (base, surface) in [
            (json!({"base": bytes}), Surface::Compose),
            (Value::Null, Surface::Data),
        ] {
            if prepared
                .evaluate_cached(&base, surface, &cache)
                .map_err(|error| error.to_string())?
                .identity()
                == identity
            {
                return Err("retained settings identity discarded its base or surface".into());
            }
        }
        // Keep original bytes and imports but change only the supplied root.
        let renamed = prepare("another.ts", source, &tree)
            .await
            .map_err(|error| error.to_string())?;
        if renamed
            .evaluate_cached(&Value::Null, Surface::Compose, &cache)
            .map_err(|error| error.to_string())?
            .identity()
            == identity
        {
            return Err("retained settings identity discarded its root".into());
        }
        // Parsing the root may fail before any dependency is read. Its actual
        // bytes and semantic failure still belong to the same public API.
        let invalid = prepare("agent.ts", b"export default {", &tree)
            .await
            .map_err(|error| error.to_string())?;
        if invalid.reads().len() != 1 {
            return Err("invalid root invented a dependency observation".into());
        }
        let invalid = invalid
            .evaluate_cached(&Value::Null, Surface::Compose, &cache)
            .map_err(|error| error.to_string())?;
        if invalid.identity() == identity || !matches!(invalid.result(), Err(Error::Invalid(_))) {
            return Err("invalid root lost its original semantic decision".into());
        }
    }
    Ok(())
}
