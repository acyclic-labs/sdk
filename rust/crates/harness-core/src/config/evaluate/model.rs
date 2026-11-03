//! Generated settings exercise the original evaluator and its original cache.
use super::*;
use serde_json::json;
use std::cell::Cell;

thread_local! {
    static VM_ENTRIES: Cell<usize> = const { Cell::new(0) };
    static DEPENDENCY_CHECKS: Cell<usize> = const { Cell::new(0) };
}

pub(super) fn entered() {
    VM_ENTRIES.set(VM_ENTRIES.get().wrapping_add(1));
}

pub(super) fn dependency_checked() {
    DEPENDENCY_CHECKS.set(DEPENDENCY_CHECKS.get().wrapping_add(1));
}

struct Files {
    input: Vec<u8>,
    cycle: bool,
}

impl Tree for Files {
    fn read<'a>(&'a self, path: &'a str) -> BoxFuture<'a, Result<Option<Vec<u8>>>> {
        Box::pin(async move {
            Ok(match path {
                "sys/input.json" => Some(self.input.clone()),
                "sys/a.ts" => Some(b"import b from './b.ts'; import input from './input.json'; export default [b, input]; export {input};".to_vec()),
                "sys/b.ts" => Some(if self.cycle {
                    b"import a from './a.ts'; export default a;".to_vec()
                } else {
                    b"import input from './input.json'; export default input;".to_vec()
                }),
                _ => None,
            })
        })
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

// JSON numbers pass through ECMAScript Number, including integer rounding and
// negative-zero normalization. Compare that numeric domain, retaining every
// object key, array position and non-numeric value exactly.
fn json_equivalent(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Number(left), Value::Number(right)) => left.as_f64() == right.as_f64(),
        (Value::Array(left), Value::Array(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .zip(right)
                    .all(|(left, right)| json_equivalent(left, right))
        }
        (Value::Object(left), Value::Object(right)) => {
            left.len() == right.len()
                && left.iter().all(|(key, left)| {
                    right
                        .get(key)
                        .is_some_and(|right| json_equivalent(left, right))
                })
        }
        _ => left == right,
    }
}

async fn raw_source(bytes: &[u8]) -> std::result::Result<(), String> {
    let text = String::from_utf8_lossy(bytes);
    let specifier = serde_json::to_string(&text).map_err(|error| error.to_string())?;
    let tree = Files {
        input: specifier.as_bytes().to_vec(),
        cycle: false,
    };
    let dynamic = format!("export default import({specifier});");
    // Parse arbitrary programs without executing their untrusted control flow.
    // Accepted TypeScript must emit JavaScript admitted by the same production
    // parser, with the same resolved imports after type erasure.
    for path in ["raw.ts", "raw.js"] {
        let literal = if path.ends_with(".ts") {
            format!("export default ({specifier} satisfies string);")
        } else {
            format!("export default {specifier};")
        };
        strip(path, literal.as_bytes())
            .map_err(|error| format!("encoded source literal admission: {error}"))?;
        // The same valid literal becomes an import specifier. Both public
        // surfaces must refuse that syntax before entering the engine, even
        // when executing it would independently fail to find its module.
        for surface in [Surface::Compose, Surface::Data] {
            let entries = VM_ENTRIES.get();
            let refusal = evaluate_cached(
                path,
                dynamic.as_bytes(),
                &tree,
                &Value::Null,
                surface,
                &Cache::default(),
            )
            .await;
            if !matches!(refusal, Err(Error::Invalid(_))) || VM_ENTRIES.get() != entries {
                return Err("dynamic import escaped source admission".into());
            }
        }
        // Resolve generated names through the actual closure and QuickJS
        // linker, rather than restating the resolver's implementation. The
        // input remains data; only this fixed import program is executed.
        let depth = usize::from(bytes.first().copied().unwrap_or_default() % 8);
        let importer = format!("{}{path}", "directory/".repeat(depth));
        let relative = format!(
            "{}sys/input.json",
            if depth == 0 {
                "./".into()
            } else {
                "../".repeat(depth)
            }
        );
        for name in ["sys/input.json", relative.as_str()] {
            let name = serde_json::to_string(name).map_err(|error| error.to_string())?;
            let source = format!("import input from {name}; export default input;");
            let actual = evaluate_cached(
                &importer,
                source.as_bytes(),
                &tree,
                &Value::Null,
                Surface::Data,
                &Cache::default(),
            )
            .await
            .map_err(|error| format!("static import admission: {error}"))?;
            if actual.value != Value::String(text.to_string()) || actual.form != Form::Value {
                return Err("static import changed its original data".into());
            }
        }
        // A root escape or noncanonical name must fail before any tree read
        // or VM entry, even when the engine could normalize it to a held file.
        for name in [
            format!("{}sys/input.json", "../".repeat(depth + 1)),
            "input.json".into(),
            "/sys/input.json".into(),
            "./a/../../input.json".into(),
            "sys//input.json".into(),
            "sys/./input.json".into(),
            "sys/a/../input.json".into(),
        ] {
            let name = serde_json::to_string(&name).map_err(|error| error.to_string())?;
            let source = format!("import input from {name}; export default input;");
            if strip(&importer, source.as_bytes()).is_ok() {
                return Err("noncanonical static import escaped source admission".into());
            }
        }
        if let Ok(module) = strip(path, bytes) {
            let emitted = strip("raw.js", module.script.as_bytes())
                .map_err(|error| format!("emitted JavaScript admission: {error}"))?;
            if module.imports != emitted.imports {
                return Err("emitted JavaScript changed resolved imports".into());
            }
        }
    }
    let expected = match serde_json::from_slice::<Value>(bytes) {
        Ok(value) => Some(value),
        // IgnoredAny admits numbers outside serde's finite Value domain. A
        // later duplicate property can overwrite one before the production
        // JSON fold sees it. Use the original engine parser for that domain;
        // the fixed program contains only an encoded data string.
        Err(_) if serde_json::from_slice::<serde::de::IgnoredAny>(bytes).is_ok() => {
            let source = format!("export default JSON.parse({specifier});");
            evaluate_cached(
                "raw-reference.js",
                source.as_bytes(),
                &tree,
                &Value::Null,
                Surface::Data,
                &Cache::default(),
            )
            .await
            .ok()
            .map(|result| result.value)
        }
        Err(_) => None,
    };
    let actual = evaluate_cached(
        "raw.json",
        bytes,
        &tree,
        &Value::Null,
        Surface::Data,
        &Cache::default(),
    )
    .await;
    match (expected, actual) {
        (Some(expected), Ok(actual))
            if actual.form == Form::Value && json_equivalent(&expected, &actual.value) =>
        {
            Ok(())
        }
        (None, Err(_)) => Ok(()),
        (expected, actual) => Err(format!(
            "raw JSON semantics: {actual:?}, expected {expected:?}"
        )),
    }
}

/// No process-global cache growth: each input owns one cache, shared across
/// changes of source, imported content, base and surface at the same path.
pub async fn check(bytes: &[u8]) -> std::result::Result<(), String> {
    raw_source(bytes).await?;
    let cache = Cache::default();
    let mode = bytes.first().copied().unwrap_or_default() % 13;
    let pending = mode == 11 && bytes.get(1).copied().unwrap_or_default() & 1 == 0;
    let cycle = mode == 12 && bytes.get(1).copied().unwrap_or_default() & 1 != 0;
    let literal = json!({
        "text": String::from_utf8_lossy(bytes),
        "bytes": bytes,
    });
    for surface in [Surface::Compose, Surface::Data] {
        for revision in 0..2 {
            // Change one cache-key dimension at a time. Otherwise a changed
            // base could hide a missing source or imported-content dependency.
            let base_revision = if mode == 1 { revision } else { 0 };
            let base = if mode == 2 {
                json!({
                    "nested": {"config": {"base": revision}},
                    "items": [{"base": revision}, revision],
                    "mismatch": {"0": revision},
                    "present": null,
                })
            } else {
                json!({"base": base_revision, "nested": {"base": base_revision}})
            };
            let imported = json!({"imported": revision, "value": literal});
            let tree = Files {
                input: serde_json::to_vec(&imported).map_err(|error| error.to_string())?,
                cycle,
            };
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
                2 => {
                    let (prefix, suffix, form) =
                        if bytes.get(1).copied().unwrap_or_default() & 1 == 0 {
                            ("", "", Form::Value)
                        } else {
                            ("(base) => (", ")", Form::Function)
                        };
                    (
                        format!(
                            "export default {prefix}{{
                                nested: {{config: (c) => ({{...c, added: {encoded}}})}},
                                items: [(c) => ({{...c, added: true}}), (c) => c + 1],
                                mismatch: [(c) => c === undefined],
                                present: (c) => c === null,
                                constructor: (c) => c === undefined,
                                extra: {{config: (c) => ({{seen: c === undefined}})}},
                            }}{suffix};"
                        ),
                        json!({
                            "nested": {"config": {"base": revision, "added": literal}},
                            "items": [{"base": revision, "added": true}, revision + 1],
                            "mismatch": [true],
                            "present": true,
                            "constructor": true,
                            "extra": {"config": {"seen": true}},
                        }),
                        form,
                        true,
                    )
                }
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
                        "export default new Set();",
                        "export default 1n;",
                        "export default Infinity;",
                        "const value = {}; value.self = value; export default value;",
                    ][bytes.get(1).copied().unwrap_or_default() as usize % 8]
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
                9 => (
                    "throw 'settings refused';".to_owned(),
                    Value::Null,
                    Form::Value,
                    false,
                ),
                10 => (
                    [
                        "Number.isFinite = () => true; export default NaN;",
                        "Object.getPrototypeOf = () => Object.prototype; export default new Map();",
                        "Object.keys = () => []; export default {bad: NaN};",
                        "Array.prototype.map = () => []; export default [NaN];",
                        "export default () => { Number.isFinite = () => true; return NaN; };",
                        "export default {nested: () => { Number.isFinite = () => true; return NaN; }};",
                        "export default () => { Object.getPrototypeOf = () => Object.prototype; return new Map(); };",
                        "export default {nested: () => { Object.getPrototypeOf = () => Object.prototype; return new Map(); }};",
                        "JSON.stringify = () => 'null'; export default NaN;",
                        "JSON.parse = () => ({}); export default (base) => base;",
                        "Object.prototype.toJSON = () => null; export default [NaN];",
                        "Function.prototype.call = () => false; export default {bad: NaN};",
                        "globalThis.Number = {isFinite: () => true}; export default NaN;",
                        "export default Object.assign([NaN], {map: () => []});",
                        "export default {get value() { return 1; }};",
                        "export default [Object.create(null, {value: {enumerable: true, get() { return 1; }}})];",
                    ][bytes.get(1).copied().unwrap_or_default() as usize % 16]
                        .to_owned(),
                    Value::Null,
                    Form::Value,
                    false,
                ),
                12 => (
                    "import a from 'sys/a.ts'; import b from 'sys/b.ts'; export default [a, b];".to_owned(),
                    json!([[imported, imported], imported]),
                    Form::Value,
                    false,
                ),
                _ => (
                    if pending {
                        "await new Promise(() => {}); export default null;".to_owned()
                    } else {
                        format!("export default await Promise.resolve(({encoded}));")
                    },
                    literal.clone(),
                    Form::Value,
                    false,
                ),
            };
            let expected =
                (mode < 4 || mode == 7 || (mode == 11 && !pending) || (mode == 12 && !cycle))
                    .then(|| (value.clone(), form))
                    .filter(|_| !(functions && surface == Surface::Data));
            let dependency_checks = DEPENDENCY_CHECKS.get();
            let cold = observed(
                evaluate_cached("agent.ts", source.as_bytes(), &tree, &base, surface, &cache).await,
            );
            agrees(&cold, expected.as_ref())?;
            if pending
                && !cold
                    .as_ref()
                    .err()
                    .is_some_and(|error| error.contains("a top-level await never settles"))
            {
                return Err("top-level await lost its pending-producer refusal".into());
            }
            if mode == 8
                && cold
                    .as_ref()
                    .err()
                    .is_some_and(|error| error.contains("0x"))
            {
                return Err("a non-scalar throw retained a native address".into());
            }
            // Files::read is immediately ready, so this synchronous cache/VM
            // sequence cannot yield to another evaluation on the same thread.
            let entries = VM_ENTRIES.get();
            let warm = observed(
                evaluate_cached("agent.ts", source.as_bytes(), &tree, &base, surface, &cache).await,
            );
            if VM_ENTRIES.get() != entries {
                return Err("warm cache re-entered QuickJS".into());
            }
            let fresh = observed(
                evaluate_cached(
                    "agent.ts",
                    source.as_bytes(),
                    &tree,
                    &base,
                    surface,
                    &Cache::default(),
                )
                .await,
            );
            if VM_ENTRIES.get() != entries.wrapping_add(usize::from(!cycle)) {
                return Err("fresh evaluation entered QuickJS before closure admission or skipped execution".into());
            }
            // The graph has five edges, or reaches its back edge after three.
            // Cold, warm and fresh each admit the original dependency closure.
            if mode == 12
                && DEPENDENCY_CHECKS.get().wrapping_sub(dependency_checks)
                    != 3 * if cycle { 3 } else { 5 }
            {
                return Err("dependency membership work grew with active depth".into());
            }
            if cold != warm || warm != fresh {
                return Err(format!(
                    "cache/fresh determinism: cold={cold:?}, warm={warm:?}, fresh={fresh:?}"
                ));
            }
            if mode == 3 && surface == Surface::Compose {
                // Compose through the public entry. A replacement import,
                // optional off cut, absent layer and typed callback must
                // preserve their original value and provenance together.
                let upper = b"import type { AgentConfig } from 'sys/types/config';
                    import { input } from 'sys/a.ts';
                    interface Unused { x: number }
                    export default (base: AgentConfig) => ({below: base, named: input});";
                let cut = bytes.get(1).copied().unwrap_or_default() & 1 != 0;
                let generation = format!("revision-{revision}");
                let layers = [
                    Layer {
                        name: "seed",
                        generation: &generation,
                        bytes: Some(source.as_bytes()),
                    },
                    Layer {
                        name: "absent",
                        generation: "absent",
                        bytes: None,
                    },
                    Layer {
                        name: "cut",
                        generation: "cut",
                        bytes: cut.then_some(b"off: true".as_slice()),
                    },
                    Layer {
                        name: "home",
                        generation: &generation,
                        bytes: Some(upper.as_slice()),
                    },
                ];
                let below = if cut { &base } else { &value };
                let upper_result = evaluate("agent.ts", upper, &tree, below, surface)
                    .await
                    .map_err(|error| format!("layer callback admission: {error}"))?;
                let (composed, chain) = compose("agent.ts", &layers, &tree, base.clone())
                    .await
                    .map_err(|error| format!("layer composition admission: {error}"))?;
                if composed != json!({"below": below, "named": value})
                    || composed != upper_result.value
                {
                    return Err("layer composition lost its original lower value".into());
                }
                let expected = [
                    ("seed", fresh.as_ref().map_err(Clone::clone)?),
                    ("home", &upper_result),
                ]
                .into_iter()
                .skip(usize::from(cut));
                if chain.len() != expected.len()
                    || chain.iter().zip(expected).any(|(link, (name, module))| {
                        link.layer != name
                            || link.generation != generation
                            || link.form != module.form
                            || link.module_digest
                                != blake3::Hash::from_bytes(module.digest).to_hex().as_str()
                    })
                {
                    return Err("layer provenance lost its original source binding".into());
                }
            }
            if functions && surface == Surface::Compose {
                // Keep source and base unchanged while changing only the
                // surface. Restore the original entry before the next
                // revision so eviction cannot hide a missing content key.
                let data = observed(
                    evaluate_cached(
                        "agent.ts",
                        source.as_bytes(),
                        &tree,
                        &base,
                        Surface::Data,
                        &cache,
                    )
                    .await,
                );
                agrees(&data, None)?;
                let restored = observed(
                    evaluate_cached("agent.ts", source.as_bytes(), &tree, &base, surface, &cache)
                        .await,
                );
                if restored != cold {
                    return Err("surface replacement changed the original evaluation".into());
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn generated_settings_match_fresh_evaluation() {
        for source in [
            include_bytes!("../../../../harness-fuzz/corpus/config_evaluation/json-properties")
                .as_slice(),
            include_bytes!("../../../../harness-fuzz/corpus/config_evaluation/json-numbers")
                .as_slice(),
            include_bytes!(
                "../../../../harness-fuzz/corpus/config_evaluation/json-overwritten-overflow"
            )
            .as_slice(),
            include_bytes!("../../../../harness-fuzz/corpus/config_evaluation/json-overflow")
                .as_slice(),
            include_bytes!("../../../../harness-fuzz/corpus/config_evaluation/raw-typescript")
                .as_slice(),
            include_bytes!("../../../../harness-fuzz/corpus/config_evaluation/dynamic-import")
                .as_slice(),
            include_bytes!("../../../../harness-fuzz/corpus/config_evaluation/malformed-json")
                .as_slice(),
        ] {
            check(source).await.unwrap();
        }
        for mode in 0..13 {
            let variants = match mode {
                2 | 3 | 11 | 12 => 2,
                4 => 8,
                8 => 4,
                10 => 16,
                _ => 1,
            };
            for variant in 0..variants {
                check(&[mode, variant, b'\"', b'\\', 0, 255]).await.unwrap();
            }
        }
    }
}
