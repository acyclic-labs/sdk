//! The one settings evaluator: TypeScript stripped by oxc, run as ES modules
//! in QuickJS, cached by the digest of everything it read.
use super::{Form, Link};
use crate::{Error, Result, layers::Layer};
use futures::future::BoxFuture;
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::{
    Arc, Mutex, OnceLock,
    atomic::{AtomicU64, Ordering},
};

/// The evaluator's identity: a change to its semantics changes every key.
const VERSION: &str = "acyclic.harness.config.v3";

/// Interrupt polls (QuickJS polls every ten thousand instructions)
/// before a settings module stops. It is deterministic, so a module passes
/// or fails alike on every host; no plain-data fold comes near it.
const FUEL: u64 = 1_000;

/// Files a module may import from the same pinned effective tree and `sys/`.
pub trait Tree: Send + Sync {
    /// The original owned bytes of `path`, or `None` when no layer holds it.
    /// Module loading borrows this backing through parsing; an adapter must
    /// retain its original allocation owner rather than copy away its custody.
    fn read<'a>(
        &'a self,
        path: &'a str,
    ) -> BoxFuture<'a, Result<Option<acyclic_stream::OwnedBytes>>>;
}

/// What a module's default export may be.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Surface {
    /// A settings entry point: a value or a function of the value below.
    Compose,
    /// Plain data only (`policy.ts`, `context.ts`, schedules): the kernel
    /// intersects or validates it, so a function could drop a lower rule.
    Data,
}

/// One evaluated module.
#[derive(Clone, Debug, PartialEq)]
pub struct Evaluated {
    /// The plain-data result.
    pub value: Value,
    /// Whether the export replaced the base or was a function of it.
    pub form: Form,
    /// blake3 over the module and every module it imports.
    pub digest: [u8; 32],
}

mod prepared;
pub use prepared::{Evaluation, ModuleRead, Prepared, prepare};
mod composition;
pub use composition::{
    Composition, LayerInput, LayerState, ModuleCut, compose_result, compose_with_evidence,
};

/// Evaluates the module at `path` holding `source`, with `base` as the value
/// below it. Imports resolve in `tree`. A throw, an exhausted budget, an
/// import outside the tree, a cycle, a missing `export default` or a result
/// that is not plain data is [`Error::Invalid`], naming `path`.
pub async fn evaluate(
    path: &str,
    source: &[u8],
    tree: &dyn Tree,
    base: &Value,
    surface: Surface,
) -> Result<Evaluated> {
    evaluate_decision(path, source, tree, base, surface)
        .await?
        .into_result()
}

/// Evaluate once while retaining the original input identity on semantic
/// refusal. This is the same inline path as [`evaluate`]; it allocates no
/// additional preparation future. Operational tree failures remain outer
/// errors. Use [`prepare`] when the original scripts must be evaluated again.
pub async fn evaluate_decision(
    path: &str,
    source: &[u8],
    tree: &dyn Tree,
    base: &Value,
    surface: Surface,
) -> Result<Evaluation> {
    evaluate_decision_cached(path, source, tree, base, surface, cache()).await
}

#[cfg(any(test, feature = "test-support"))]
async fn evaluate_cached(
    path: &str,
    source: &[u8],
    tree: &dyn Tree,
    base: &Value,
    surface: Surface,
    cache: &Cache,
) -> Result<Evaluated> {
    evaluate_decision_cached(path, source, tree, base, surface, cache)
        .await?
        .into_result()
}

async fn evaluate_decision_cached(
    path: &str,
    source: &[u8],
    tree: &dyn Tree,
    base: &Value,
    surface: Surface,
    cache: &Cache,
) -> Result<Evaluation> {
    prepared::load(path, source, tree, false)
        .await?
        .evaluate_cached(path, base, surface, cache)
}

/// Folds the copies of a settings entry point over `layers` (lowest first)
/// onto `base`, returning the result and its chain (`<path>.why.json`).
pub async fn compose(
    path: &str,
    layers: &[Layer<'_>],
    tree: &dyn Tree,
    base: Value,
) -> Result<(Value, Vec<Link>)> {
    compose_result(path, layers, tree, base).await?
}

/// The most recently evaluated result or failure. Replacing the entry drops
/// its ownership instead of retaining every historical agent/revision.
type Outcome = std::result::Result<Arc<(Value, Form)>, String>;
type Cache = Mutex<Option<([u8; 32], Outcome)>>;

fn cache() -> &'static Cache {
    static CACHE: OnceLock<Cache> = OnceLock::new();
    CACHE.get_or_init(Default::default)
}

/// One module of a closure, as JavaScript.
struct Module {
    script: String,
    imports: Vec<String>,
}

/// `path`'s source as a JavaScript module and its resolved static imports.
fn strip(path: &str, source: &[u8]) -> std::result::Result<Module, String> {
    let text = std::str::from_utf8(source).map_err(|_| format!("{path} is not UTF-8"))?;
    if path.ends_with(".json") {
        serde_json::from_str::<serde::de::IgnoredAny>(text)
            .map_err(|error| format!("{path}: {error}"))?;
        return Ok(Module {
            script: format!(
                "export default JSON.parse({})\n;",
                serde_json::to_string(text).map_err(|error| error.to_string())?
            ),
            imports: Vec::new(),
        });
    }
    let source_type = if path.ends_with(".ts") {
        oxc_span::SourceType::ts()
    } else if path.ends_with(".js") {
        oxc_span::SourceType::mjs()
    } else {
        return Err(format!("{path}: a module is a .ts, .js or .json file"));
    };
    macro_rules! refuse_errors {
        ($diagnostics:expr) => {{
            let text = $diagnostics
                .errors()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join("; ");
            if !text.is_empty() {
                return Err(format!("{path}: {text}"));
            }
        }};
    }
    let allocator = oxc_allocator::Allocator::default();
    let parsed = oxc_parser::Parser::new(&allocator, text, source_type).parse();
    refuse_errors!(parsed.diagnostics);
    if !parsed.module_record.dynamic_imports.is_empty() {
        return Err(format!(
            "{path}: settings modules import statically; import() is unavailable"
        ));
    }
    let mut program = parsed.program;
    let scoping = oxc_semantic::SemanticBuilder::new()
        .build(&program)
        .semantic
        .into_scoping();
    let transformed = oxc_transformer::Transformer::new(
        &allocator,
        std::path::Path::new(path),
        &oxc_transformer::TransformOptions::default(),
    )
    .build_with_scoping(scoping, &mut program);
    refuse_errors!(transformed.diagnostics);
    let script = oxc_codegen::Codegen::new().build(&program).code;
    // Imports left after type erasure: re-read them from the emitted module.
    let mut imports = {
        let emitted =
            oxc_parser::Parser::new(&allocator, &script, oxc_span::SourceType::mjs()).parse();
        refuse_errors!(emitted.diagnostics);
        emitted
            .module_record
            .requested_modules
            .keys()
            .map(|specifier| resolve(path, specifier.as_str()))
            .collect::<std::result::Result<Vec<_>, _>>()?
    };
    imports.sort();
    Ok(Module { script, imports })
}

/// The name QuickJS gives `specifier` imported from `importer`: a leading
/// `./` or `../` resolves against the importer's directory, anything else
/// is used as written. Only names inside the tree are allowed.
fn resolve(importer: &str, specifier: &str) -> std::result::Result<String, String> {
    let refuse = || {
        format!(
            "{importer} imports {specifier}: imports name a file in the effective tree \
             (\"./x.ts\") or under sys/"
        )
    };
    let name = if specifier.starts_with('.') {
        let mut directory: Vec<&str> = importer
            .rsplit_once('/')
            .map_or_else(Vec::new, |(d, _)| d.split('/').collect());
        let mut rest = specifier;
        loop {
            if let Some(after) = rest.strip_prefix("./") {
                rest = after;
            } else if let Some(after) = rest.strip_prefix("../") {
                directory.pop().ok_or_else(refuse)?;
                rest = after;
            } else {
                break;
            }
        }
        directory.push(rest);
        directory.join("/")
    } else if specifier.starts_with("sys/") {
        specifier.to_owned()
    } else {
        return Err(refuse());
    };
    if name
        .split('/')
        .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        return Err(refuse());
    }
    Ok(name)
}

/// Folds and encodes a module's default export, in the engine. A function
/// anywhere in it, the export itself or a nested field such as a plugin
/// entry or its `config`, is called with a copy of the base at the same
/// path (`undefined` where the base has none) and must return data there.
/// The result is rebuilt as plain data, so cycles, accessors and exotic
/// objects never reach the encoder.
// The trusted closure is prepared before module evaluation. Protect JSON
// imports and the fold against replacements by modules or their callbacks.
const FINISH: &str = r#"(() => {
    delete globalThis.Date;
    delete Math.random;
    const freeze = Object.freeze;
    const define = Object.defineProperty;
    for (const name of ['Object', 'Array', 'Number', 'JSON', 'Function', 'TypeError', 'Set']) {
        const value = globalThis[name];
        if (value.prototype) freeze(value.prototype);
        freeze(value);
        define(globalThis, name, { value, writable: false, configurable: false });
    }
    return (namespace, base, data) => {
    const own = Object.prototype.hasOwnProperty;
    if (!own.call(namespace, 'default'))
        throw new TypeError('has no `export default`');
    const open = new Set();
    const item = (v, key, at) => {
        const descriptor = Object.getOwnPropertyDescriptor(v, key);
        if (!own.call(descriptor, 'value'))
            throw new TypeError(`${at}.${key} is an accessor, not plain data`);
        return descriptor.value;
    };
    const fold = (v, below, at) => {
        if (typeof v === 'function') {
            if (data) throw new TypeError(`${at} must be plain data, not a function of the defaults`);
            v = v(below === undefined ? undefined : JSON.parse(JSON.stringify(below)));
            if (typeof v === 'function') throw new TypeError(`${at} returned a function, not plain data`);
        }
        if (v === null || typeof v === 'string' || typeof v === 'boolean') return v;
        if (typeof v === 'number') {
            if (!Number.isFinite(v)) throw new TypeError(`${at} is not a finite number`);
            return v;
        }
        if (typeof v !== 'object') throw new TypeError(`${at} is a ${typeof v}, not plain data`);
        if (open.has(v)) throw new TypeError(`${at} contains itself`);
        const array = Array.isArray(v);
        const prototype = Object.getPrototypeOf(v);
        if (prototype !== (array ? Array.prototype : Object.prototype) && !(prototype === null && !array))
            throw new TypeError(`${at} is not a plain object or array`);
        const map = below !== null && typeof below === 'object' && Array.isArray(below) === array;
        const at_ = (key) => (map && own.call(below, key) ? below[key] : undefined);
        open.add(v);
        let out;
        if (array) {
            out = [];
            out.length = v.length;
            for (let index = 0; index < v.length; index++)
                if (own.call(v, index)) out[index] = fold(item(v, index, at), at_(index), `${at}[${index}]`);
        } else {
            out = Object.create(null);
            const keys = Object.keys(v);
            for (let index = 0; index < keys.length; index++) {
                const key = keys[index];
                const value = item(v, key, at);
                if (value !== undefined) out[key] = fold(value, at_(key), `${at}.${key}`);
            }
        }
        open.delete(v);
        return out;
    };
    const form = typeof namespace.default === 'function' ? 'function' : 'value';
    return { form, json: JSON.stringify(fold(namespace.default, JSON.parse(base), 'the default export')) };
    };
})()"#;

/// Evaluates a resolved closure once, with no host functions installed.
fn run(
    modules: &[(String, Module)],
    base: &[u8],
    surface: Surface,
) -> std::result::Result<(Value, Form), String> {
    use rquickjs::{CaughtError, Context, Function, Object, Runtime};
    let engine = |error: rquickjs::Error| error.to_string();
    #[cfg(any(test, feature = "test-support"))]
    model::entered();
    let runtime = Runtime::new().map_err(engine)?;
    runtime.set_memory_limit(crate::executor::ENGINE_MEMORY);
    let fuel = Arc::new(AtomicU64::new(FUEL));
    let remaining = Arc::clone(&fuel);
    runtime.set_interrupt_handler(Some(Box::new(move || {
        remaining
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |left| {
                left.checked_sub(1)
            })
            // Stop on the last unit and on every poll after it.
            .ok()
            .is_none_or(|left| left == 1)
    })));
    let context = Context::full(&runtime).map_err(engine)?;
    let base = std::str::from_utf8(base).map_err(|error| error.to_string())?;
    let outcome = context.with(|ctx| {
        let result = (|| {
            // Settings are pure: no clock or randomness, and no host
            // function, loader, tool or I/O is installed.
            let finish: Function = ctx.eval(FINISH)?;
            // The context owns declared dependencies; only the root needs
            // a Rust handle for evaluation.
            let (root, dependencies) = modules.split_last().ok_or(rquickjs::Error::Unknown)?;
            for (name, module) in dependencies {
                rquickjs::Module::declare(ctx.clone(), name.as_str(), module.script.as_str())?;
            }
            let (name, module) = root;
            let (module, promise) =
                rquickjs::Module::declare(ctx.clone(), name.as_str(), module.script.as_str())?
                    .eval()?;
            promise.finish::<()>()?;
            let result: Object =
                finish.call((module.namespace()?, base, surface == Surface::Data))?;
            let encoded = (
                result.get::<_, String>("form")?,
                result.get::<_, String>("json")?,
            );
            Ok::<_, rquickjs::Error>(encoded)
        })();
        result.map_err(|error| match error {
            rquickjs::Error::WouldBlock => "a top-level await never settles".to_owned(),
            error => match CaughtError::from_error(&ctx, error) {
                CaughtError::Exception(exception) => {
                    let message = exception.message().unwrap_or_default();
                    match exception.stack() {
                        Some(stack) if !stack.is_empty() => format!("{message}\n{stack}"),
                        _ => message,
                    }
                }
                CaughtError::Value(value) => {
                    // QuickJS Debug prints native addresses for reference
                    // values. Preserve scalar causes, but never cache an
                    // allocator-dependent address as a settings diagnostic.
                    if value.is_string() || value.is_bool() || value.is_number() {
                        format!("threw {value:?}")
                    } else {
                        format!("threw {:?}", value.type_of())
                    }
                }
                CaughtError::Error(error) => error.to_string(),
            },
        })
    });
    if fuel.load(Ordering::Relaxed) == 0 {
        return Err(format!("exceeded its instruction budget ({FUEL} polls)"));
    }
    let (form, json) = outcome?;
    let value = serde_json::from_str(&json).map_err(|error| error.to_string())?;
    let form = if form == "function" {
        Form::Function
    } else {
        Form::Value
    };
    Ok((value, form))
}

#[cfg(test)]
mod tests;

#[cfg(any(test, feature = "test-support"))]
#[doc(hidden)]
pub mod model;
