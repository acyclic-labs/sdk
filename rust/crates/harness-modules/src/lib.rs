//! Static settings-module compilation and effective-tree import resolution.
#![cfg(not(target_arch = "wasm32"))]

/// One module of a closure, as JavaScript.
pub struct Module {
    /// Emitted JavaScript after TypeScript erasure.
    pub script: String,
    /// Sorted, deduplicated static imports resolved in the effective tree.
    pub imports: Vec<String>,
}

/// Compile a static settings module and resolve imports from its type-erased tree.
pub fn compile_module(path: &str, source: &[u8]) -> std::result::Result<Module, String> {
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
    // Type erasure has already removed type-only imports and re-exports.
    // Link the remaining declarations from this same tree, without parsing
    // the emitted source into a second arena tree and module record.
    let mut imports = program
        .body
        .iter()
        .filter_map(|statement| statement.as_module_declaration()?.source())
        .map(|source| {
            let specifier = source
                .value
                .as_str()
                .ok_or_else(|| format!("{path}: module paths must be valid Unicode"))?;
            resolve(path, specifier)
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    imports.sort();
    imports.dedup();
    let script = oxc_codegen::Codegen::new().build(&program).code;
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
    } else if specifier == "acyclic" || specifier.starts_with("sys/") {
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
