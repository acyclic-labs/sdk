use crate::{Error, Result};
use oxc_allocator::Allocator;
use oxc_codegen::Codegen;
use oxc_parser::Parser;
use oxc_semantic::SemanticBuilder;
use oxc_span::SourceType;
use oxc_transformer::{TransformOptions, Transformer};
use std::path::Path;

/// Removes TypeScript syntax in-process, without type checking or a compiler
/// subprocess. Ordinary JavaScript is left at its original language level.
pub fn strip_types(source: &str) -> Result<String> {
    let allocator = Allocator::default();
    // Parse as a function body so top-level return/await match code mode.
    let source = format!("async function __code_mode__() {{\n{source}\n}}");
    let parsed = Parser::new(&allocator, &source, SourceType::ts()).parse();
    if !parsed.errors.is_empty() {
        return Err(invalid("code contains invalid TypeScript"));
    }
    let mut program = parsed.program;
    let semantic = SemanticBuilder::new().build(&program);
    if !semantic.errors.is_empty() {
        return Err(invalid("code contains invalid bindings"));
    }
    let mut options = TransformOptions::default();
    options.typescript.only_remove_type_imports = true;
    options.typescript.allow_namespaces = false;
    let transformed = Transformer::new(&allocator, Path::new("code.ts"), &options)
        .build_with_scoping(semantic.semantic.into_scoping(), &mut program);
    if !transformed.errors.is_empty() {
        return Err(invalid("code uses unsupported TypeScript syntax"));
    }
    let javascript = Codegen::new().build(&program).code;
    // Keep the parsed wrapper, rather than slicing braces or reparsing source.
    Ok(format!("{javascript}\nreturn await __code_mode__();"))
}

fn invalid(reason: &'static str) -> Error {
    Error::ToolRejected {
        code: crate::wire::ErrorCode::Invalid,
        reason,
    }
}
