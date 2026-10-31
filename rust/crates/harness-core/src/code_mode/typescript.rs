//! The TypeScript eraser runs in the same credited engine heap as execution.
use super::{
    Result,
    engine::{Context, Ctx, Runtime, Value, at_stage},
};

const SOURCE: &std::ffi::CStr = match std::ffi::CStr::from_bytes_with_nul(
    concat!(
        "(function(){\n",
        include_str!("typescript_bundle.js"),
        "\nreturn __harness_typescript.transform;\n})()\0"
    )
    .as_bytes(),
) {
    Ok(source) => source,
    Err(_) => panic!("TypeScript eraser source must have exactly one trailing zero"),
};

pub(super) struct Compiler {
    pub(super) context: Context,
}
impl Compiler {
    pub(super) fn new(runtime: &Runtime) -> Result<Self> {
        let context = Context::new(runtime, None)?;
        context.with(|ctx| {
            // The bundle's var declarations belong to this function, never the
            // global object. Strict mode cannot delete a global var binding.
            let transform = at_stage(ctx.eval(SOURCE), "TypeScript eraser initialization failed")?;
            let factory = at_stage(
                ctx.eval(c"(transform) => (source) => { const wrapped = \"async function __code_mode__() {\\n\" + source + \"\\n}\"; return transform(wrapped) + \"\\nreturn await __code_mode__();\"; }"),
                "TypeScript preparation factory initialization failed",
            )?;
            let prepare = at_stage(factory.call([transform]), "TypeScript preparation binding failed")?;
            ctx.global()?.set(c"__prepare", prepare)
        })?;
        Ok(Self { context })
    }
    pub(super) fn prepare<'a>(ctx: Ctx<'a>, source: &str) -> Result<Value<'a>> {
        at_stage(
            ctx.global()?.get(c"__prepare")?.call([ctx.string(source)?]),
            "TypeScript source transformation failed",
        )
    }
}
