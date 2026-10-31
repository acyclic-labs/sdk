//! The TypeScript eraser runs in the same credited engine heap as execution.
use super::{
    Result,
    engine::{Context, Ctx, Runtime, Value},
};

const SOURCE: &std::ffi::CStr = match std::ffi::CStr::from_bytes_with_nul(
    concat!(include_str!("typescript_bundle.js"), "\0").as_bytes(),
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
            ctx.eval(SOURCE)?;
            ctx.eval(c"globalThis.__prepare = ((transform) => (source) => { const wrapped = \"async function __code_mode__() {\\n\" + source + \"\\n}\"; return transform(wrapped) + \"\\nreturn await __code_mode__();\"; })(__harness_typescript.transform); delete globalThis.__harness_typescript;").map(drop)
        })?;
        Ok(Self { context })
    }
    pub(super) fn prepare<'a>(ctx: Ctx<'a>, source: &str) -> Result<Value<'a>> {
        ctx.global()?.get(c"__prepare")?.call([ctx.string(source)?])
    }
}
