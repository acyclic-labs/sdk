//! The TypeScript eraser runs in the same credited QuickJS heap as execution.
//! Its private realm is shared by all invocations; source and transformed output
//! stay JavaScript strings, with no Rust parser arena or generated-code buffer.
use super::{Result, js_error};
use rquickjs::{Context, Ctx, Function, Runtime};

pub(super) struct Compiler {
    pub(super) context: Context,
}

impl Compiler {
    pub(super) fn new(runtime: &Runtime) -> Result<Self> {
        let context = Context::full(runtime).map_err(js_error)?;
        context
            .with(|ctx| {
                ctx.eval::<(), _>(include_str!("typescript_bundle.js"))?;
                // Capture the original pure transform privately. The model never
                // receives this function, its realm or its mutable parser globals.
                ctx.eval::<(), _>(
                    r#"
                globalThis.__prepare = ((transform) => (source) => {
                    const wrapped = "async function __code_mode__() {\n" + source + "\n}";
                    return transform(wrapped) + "\nreturn await __code_mode__();";
                })(__harness_typescript.transform);
                delete globalThis.__harness_typescript;
            "#,
                )
            })
            .map_err(js_error)?;
        Ok(Self { context })
    }

    pub(super) fn prepare<'js>(
        ctx: &Ctx<'js>,
        source: &str,
    ) -> rquickjs::Result<rquickjs::String<'js>> {
        let prepare: Function = ctx.globals().get("__prepare")?;
        prepare.call((source,))
    }
}
