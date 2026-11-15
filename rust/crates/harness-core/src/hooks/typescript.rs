//! Plugin before-hooks written in TypeScript, run in the one settings
//! evaluator. No process starts: the hook is a pure function of its request.
use crate::{
    Error, Result,
    config::{self, Surface, Tree},
};
use futures::future::BoxFuture;

/// A hook's modules, pinned at admission: the only files its imports read.
struct Modules<'a>(&'a [(String, String)]);

impl Tree for Modules<'_> {
    fn read<'a>(
        &'a self,
        path: &'a str,
    ) -> BoxFuture<'a, Result<Option<acyclic_stream::OwnedBytes>>> {
        Box::pin(async move {
            Ok(self
                .0
                .iter()
                .find(|(module, _)| module == path)
                .map(|(_, source)| acyclic_stream::OwnedBytes::from(source.as_bytes().to_vec())))
        })
    }
}

/// Calls the hook's default export with `input`, the same `{request, config}`
/// a script hook reads on stdin. The decision returns as a script's
/// successful stdout, so one decoder judges every hook form. A module that
/// fails to load or throws is a hook failure, which blocks the request.
pub(super) async fn run(
    modules: &[(String, String)],
    input: &[u8],
) -> Result<(Option<i32>, Vec<u8>, Vec<u8>)> {
    let [(path, source), ..] = modules else {
        return Err(Error::Invalid("a TypeScript hook has a module".into()));
    };
    let input: serde_json::Value = serde_json::from_slice(input)
        .map_err(|error| Error::Invalid(format!("plugin hook request: {error}")))?;
    let evaluated = config::prepare(path, source.as_bytes(), &Modules(modules))
        .await?
        .evaluate(&input, Surface::Compose)?
        .into_result()?;
    let decision = serde_json::to_vec(&evaluated.value)
        .map_err(|error| Error::Invalid(format!("plugin hook decision: {error}")))?;
    Ok((Some(0), decision, Vec::new()))
}
