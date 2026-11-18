//! `assert_in_execution_root!()` fails the compile of the crate that calls it when that crate's
//! `${exec_root}` substitution (`PROBE_EXEC_ROOT`) is not the compile action's working directory, when
//! its `OUT_DIR` or `CARGO_MANIFEST_DIR` lies outside the compile action's working directory, which is
//! the action's execution root under every sandbox, or when its build script's file does not read
//! back through `OUT_DIR` there.

use proc_macro::TokenStream;
use std::path::Path;

#[proc_macro]
pub fn assert_in_execution_root(_: TokenStream) -> TokenStream {
    let root = std::env::current_dir().expect("a compile action has a working directory");
    let mut refusals = Vec::new();
    // `${exec_root}` is the compile action's working directory, under every sandbox.
    match std::env::var_os("PROBE_EXEC_ROOT") {
        Some(exec_root) if Path::new(&exec_root) == root => {}
        Some(exec_root) => refusals.push(format!(
            "${{exec_root}} {} is not the execution root {}",
            Path::new(&exec_root).display(),
            root.display()
        )),
        None => refusals.push("PROBE_EXEC_ROOT is unset".to_owned()),
    }
    for name in ["OUT_DIR", "CARGO_MANIFEST_DIR"] {
        match std::env::var_os(name) {
            Some(path) if Path::new(&path).starts_with(&root) => {}
            Some(path) => refusals.push(format!(
                "{name} {} lies outside the execution root {}",
                Path::new(&path).display(),
                root.display()
            )),
            None => refusals.push(format!("{name} is unset")),
        }
    }
    if let Some(out) = std::env::var_os("OUT_DIR")
        && std::fs::read_to_string(Path::new(&out).join("probe.txt")).is_err()
    {
        refusals.push(format!(
            "the build script's probe.txt does not read back through OUT_DIR {}",
            Path::new(&out).display()
        ));
    }
    refusals
        .iter()
        .map(|refusal| format!("compile_error!({refusal:?});"))
        .collect::<String>()
        .parse()
        .expect("compile_error! invocations are tokens")
}
