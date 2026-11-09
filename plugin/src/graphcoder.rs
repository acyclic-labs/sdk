//! The GraphCoder preset uses the same frontend, client and terminal as `acyclic`.
fn main() -> acyclic_plugin::CliCompletion {
    acyclic_plugin::main_from(
        ["acyclic", "run", "--template", "graphcoder"]
            .map(std::ffi::OsString::from)
            .into_iter()
            .chain(std::env::args_os().skip(1)),
        std::convert::identity,
        None,
    )
}
