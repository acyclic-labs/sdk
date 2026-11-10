//! The GraphCoder preset uses the same frontend, client and terminal as `acyclic`.
fn main() -> std::process::ExitCode {
    let arguments: Vec<_> = std::env::args_os().skip(1).collect();
    if matches!(arguments.as_slice(), [flag] if flag == "--version" || flag == "-V") {
        println!("graphcoder {}", env!("CARGO_PKG_VERSION"));
        return std::process::ExitCode::SUCCESS;
    }
    std::process::Termination::report(acyclic_plugin::main_from(
        ["acyclic", "--template", "graphcoder"]
            .map(std::ffi::OsString::from)
            .into_iter()
            .chain(arguments),
        std::convert::identity,
        None,
    ))
}
