//! The GraphCoder preset uses the same frontend, client and terminal as `acyclic`.
fn main() -> std::process::ExitCode {
    let arguments: Vec<_> = std::env::args_os().skip(1).collect();
    if matches!(arguments.as_slice(), [flag] if flag == "--version" || flag == "-V") {
        println!("graphcoder {}", env!("CARGO_PKG_VERSION"));
        return std::process::ExitCode::SUCCESS;
    }
    if matches!(arguments.as_slice(), [command, flag] if command == "contracts" && flag == "--json")
    {
        let counters = acyclic_harness_providers::catalog::token_counters();
        return std::process::Termination::report(serde_json::to_writer(
            std::io::stdout().lock(),
            &counters,
        ));
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
