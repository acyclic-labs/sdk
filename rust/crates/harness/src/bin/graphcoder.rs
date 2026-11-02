//! Qualification entry for the shared product runner.
fn main() -> acyclic_plugin::CliCompletion {
    acyclic_plugin::main_with(std::convert::identity, None)
}
