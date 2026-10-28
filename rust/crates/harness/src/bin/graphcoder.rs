//! `GraphCoder` with the self-deployed server seams; see [`graphcoder`].
fn main() -> std::process::ExitCode {
    graphcoder::main_with(std::convert::identity)
}
