//! `GraphCoder` with the self-deployed server seams; see [`graphcoder`].
fn main() -> graphcoder::CliCompletion {
    graphcoder::main_with(std::convert::identity, None)
}
