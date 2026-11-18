//! Capture canonical failure bytes through the public production serializer.
use acyclic_stream::{StreamCategory, StreamError};

fn capture() -> Result<(), Box<dyn std::error::Error>> {
    let directory = std::path::PathBuf::from(
        std::env::args_os()
            .nth(1)
            .ok_or("expected corpus directory")?,
    );
    std::fs::create_dir_all(&directory)?;
    for error in StreamCategory::ALL.map(StreamError::from) {
        let bytes = serde_json::to_vec(&error)?;
        std::fs::write(directory.join(error.code()), &bytes)?;
        println!(
            "captured {}: {}",
            error.code(),
            std::str::from_utf8(&bytes)?
        );
    }
    Ok(())
}

fn main() {
    if let Err(error) = capture() {
        eprintln!(
            "step=capture-stream-failures: {error}; recovery: bazel run //fuzz:stream_error_corpus_example -- \"$PWD/fuzz/corpus/stream_error\""
        );
        std::process::exit(1);
    }
}
