//! On-demand checks of the stored codecs through their canonical implementation.
//!
//! The blob oracle lives with the blob codec; the local append-log oracle lives
//! with the local medium. This module is the one composition of both, so a
//! caller that checks a persisted value checks every codec that can hold it.
use acyclic_objects_core::Error;

/// Prepare an obsolete value using the original local compaction chunk size.
#[cfg(not(target_arch = "wasm32"))]
pub fn compaction_chunk_bytes() -> Result<usize, Error> {
    crate::local::codec_fuzz::compaction_chunk_bytes()
}

/// Exercise canonical identity and typed framing refusal through the original
/// stored Objects decoders, including the metadata behind HEAD.
pub fn check(bytes: &[u8]) -> bool {
    if !acyclic_objects_core::codec_fuzz::check(bytes) {
        return false;
    }
    #[cfg(not(target_arch = "wasm32"))]
    if !crate::local::codec_fuzz::check(bytes) {
        return false;
    }
    true
}

/// Capture actual SDK encoder output; names identify the producing codec.
pub fn seeds() -> Result<Vec<(&'static str, Vec<u8>)>, Error> {
    let seeds = acyclic_objects_core::codec_fuzz::seeds()?;
    #[cfg(not(target_arch = "wasm32"))]
    let seeds = [seeds, crate::local::codec_fuzz::seeds()?].concat();
    Ok(seeds)
}

#[cfg(test)]
mod tests {
    #[test]
    fn stored_writer_corpus_satisfies_the_fuzz_oracle() -> Result<(), Box<dyn std::error::Error>> {
        for (name, bytes) in super::seeds()? {
            assert!(super::check(&bytes), "persisted fuzz oracle: {name}");
        }
        for name in ["head-live", "head-dying"] {
            let bytes =
                acyclic_compatibility_golden::frame(&format!("corpus/object_persistence/{name}"))?;
            assert!(super::check(&bytes), "persisted fuzz oracle: {name}");
        }
        Ok(())
    }
}
