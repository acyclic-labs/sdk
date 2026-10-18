//! Exercises the production file-tool and protected file-metadata codecs.
#![no_main]

pub use acyclic_fuzz_support as fuzz_support;

use acyclic_harness::{conversation::FileRef, tool::files::ReadFileInput, tools::BashInput};

fuzz_support::fuzz_target!(|bytes: &[u8], fuzz_input| {
    if let Some(input) =
        fuzz_support::FuzzInput::parse(fuzz_input, serde_json::from_slice::<BashInput>)
    {
        let encoded = serde_json::to_vec(&input)
            .unwrap_or_else(|error| unreachable!("input encoding failed: {error}"));
        let _ = fuzz_support::round_trip(&input, serde_json::from_slice::<BashInput>(&encoded));
        let _ = input.validate();
    }
    if let Some(input) =
        fuzz_support::FuzzInput::parse(fuzz_input, serde_json::from_slice::<ReadFileInput>)
    {
        let encoded = serde_json::to_vec(&input)
            .unwrap_or_else(|error| unreachable!("input encoding failed: {error}"));
        let _ = fuzz_support::round_trip(&input, serde_json::from_slice::<ReadFileInput>(&encoded));
    }
    if let Some(metadata) =
        fuzz_support::FuzzInput::parse(fuzz_input, serde_json::from_slice::<FileRef>)
        && metadata.validate(&mut None).is_ok()
    {
        let encoded = serde_json::to_vec(&metadata)
            .unwrap_or_else(|error| unreachable!("metadata encoding failed: {error}"));
        let _ = fuzz_support::round_trip(&metadata, serde_json::from_slice::<FileRef>(&encoded));
    }
});
