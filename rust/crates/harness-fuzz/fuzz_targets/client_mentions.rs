#![no_main]

pub use acyclic_fuzz_support as fuzz_support;
use acyclic_harness::client::CommandResolution;

#[path = "../support/mentions.rs"]
mod oracle;
#[path = "../support/workspace_media.rs"]
mod workspace_media;

fuzz_support::fuzz_target!(|data: &[u8], fuzz_input| {
    if data.len() > 4096 {
        return;
    }
    workspace_media::exercise(data);
    let Some(resolution) =
        fuzz_support::FuzzInput::parse(fuzz_input, serde_json::from_slice::<CommandResolution>)
    else {
        return;
    };
    let encoded = serde_json::to_vec(&resolution).unwrap();
    let _ = fuzz_support::round_trip(
        &(resolution),
        serde_json::from_slice::<CommandResolution>(&encoded),
    );
    oracle::check(&resolution);
});
