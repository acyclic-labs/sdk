#![no_main]
use acyclic_harness::client::CommandResolution;
use libfuzzer_sys::fuzz_target;

#[path = "../support/mentions.rs"]
mod oracle;
#[path = "../support/workspace_media.rs"]
mod workspace_media;

fuzz_target!(|data: &[u8]| {
    if data.len() > 4096 {
        return;
    }
    workspace_media::exercise(data);
    let Ok(resolution) = serde_json::from_slice::<CommandResolution>(data) else {
        return;
    };
    let encoded = serde_json::to_vec(&resolution).unwrap();
    assert_eq!(
        serde_json::from_slice::<CommandResolution>(&encoded).unwrap(),
        resolution
    );
    oracle::check(&resolution);
});
