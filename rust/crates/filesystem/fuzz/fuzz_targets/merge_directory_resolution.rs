//! Raw resolutions and persisted merge files enter their shipped decoders.
#![no_main]

use acyclic_fs::workspace::{MergeCleanFile, MergeConflictFile, MergeDirectoryPlan};

fn admit<T: serde::de::DeserializeOwned>(bytes: &[u8]) {
    let _: T =
        serde_json::from_slice(bytes).expect("captured merge-directory writer output was refused");
    acyclic_fs::workspace::check_merge_directory_resolution(bytes);
}

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    // Admission is unconditional: rejecting every input must fail this oracle.
    admit::<MergeDirectoryPlan>(include_bytes!(
        "../corpus/merge_directory_resolution/plan-v1"
    ));
    admit::<MergeCleanFile>(include_bytes!(
        "../corpus/merge_directory_resolution/clean-0-v1"
    ));
    admit::<MergeConflictFile>(include_bytes!(
        "../corpus/merge_directory_resolution/conflicts-0-v1"
    ));
    admit::<MergeConflictFile>(include_bytes!(
        "../corpus/merge_directory_resolution/conflicts-1-v1"
    ));
    admit::<MergeConflictFile>(include_bytes!(
        "../corpus/merge_directory_resolution/type-v1"
    ));
    admit::<MergeConflictFile>(include_bytes!(
        "../corpus/merge_directory_resolution/mode-v1"
    ));
    for trace in [
        include_bytes!("../corpus/merge_directory_resolution/resolution-take-ours-v1").as_slice(),
        include_bytes!("../corpus/merge_directory_resolution/resolution-take-theirs-v1").as_slice(),
        include_bytes!("../corpus/merge_directory_resolution/resolution-remove-v1").as_slice(),
        include_bytes!("../corpus/merge_directory_resolution/resolution-text-accepted-v1")
            .as_slice(),
        include_bytes!("../corpus/merge_directory_resolution/resolution-text-refused-v1")
            .as_slice(),
    ] {
        acyclic_fs::workspace::check_captured_merge_resolution(trace, bytes);
    }
    acyclic_fs::workspace::check_merge_directory_resolution(bytes);
});
