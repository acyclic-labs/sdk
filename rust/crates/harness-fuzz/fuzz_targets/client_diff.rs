#![no_main]
use acyclic_harness::client::FileDiff;
use libfuzzer_sys::fuzz_target;

fn check(before: Option<&[u8]>, after: Option<&[u8]>, render: usize, work: usize) {
    let Ok(Some(diff)) = FileDiff::between(before, after, render, work) else {
        return;
    };
    assert!(diff.patch.len() <= render);
    let encoded = serde_json::to_vec(&diff).expect("encode diff");
    assert_eq!(
        serde_json::from_slice::<FileDiff>(&encoded).expect("decode diff"),
        diff
    );
    if !diff.binary {
        let before = std::str::from_utf8(before.unwrap_or_default()).expect("text base");
        let after = std::str::from_utf8(after.unwrap_or_default()).expect("text endpoint");
        let patch = diffy::Patch::from_str(&diff.patch).expect("generated patch");
        assert_eq!(diffy::apply(before, &patch).expect("patch applies"), after);
    }
}

fuzz_target!(|input: &[u8]| {
    if let Ok(diff) = serde_json::from_slice::<FileDiff>(input) {
        let encoded = serde_json::to_vec(&diff).expect("encode parsed diff");
        assert_eq!(
            serde_json::from_slice::<FileDiff>(&encoded).expect("decode parsed diff"),
            diff
        );
    }
    let Some((&flags, bytes)) = input.split_first() else {
        return;
    };
    let Some((split, bytes)) = bytes.split_first_chunk::<2>() else {
        return;
    };
    let split = usize::from(u16::from_le_bytes(*split)) % (bytes.len() + 1);
    let (before, after) = bytes.split_at(split);
    let before = (flags & 1 != 0).then_some(before);
    let after = (flags & 2 != 0).then_some(after);
    check(before, after, 8192, 65_536);
});
