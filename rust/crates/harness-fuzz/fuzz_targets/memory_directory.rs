//! Fuzz directory selection, generation visibility and serialized-page round trips.
#![no_main]
use libfuzzer_sys::fuzz_target;
#[path = "../../harness-core/src/memory_store/directory/page.rs"]
mod page;
use page::{MemoryStoreEntry, MemoryStoreEntryKind, MemoryStorePage};
#[path = "../../harness-core/src/memory_store/directory/page/reference.rs"]
mod reference;

#[derive(serde::Serialize, serde::Deserialize)]
struct Case {
    paths: Vec<(String, Option<u64>)>,
    path: String,
    generation: u64,
    after: Option<String>,
    maximum: u32,
}
fuzz_target!(|data: &[u8]| {
    if data.len() > 65_536 {
        return;
    }
    let Ok(case) = serde_json::from_slice::<Case>(data) else {
        return;
    };
    if case.paths.len() > 256 {
        return;
    }
    let maximum = case.maximum as usize;
    let paths: std::collections::BTreeMap<_, _> = case.paths.into_iter().collect();
    let mut ordered: Vec<_> = paths.iter().collect();
    ordered.sort_by_key(|(_, generation)| generation.unwrap_or(0));
    let mut index = page::DirectoryIndex::default();
    for (path, generation) in ordered {
        index.publish(path, generation.unwrap_or(0));
    }
    let actual = index.page(
        &case.path,
        case.generation,
        case.after.as_deref(),
        maximum,
        |_| {},
    );
    let encoded = serde_json::to_vec(&actual).expect("serialize generated page");
    let decoded: page::MemoryStorePage =
        serde_json::from_slice(&encoded).expect("decode generated page");
    assert_eq!(decoded, actual);
    assert_eq!(
        actual,
        reference::expected(
            &paths,
            &case.path,
            case.generation,
            case.after.as_deref(),
            maximum,
            |created| *created
        )
    );
});
