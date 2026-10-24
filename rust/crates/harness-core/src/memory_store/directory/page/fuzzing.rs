use super::{DirectoryIndex, MemoryStorePage, reference};

#[derive(serde::Serialize, serde::Deserialize)]
struct Case {
    paths: Vec<(String, Option<u64>)>,
    path: String,
    generation: u64,
    after: Option<String>,
    maximum: u32,
}
pub fn check(data: &[u8]) {
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
    let mut index = DirectoryIndex::default();
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
    let decoded: MemoryStorePage = serde_json::from_slice(&encoded).expect("decode generated page");
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
}
