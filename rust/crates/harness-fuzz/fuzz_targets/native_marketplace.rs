#![no_main]

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

// The installed binary uses this same decoder before canonicalizing paths.
#[path = "../../../../plugin/src/install/marketplace.rs"]
mod marketplace;

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    let admitted = marketplace::Marketplaces::parse(bytes);
    let Ok(raw) = serde_json::from_slice::<serde_json::Value>(bytes) else {
        assert!(admitted.is_err(), "non-JSON native output must be refused");
        return;
    };
    let listed = admitted.expect("valid native JSON must reach shape admission");
    let source = listed.local_source();
    let Some(entries) = raw
        .get("marketplaces")
        .and_then(serde_json::Value::as_array)
    else {
        assert!(
            source.is_err(),
            "native output must name a marketplace list"
        );
        return;
    };
    if entries.iter().any(|entry| {
        entry
            .get("name")
            .and_then(serde_json::Value::as_str)
            .is_none()
    }) {
        assert!(
            source.is_err(),
            "every native marketplace must retain its name"
        );
        return;
    }
    let selected: Vec<_> = entries
        .iter()
        .filter(|entry| entry["name"] == "acyclic")
        .collect();
    match selected.as_slice() {
        [] => assert!(
            source.unwrap().is_none(),
            "foreign registrations are not ours"
        ),
        [entry] => {
            let root = entry.get("root").and_then(serde_json::Value::as_str);
            let original = entry
                .get("marketplaceSource")
                .and_then(|source| source.get("source"))
                .and_then(serde_json::Value::as_str);
            if entry["marketplaceSource"]["sourceType"] != "local"
                || root.is_none()
                || original.is_none()
            {
                assert!(
                    source.is_err(),
                    "native source must have local root and origin"
                );
            } else {
                let (admitted_root, admitted_original) = source
                    .unwrap()
                    .expect("original local source must be retained");
                assert_eq!(Some(admitted_root), root, "selected root identity changed");
                assert_eq!(
                    Some(admitted_original),
                    original,
                    "selected origin identity changed"
                );
            }
        }
        _ => assert!(
            source.is_err(),
            "duplicate native registrations must be refused"
        ),
    }
});
