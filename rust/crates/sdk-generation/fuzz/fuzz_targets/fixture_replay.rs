#![no_main]
use sdk_docs::path_identity as paths;
use sdk_generation::historical;
#[path = "../git_layout.rs"]
mod git_layout;
#[path = "../../src/source_selection.rs"]
mod source_selection;

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    let _ = git_layout::check_input(bytes);
    if let Ok(text) = std::str::from_utf8(bytes) {
        let fields = text
            .lines()
            .take(8)
            .map(str::to_owned)
            .collect::<std::collections::BTreeSet<_>>();
        let sources = fields
            .iter()
            .map(|name| sdk_docs::GeneratedSource {
                physical_path: name.into(),
                logical_path: name.into(),
                sha256: name.clone(),
            })
            .collect::<Vec<_>>();
        for windows in [false, true] {
            // Every exact reference must survive selection, including malformed
            // and platform-specific spellings; the selector is conservative.
            assert_eq!(
                source_selection::referenced(&sources, &fields, windows),
                sources
            );
        }
        let mut duplicate = sources.clone();
        duplicate.extend(sources.clone());
        source_selection::deduplicate(&mut duplicate);
        let once = duplicate.clone();
        source_selection::deduplicate(&mut duplicate);
        assert_eq!(duplicate, once);
        let mut fields = text.splitn(3, '\n');
        if let (Some(old), Some(new), Some(path)) = (fields.next(), fields.next(), fields.next()) {
            for windows in [false, true] {
                if let Some(rebased) = paths::rebase(path, old, new, windows) {
                    assert_eq!(paths::rebase(&rebased, new, new, windows), Some(rebased));
                    let before = paths::stable_filename(path, old);
                    let root_is_windows = old.as_bytes().get(1) == Some(&b':')
                        || old.starts_with("\\\\")
                        || old.starts_with("//");
                    let new_is_windows = new.as_bytes().get(1) == Some(&b':')
                        || new.starts_with("\\\\")
                        || new.starts_with("//");
                    if before.starts_with("<root>/")
                        && windows == root_is_windows
                        && windows == new_is_windows
                    {
                        assert_eq!(
                            paths::stable_filename(
                                &paths::rebase(path, old, new, windows).unwrap(),
                                new
                            ),
                            before
                        );
                    }
                }
            }
        }
    }
});
