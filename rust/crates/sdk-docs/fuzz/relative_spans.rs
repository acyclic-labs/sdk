use super::relative_spans;
use rustdoc_types::{Item, Span};
use std::{collections::BTreeSet, path::PathBuf};

pub fn check(prototype: &Item, name: &[u8]) {
    let name = name
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let local = PathBuf::from(format!("src/source-{name}.rs"));
    let other = PathBuf::from(format!("other/source-{name}.rs"));
    let foreign = PathBuf::from(format!("foreign/source-{name}.rs"));
    let absolute = std::env::current_dir().unwrap().join(&local);
    let item = |filename, crate_id, present: bool| {
        let mut item = prototype.clone();
        item.crate_id = crate_id;
        item.span = present.then_some(Span {
            filename,
            begin: (1, 0),
            end: (2, 0),
        });
        item
    };
    let items = [
        item(local.clone(), 0, true),
        item(other.clone(), 0, true),
        item(local.clone(), 0, true),
        item(foreign, 1, true),
        item(absolute, 0, true),
        item(local.clone(), 0, false),
    ];
    let expected = BTreeSet::from([local.as_path(), other.as_path()]);
    assert_eq!(
        relative_spans::sources(&items),
        expected,
        "source spans must retain exactly the local relative paths"
    );
}
