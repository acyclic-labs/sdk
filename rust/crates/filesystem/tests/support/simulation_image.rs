use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore, Workspace};
use std::{collections::BTreeMap, error::Error};

pub(crate) type Image = BTreeMap<String, Vec<u8>>;

pub(crate) async fn check<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>(
    workspace: &Workspace<A, O>,
    expected: &Image,
) -> Result<(), Box<dyn Error>> {
    // Reopen the immutable generation, rather than trusting a checkout cache.
    let head = unbounded!(workspace, head())?;
    let reopened = unbounded!(workspace, generation(head.id()))?;
    let mut names = Vec::new();
    let mut after = None;
    loop {
        let (page, _allocation) = unbounded!(workspace, list_directory("/", after.as_ref(), 3))?;
        names.extend(
            page.entries
                .iter()
                .map(|entry| entry.name.as_bytes().to_vec()),
        );
        if !page.has_more {
            break;
        }
        after = Some(
            page.entries
                .last()
                .ok_or("empty nonterminal page")?
                .name
                .clone(),
        );
    }
    assert_eq!(
        names,
        expected
            .keys()
            .map(|path| path.as_bytes()[1..].to_vec())
            .collect::<Vec<_>>(),
        "publication exposed an unexpected or missing file"
    );
    for (path, bytes) in expected {
        assert_eq!(unbounded!(workspace, read(path, 256))?.as_ref(), bytes);
        assert_eq!(unbounded!(reopened, read(path, 256))?.as_ref(), bytes);
    }
    Ok(())
}
