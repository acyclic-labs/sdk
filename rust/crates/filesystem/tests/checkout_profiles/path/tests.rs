use acyclic_fs::kernel::NamespacePath;
use acyclic_fs::path::{PathError, PortablePath};
use acyclic_fs_format::model::VolumeLimits;

#[test]
fn parsing_is_canonical_and_bounded() {
    let limits = VolumeLimits::default();
    assert_eq!(
        PortablePath::parse("/a/b", limits).map(|path| path.depth()),
        Ok(2)
    );
    assert_eq!(
        PortablePath::parse("a", limits),
        Err(PathError::NotAbsolute)
    );
    assert_eq!(
        PortablePath::parse("/a//b", limits),
        Err(PathError::EmptyComponent)
    );
    assert_eq!(
        PortablePath::parse("/a/../b", limits),
        Err(PathError::ParentComponent)
    );
}

#[test]
fn ancestry_requires_component_boundaries() -> Result<(), PathError> {
    let limits = VolumeLimits::default();
    let shared = PortablePath::parse("/shared", limits)?;
    assert!(PortablePath::parse("/shared/a", limits)?.is_within(&shared));
    assert!(!PortablePath::parse("/shared-other", limits)?.is_within(&shared));
    Ok(())
}

#[test]
fn every_portable_path_boundary_and_accessor_is_explicit() -> Result<(), PathError> {
    let limits = VolumeLimits::default();
    let root = PortablePath::parse(PortablePath::ROOT, limits)?;
    assert_eq!(root.as_str(), "/");
    assert_eq!(root.depth(), 0);
    assert_eq!(root.components().collect::<Vec<_>>(), Vec::<&str>::new());
    assert!(root.is_within(&root));
    assert_eq!(root.to_string(), "/");

    let nested = PortablePath::parse("/ab/c", limits)?;
    assert_eq!(nested.components().collect::<Vec<_>>(), ["ab", "c"]);
    assert!(nested.is_within(&root));
    assert!(nested.is_within(&nested));

    for (input, expected) in [
        ("/a/", PathError::TrailingSeparator),
        ("/./a", PathError::CurrentComponent),
        ("/a\0b", PathError::Nul),
    ] {
        assert_eq!(PortablePath::parse(input, limits), Err(expected));
    }
    Ok(())
}

#[test]
fn complete_deep_paths_share_portable_and_namespace_semantics()
-> Result<(), Box<dyn std::error::Error>> {
    let spelling = "/leaf".repeat(8_193);
    let path = PortablePath::parse(&spelling, VolumeLimits::default())?;
    assert_eq!(path.depth(), 8_193);
    let namespace = NamespacePath::from_portable(&path, VolumeLimits::default())?;
    assert_eq!(namespace.depth(), path.depth());
    assert_eq!(namespace.encoded_bytes() as usize, spelling.len());
    assert!(
        namespace
            .components()
            .iter()
            .all(|name| name.as_bytes() == b"leaf")
    );
    Ok(())
}
