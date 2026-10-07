use super::*;

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
    let limits = VolumeLimits {
        maximum_path_bytes: 8,
        maximum_component_bytes: 3,
        maximum_path_depth: 2,
        ..VolumeLimits::default()
    };
    let root = PortablePath::parse(PortablePath::ROOT, limits)?;
    assert_eq!(root.as_str(), "/");
    assert_eq!(root.depth(), 0);
    assert_eq!(root.components().collect::<Vec<_>>(), Vec::<&str>::new());
    assert!(root.is_within(&root));
    assert_eq!(root.to_string(), "/");
    assert_eq!(format!("{root:?}"), "PortablePath(\"/\")");

    let nested = PortablePath::parse("/ab/c", limits)?;
    assert_eq!(nested.components().collect::<Vec<_>>(), ["ab", "c"]);
    assert!(nested.is_within(&root));
    assert!(nested.is_within(&nested));

    for (input, expected) in [
        ("/a/", PathError::TrailingSeparator),
        ("/./a", PathError::CurrentComponent),
        ("/a\0b", PathError::Nul),
        ("/abcd", PathError::ComponentTooLong),
        ("/a/b/c", PathError::TooDeep),
        ("/12345678", PathError::PathTooLong),
    ] {
        assert_eq!(PortablePath::parse(input, limits), Err(expected));
    }
    Ok(())
}

mod properties {
    use super::*;
    use crate::kernel::{LogicalName, NameEncoding, NamespacePath};
    use proptest::prelude::*;

    fn limits() -> VolumeLimits {
        VolumeLimits {
            maximum_path_bytes: 12,
            maximum_component_bytes: 4,
            maximum_path_depth: 3,
            ..VolumeLimits::default()
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        /// `PortablePath::parse` admits exactly the paths whose components
        /// are canonical UTF-8 logical names within the namespace bounds,
        /// and converts them to a `NamespacePath` that renders back unchanged.
        #[test]
        fn portable_parsing_agrees_with_namespace_paths(value in "[/a.\0\\\\é]{0,14}") {
            let components = value.strip_prefix('/').map(|rest| {
                if rest.is_empty() {
                    Ok(Vec::new())
                } else {
                    rest.split('/')
                        .map(|component| {
                            LogicalName::new(
                                NameEncoding::Utf8,
                                component.as_bytes().to_vec(),
                                limits().maximum_component_bytes,
                            )
                        })
                        .collect::<Result<Vec<_>, _>>()
                }
            });
            let expected = components.is_some_and(|components| {
                components.is_ok_and(|components| NamespacePath::new(components, limits()).is_ok())
            });
            let parsed = PortablePath::parse(&value, limits());
            prop_assert_eq!(parsed.is_ok(), expected, "{:?}", parsed);
            if let Ok(parsed) = parsed {
                let namespace = NamespacePath::from_portable(&parsed, limits())
                    .expect("admitted portable paths convert");
                let rendered = namespace
                    .components()
                    .iter()
                    .map(|name| std::str::from_utf8(name.as_bytes()).expect("UTF-8 name"))
                    .fold(String::new(), |path, name| path + "/" + name);
                prop_assert_eq!(if rendered.is_empty() { "/" } else { &rendered }, value.as_str());
            }

            let relative = value.strip_prefix('/').unwrap_or(&value);
            let unbounded = VolumeLimits::default();
            prop_assert_eq!(
                is_canonical_relative(relative),
                !relative.is_empty()
                    && !relative.contains('\\')
                    && PortablePath::parse(&format!("/{relative}"), unbounded).is_ok()
            );
        }
    }
}
