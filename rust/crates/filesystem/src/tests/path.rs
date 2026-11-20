use super::*;

mod properties {
    use super::*;
    use crate::kernel::{LogicalName, NameEncoding, NamespacePath};
    use crate::model::VolumeLimits;
    use proptest::prelude::*;

    fn limits() -> VolumeLimits {
        VolumeLimits {
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
