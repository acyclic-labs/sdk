use super::{Fixtures, gated, named};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};
use syn::ext::IdentExt;
use syn::visit::Visit;

type Sources = BTreeMap<PathBuf, (String, Fixtures)>;

fn opaque_sources(tokens: proc_macro2::TokenStream) -> bool {
    tokens.into_iter().any(|token| match token {
        proc_macro2::TokenTree::Ident(name) => name.unraw() == "include" || name.unraw() == "mod",
        proc_macro2::TokenTree::Group(group) => opaque_sources(group.stream()),
        _ => false,
    })
}

pub(super) fn sources(
    roots: &[(PathBuf, Fixtures)],
    read: impl Fn(&Path) -> Result<String, String>,
) -> Result<Sources, String> {
    let mut tree = Tree {
        read,
        sources: BTreeMap::new(),
        active: Vec::new(),
        directory: PathBuf::new(),
        path_directory: PathBuf::new(),
        file: PathBuf::new(),
        error: None,
    };
    for (path, fixtures) in roots {
        tree.file(path, *fixtures, false);
    }
    tree.error.map_or(Ok(tree.sources), Err)
}

struct Tree<R> {
    read: R,
    sources: Sources,
    active: Vec<PathBuf>,
    directory: PathBuf,
    path_directory: PathBuf,
    file: PathBuf,
    error: Option<String>,
}

impl<R: Fn(&Path) -> Result<String, String>> Tree<R> {
    fn file(&mut self, path: &Path, fixtures: Fixtures, explicit: bool) {
        if self.error.is_some() {
            return;
        }
        if self.active.iter().any(|active| active == path) {
            self.error = Some(format!("cyclic module: {}", path.display()));
            return;
        }
        if self.sources.contains_key(path) {
            return;
        }
        let source = match (self.read)(path) {
            Ok(source) => source,
            Err(error) => {
                self.error = Some(error);
                return;
            }
        };
        let ast = match syn::parse_file(&source) {
            Ok(ast) => ast,
            Err(error) => {
                self.error = Some(format!("{}: {error}", path.display()));
                return;
            }
        };
        if gated(&ast.attrs, false) {
            return;
        }
        self.sources.insert(path.to_owned(), (source, fixtures));
        self.active.push(path.to_owned());
        let parent = path.parent().expect("module parent");
        let directory = if explicit || path.file_name().is_some_and(|name| name == "mod.rs") {
            parent.to_owned()
        } else {
            parent.join(path.file_stem().expect("module name"))
        };
        let previous_directory = std::mem::replace(&mut self.directory, directory);
        let previous_path_directory =
            std::mem::replace(&mut self.path_directory, parent.to_owned());
        let previous_file = std::mem::replace(&mut self.file, path.to_owned());
        self.visit_file(&ast);
        self.directory = previous_directory;
        self.path_directory = previous_path_directory;
        self.file = previous_file;
        self.active.pop();
    }
}

impl<'ast, R: Fn(&Path) -> Result<String, String>> Visit<'ast> for Tree<R> {
    fn visit_item_mod(&mut self, item: &'ast syn::ItemMod) {
        if gated(&item.attrs, false) || self.error.is_some() {
            return;
        }
        let mut explicit = None;
        for attr in &item.attrs {
            if named(attr.path(), "path") {
                if explicit.is_some() {
                    self.error = Some("duplicate module path".into());
                    return;
                }
                if let syn::Meta::NameValue(value) = &attr.meta
                    && let syn::Expr::Lit(value) = &value.value
                    && let syn::Lit::Str(value) = &value.lit
                {
                    explicit = Some(value.value());
                } else {
                    self.error = Some("nonliteral module path".into());
                    return;
                }
            }
            // Conditional path expansion is deliberately unsupported, never
            // silently resolved against whichever host runs the test.
            if named(attr.path(), "cfg_attr") {
                self.error =
                    Some("conditional module attributes require explicit resolution".into());
                return;
            }
        }
        if let Some((_, items)) = &item.content {
            let directory = explicit.map_or_else(
                || self.directory.join(item.ident.unraw().to_string()),
                |path| self.path_directory.join(path),
            );
            let previous_path = std::mem::replace(&mut self.path_directory, directory.clone());
            let previous = std::mem::replace(&mut self.directory, directory);
            for item in items {
                self.visit_item(item);
            }
            self.directory = previous;
            self.path_directory = previous_path;
        } else {
            let explicit_path = explicit.is_some();
            let path = if let Some(explicit) = explicit {
                self.path_directory.join(explicit)
            } else {
                let name = item.ident.unraw().to_string();
                let flat = self.directory.join(format!("{name}.rs"));
                let nested = self.directory.join(name).join("mod.rs");
                match ((self.read)(&flat).is_ok(), (self.read)(&nested).is_ok()) {
                    (true, false) => flat,
                    (false, true) => nested,
                    _ => {
                        self.error = Some(format!(
                            "unresolved or ambiguous module {} in {}",
                            item.ident,
                            self.file.display()
                        ));
                        return;
                    }
                }
            };
            self.file(&path, Fixtures::None, explicit_path);
        }
    }

    fn visit_macro(&mut self, item: &'ast syn::Macro) {
        if item
            .path
            .segments
            .last()
            .is_some_and(|segment| segment.ident.unraw() == "include")
            || opaque_sources(item.tokens.clone())
        {
            self.error = Some(format!(
                "runtime include! requires explicit source resolution in {}",
                self.file.display()
            ));
        }
        syn::visit::visit_macro(self, item);
    }

    fn visit_use_tree(&mut self, item: &'ast syn::UseTree) {
        if matches!(item, syn::UseTree::Name(name) if name.ident.unraw() == "include")
            || matches!(item, syn::UseTree::Rename(name) if name.ident.unraw() == "include")
        {
            self.error = Some("aliased include! requires explicit source resolution".into());
        }
        syn::visit::visit_use_tree(self, item);
    }
}

#[cfg(test)]
mod tests {
    use super::super::policy;
    use super::*;

    fn scan(files: &BTreeMap<PathBuf, String>) -> Result<Sources, String> {
        sources(&[(PathBuf::from("src/root.rs"), Fixtures::None)], |path| {
            files
                .get(path)
                .cloned()
                .ok_or_else(|| format!("missing {}", path.display()))
        })
    }

    proptest::proptest! {
        #![proptest_config(proptest::test_runner::Config::with_cases(32))]
        #[test]
        fn descendants_cannot_hide_a_planted_predicate(depth in 1usize..6, explicit in proptest::bool::ANY) {
            let mut files = BTreeMap::new();
            let mut path = PathBuf::from("src/root.rs");
            for level in 0..depth {
                let child = if explicit { path.parent().unwrap().join(format!("child_{level}.rs")) }
                    else { path.with_extension("").join("child.rs") };
                files.insert(path.clone(), if explicit {
                    format!("#[path = \"child_{level}.rs\"] mod child;")
                } else { "mod child;".into() });
                path = child;
            }
            files.insert(path.clone(), "#[cfg(feature = \"test-support\")] fn resolve() {}".into());
            let found = scan(&files).unwrap();
            proptest::prop_assert_eq!(found.len(), depth + 1);
            proptest::prop_assert_eq!(found.values().map(|(source, fixtures)| policy(source, *fixtures).violations).sum::<usize>(), 1);
            // Reverted hand-kept root list misses the same planted child.
            proptest::prop_assert_eq!(policy(&files[Path::new("src/root.rs")], Fixtures::None).violations, 0);
        }
    }

    #[test]
    fn explicit_paths_are_read_and_missing_or_included_children_fail_closed() {
        let mut files = BTreeMap::from([
            (
                PathBuf::from("src/root.rs"),
                "#[path = \"hidden.rs\"] mod child; #[cfg(test)] mod tests;".into(),
            ),
            (
                PathBuf::from("src/hidden.rs"),
                "#[cfg(feature = \"test-support\")] fn resolve() {}".into(),
            ),
        ]);
        assert_eq!(scan(&files).unwrap().len(), 2);
        files.remove(Path::new("src/hidden.rs"));
        assert!(scan(&files).is_err());
        for source in [
            "mod child;",
            "include!(\"hidden.rs\");",
            "fn open() { include!(\"hidden.rs\"); }",
            "use std::include as hidden; hidden!(\"hidden.rs\");",
            "use std::r#include as hidden; hidden!(\"hidden.rs\");",
            "macro_rules! hidden { () => { include!(\"hidden.rs\"); } } hidden!();",
            "macro_rules! hidden { () => { mod child; } } hidden!();",
        ] {
            files.insert(PathBuf::from("src/root.rs"), source.into());
            assert!(scan(&files).is_err());
        }
    }

    #[test]
    fn module_directories_match_the_compiler() {
        for ((declaration, child, leaf), compiler_reached) in
            acyclic_test_support::module_cases::CASES
                .into_iter()
                .zip(acyclic_test_support::MODULE_COMPILER_REACHED)
        {
            let directory = tempfile::tempdir().unwrap();
            let root = directory.path();
            for (file, text) in [
                ("lib.rs", "mod root;"),
                ("root.rs", declaration),
                (child, "mod leaf;"),
                (
                    leaf,
                    "#[cfg(feature = \"test-support\")] compile_error!(\"policy descendant reached\");",
                ),
            ] {
                let file = root.join(file);
                std::fs::create_dir_all(file.parent().unwrap()).unwrap();
                std::fs::write(file, text).unwrap();
            }
            let found = sources(&[(root.join("root.rs"), Fixtures::None)], |path| {
                std::fs::read_to_string(path).map_err(|error| error.to_string())
            });
            assert!(found.is_ok(), "{declaration}: {:?}", found.as_ref().err());
            assert_eq!(
                found
                    .unwrap()
                    .values()
                    .map(|(source, fixtures)| policy(source, *fixtures).violations)
                    .sum::<usize>(),
                usize::from(compiler_reached)
            );
        }
    }
}
