//! Module path cases shared by the host compiler proof and target source scan.

/// Each declaration, its child module file, and the descendant it must load.
pub const CASES: [(&str, &str, &str); 7] = [
    ("mod child;", "root/child.rs", "root/child/leaf.rs"),
    ("mod child;", "root/child/mod.rs", "root/child/leaf.rs"),
    ("#[path = \"hidden.rs\"] mod child;", "hidden.rs", "leaf.rs"),
    (
        "mod nested { mod child; }",
        "root/nested/child.rs",
        "root/nested/child/leaf.rs",
    ),
    (
        "mod nested { #[path = \"hidden.rs\"] mod child; }",
        "root/nested/hidden.rs",
        "root/nested/leaf.rs",
    ),
    (
        "#[path = \"chosen\"] mod nested { mod child; }",
        "chosen/child.rs",
        "chosen/child/leaf.rs",
    ),
    (
        "#[path = \"alternate/mod.rs\"] mod child;",
        "alternate/mod.rs",
        "alternate/leaf.rs",
    ),
];
