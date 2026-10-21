use super::*;

/// The single-root view a child of `fixture` sees at `child`.
fn views(fixture: &Fixture, child: &Path) -> [RootView; 1] {
    [RootView::new(&fixture.root, child.to_path_buf())]
}

#[test]
fn patch_paths_are_routed_and_cannot_escape() {
    let fixture = Fixture::new();
    let child = fixture.directory("workspace");
    let input = json!({"command":"*** Begin Patch\n*** Add File: src/new.rs\n*** Move to: src/moved.rs\n*** End Patch"});
    let rewritten =
        rewrite_tool_input("apply_patch", input, &views(&fixture, &child), &child).unwrap();
    for name in ["src/new.rs", "src/moved.rs"] {
        assert!(
            rewritten["command"]
                .as_str()
                .unwrap()
                .contains(&child.join(name).display().to_string())
        );
    }
    let outside = fixture
        .temporary
        .path()
        .join("outside.txt")
        .display()
        .to_string();
    for directive in [
        "Add File",
        "Update File",
        "Delete File",
        "Move to",
        "Copy to",
    ] {
        for path in ["../parent.txt", &outside] {
            assert!(rewrite_tool_input("apply_patch",
                json!({"command":format!("*** Begin Patch\n*** {directive}: {path}\n*** End Patch")}),
                &views(&fixture, &child), &child).is_err());
        }
    }
}

#[test]
fn structured_paths_are_routed_and_cannot_escape() {
    let fixture = Fixture::new();
    let child = fixture.directory("workspace");
    validate_tool_paths("Read", &json!({"path":child.join("src/lib.rs")}), &child).unwrap();
    let rewritten = rewrite_tool_input(
        "Write",
        json!({"file_path":"relative.txt","content":"child"}),
        &views(&fixture, &child),
        &child,
    )
    .unwrap();
    assert_eq!(
        rewritten["file_path"],
        child.join("relative.txt").display().to_string()
    );
    for path in [
        PathBuf::from("../parent"),
        fixture.temporary.path().join("outside/file.rs"),
    ] {
        assert!(validate_tool_paths("Read", &json!({"path":path}), &child).is_err());
    }
}

#[test]
fn shell_routing_denies_parent_paths_and_expansion() {
    let fixture = Fixture::new();
    let child = fixture.directory("workspace");
    for command in [
        "type $env:TEMP\\secret",
        "cat `pwd`/secret",
        "cat ~/secret",
        "cat<../outside",
        "echo value>../outside",
        "echo value 2>../outside",
    ] {
        assert!(validate_shell_paths(command, &child).is_err());
    }
    let root = fixture.root.display().to_string();
    let rewritten = rewrite_tool_input(
        "exec_command",
        json!({"cmd":format!("type {root}{}secret", std::path::MAIN_SEPARATOR),"workdir":root}),
        &views(&fixture, &child),
        &child,
    )
    .unwrap();
    assert_eq!(
        rewritten["cmd"],
        format!("type {}", child.join("secret").display())
    );
    assert_eq!(rewritten["workdir"], child.display().to_string());
    for tool in ["exec_command", "mcp__shell__exec_command"] {
        let rewritten = rewrite_tool_input(
            tool,
            json!({"cmd":"type relative.txt","workdir":fixture.root}),
            &views(&fixture, &child),
            &child,
        )
        .unwrap();
        assert_eq!(rewritten["workdir"], child.display().to_string());
    }
    let rewritten = rewrite_tool_input(
        "PowerShell",
        json!({"command":"Get-Location"}),
        &views(&fixture, &child),
        &child,
    )
    .unwrap();
    assert!(
        rewritten["command"]
            .as_str()
            .unwrap()
            .contains(&child.display().to_string())
    );
}

#[cfg(unix)]
#[test]
fn symlinks_cannot_escape_the_child() {
    let fixture = Fixture::new();
    let child = fixture.directory("workspace");
    let outside = fixture.directory("outside");
    std::os::unix::fs::symlink(&outside, child.join("escape")).unwrap();
    assert!(
        validate_tool_paths("Read", &json!({"path":child.join("escape/file")}), &child).is_err()
    );
    assert!(
        rewrite_tool_input(
            "apply_patch",
            json!({"command":"*** Begin Patch\n*** Add File: escape/file\n*** End Patch"}),
            &views(&fixture, &child),
            &child
        )
        .is_err()
    );
}

#[test]
fn parent_root_paths_name_the_same_path_in_the_child_view() {
    let fixture = Fixture::new();
    let child = fixture.directory("workspace");
    let views = views(&fixture, &child);
    let root = |path: &str| fixture.root.join(path).display().to_string();
    // A file tool given the parent's absolute path writes the child's copy.
    let rewritten = rewrite_tool_input(
        "Write",
        json!({"file_path":root("src/new.rs"),"content":format!("see {}", root("README.md"))}),
        &views,
        &child,
    )
    .unwrap();
    assert_eq!(
        rewritten["file_path"],
        child.join("src/new.rs").display().to_string()
    );
    // File content is never rewritten.
    assert_eq!(rewritten["content"], format!("see {}", root("README.md")));
    // The root itself names the view.
    let rewritten = rewrite_tool_input(
        "Glob",
        json!({"path":fixture.root,"pattern":"*.rs"}),
        &views,
        &child,
    )
    .unwrap();
    assert_eq!(rewritten["path"], child.display().to_string());
    // A patch maps its headers and keeps its body.
    let rewritten = rewrite_tool_input(
        "apply_patch",
        json!({"command":format!(
            "*** Begin Patch\n*** Add File: {}\n+{}\n*** End Patch",
            root("added.rs"),
            root("body.rs")
        )}),
        &views,
        &child,
    )
    .unwrap();
    assert_eq!(
        rewritten["command"],
        format!(
            "*** Begin Patch\n*** Add File: {}\n+{}\n*** End Patch",
            child.join("added.rs").display(),
            root("body.rs")
        )
    );
}

#[test]
fn paths_outside_the_parent_root_keep_the_ordinary_access_decision() {
    let fixture = Fixture::new();
    let child = fixture.directory("workspace");
    let views = views(&fixture, &child);
    // A sibling whose name extends the root's is not under the root.
    let sibling = fixture.directory("root-sibling").join("file.rs");
    let outside = fixture.directory("outside").join("file.rs");
    for path in [sibling, outside] {
        let error = rewrite_tool_input("Read", json!({"file_path":path}), &views, &child)
            .expect_err("a path outside the root stays denied");
        assert!(
            error.contains("escapes the isolated agent workspace"),
            "{error}"
        );
    }
    // A path already in the child's view is kept as written.
    let inside = child.join("kept.rs").display().to_string();
    let rewritten =
        rewrite_tool_input("Read", json!({"file_path":inside}), &views, &child).unwrap();
    assert_eq!(rewritten["file_path"], inside);
}

#[test]
fn every_parent_root_maps_to_its_own_view() {
    let fixture = Fixture::new();
    let second_root = fixture.directory("second");
    let first_view = fixture.directory("views/first");
    let second_view = fixture.directory("views/second");
    let views = [
        RootView::new(&fixture.root, first_view.clone()),
        RootView::new(&second_root, second_view.clone()),
    ];
    let command = format!(
        "cat {} {}",
        fixture.root.join("a.txt").display(),
        second_root.join("b.txt").display()
    );
    let rewritten = rewrite_tool_input(
        "Bash",
        json!({"command":command}),
        &views,
        &fixture.directory("views"),
    )
    .unwrap();
    assert!(
        rewritten["command"].as_str().unwrap().ends_with(&format!(
            "cat {} {}",
            first_view.join("a.txt").display(),
            second_view.join("b.txt").display()
        )),
        "{rewritten}"
    );
}

/// Hosts spell one checkout root several ways: Windows paths differ in case,
/// separators and the verbatim prefix, and a root may be named through a
/// link that its canonical path resolves.
#[test]
fn every_spelling_of_the_parent_root_names_the_view() {
    let fixture = Fixture::new();
    let child = fixture.directory("workspace");
    let views = views(&fixture, &child);
    let root = fixture.root.display().to_string();
    let canonical = fixture.root.canonicalize().unwrap().display().to_string();
    let mut spellings = vec![root.clone(), canonical];
    if cfg!(windows) {
        spellings.extend([
            root.replace('\\', "/"),
            root.to_uppercase(),
            root.to_lowercase(),
            format!(r"\\?\{root}"),
        ]);
    }
    for spelling in spellings {
        let separator = if spelling.contains('/') && cfg!(windows) {
            "/"
        } else {
            std::path::MAIN_SEPARATOR_STR
        };
        let rewritten = rewrite_tool_input(
            "Read",
            json!({"file_path":format!("{spelling}{separator}file.rs")}),
            &views,
            &child,
        )
        .unwrap_or_else(|error| panic!("{spelling}: {error}"));
        let mapped = PathBuf::from(rewritten["file_path"].as_str().unwrap());
        assert!(
            mapped.starts_with(&child) && mapped.ends_with("file.rs"),
            "{spelling} mapped to {}",
            mapped.display()
        );
    }
}
