use super::*;

#[test]
fn patch_paths_are_routed_and_cannot_escape() {
    let fixture = Fixture::new();
    let child = fixture.directory("workspace");
    let input = json!({"command":"*** Begin Patch\n*** Add File: src/new.rs\n*** Move to: src/moved.rs\n*** End Patch"});
    let rewritten = rewrite_tool_input("apply_patch", input, &fixture.root, &child).unwrap();
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
                &fixture.root, &child).is_err());
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
        &fixture.root,
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
    assert!(
        rewrite_tool_input(
            "exec_command",
            json!({"cmd":format!("type {root}\\secret"),"workdir":root}),
            &fixture.root,
            &child
        )
        .is_err()
    );
    for tool in ["exec_command", "mcp__shell__exec_command"] {
        let rewritten = rewrite_tool_input(
            tool,
            json!({"cmd":"type relative.txt","workdir":fixture.root}),
            &fixture.root,
            &child,
        )
        .unwrap();
        assert_eq!(rewritten["workdir"], child.display().to_string());
    }
    let rewritten = rewrite_tool_input(
        "PowerShell",
        json!({"command":"Get-Location"}),
        &fixture.root,
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
            &fixture.root,
            &child
        )
        .is_err()
    );
}
