use super::*;

#[test]
fn quotes_and_escapes_remain_literal_facade_arguments() -> Result<()> {
    assert_eq!(
        parse_command("commit -m 'fix two words'")?,
        ["commit", "-m", "fix two words"]
    );
    assert_eq!(
        parse_command("grep \"$HOME and ;\" src\\ file")?,
        ["grep", "$HOME and ;", "src file"]
    );
    assert_eq!(parse_command("commit -m ''")?, ["commit", "-m", ""]);
    assert_eq!(
        parse_command("merge agents/child")?,
        ["merge", "agents/child"]
    );
    Ok(())
}

#[test]
fn malformed_and_composed_commands_are_rejected() {
    for command in [
        "",
        "  ",
        "status && merge agents/child",
        "status; whoami",
        "grep x | cat",
        "status > output",
        "commit -m 'open",
        "commit -m trailing\\",
        "status\0",
        "grep 'a\0b'",
    ] {
        assert!(parse_command(command).is_err(), "{command:?}");
    }
}
