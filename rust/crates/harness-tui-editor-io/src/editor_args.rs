use std::{ffi::OsString, io};

/// Parse $EDITOR using the current platform's argument rules.
pub fn editor_command(command: &str) -> io::Result<(OsString, Vec<OsString>)> {
    if cfg!(windows) {
        windows_editor_command(command)
    } else {
        unix_editor_command(command)
    }
}

/// Parse Unix shell quoting without expanding variables or running a shell.
pub fn unix_editor_command(command: &str) -> io::Result<(OsString, Vec<OsString>)> {
    finish(shell_words::split(command).map_err(io::Error::other)?)
}

/// Parse Windows CRT quoting. Backslashes are literal except before a double
/// quote; single quotes are ordinary characters. No shell expansion occurs.
pub fn windows_editor_command(command: &str) -> io::Result<(OsString, Vec<OsString>)> {
    let mut input = command.chars().peekable();
    let mut words = vec![];
    loop {
        while input.peek().is_some_and(|c| matches!(c, ' ' | '\t')) {
            input.next();
        }
        if input.peek().is_none() {
            break;
        }
        let mut word = String::new();
        let mut quoted = false;
        while let Some(&character) = input.peek() {
            if !quoted && matches!(character, ' ' | '\t') {
                break;
            }
            let mut slashes = 0;
            while input.peek() == Some(&'\\') {
                input.next();
                slashes += 1;
            }
            if input.peek() == Some(&'"') {
                input.next();
                word.extend(std::iter::repeat_n('\\', slashes / 2));
                if slashes % 2 == 1 {
                    word.push('"');
                } else if quoted && input.peek() == Some(&'"') {
                    input.next();
                    word.push('"');
                } else {
                    quoted = !quoted;
                }
            } else {
                word.extend(std::iter::repeat_n('\\', slashes));
                // A slash run may end at whitespace or EOF; leave the delimiter
                // to the outer loop instead of adding it to this argument.
                if !quoted && input.peek().is_some_and(|c| matches!(c, ' ' | '\t')) {
                    break;
                }
                if let Some(character) = input.next() {
                    word.push(character);
                }
            }
        }
        if quoted {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "unclosed $EDITOR quote",
            ));
        }
        words.push(word);
    }
    finish(words)
}

fn finish(words: Vec<String>) -> io::Result<(OsString, Vec<OsString>)> {
    let mut words = words.into_iter();
    let program = words
        .next()
        .filter(|word| !word.is_empty())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "$EDITOR is empty"))?;
    Ok((program.into(), words.map(OsString::from).collect()))
}
