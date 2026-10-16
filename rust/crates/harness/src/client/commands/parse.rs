use crate::{Error, Result};

pub(super) fn has_frontmatter(bytes: &[u8]) -> bool {
    bytes.starts_with(b"---\n") || bytes.starts_with(b"---\r\n")
}

pub(super) fn expand(body: &str, arguments: &str, maximum_bytes: u64) -> Result<String> {
    if body.contains('\0') {
        return Err(Error::Invalid("command body contains NUL".into()));
    }
    let mut text = String::new();
    for (index, part) in body.split("$ARGUMENTS").enumerate() {
        let argument = if index == 0 { "" } else { arguments };
        let length = text
            .len()
            .checked_add(argument.len())
            .and_then(|n| n.checked_add(part.len()))
            .ok_or_else(|| Error::Invalid("expanded command size overflow".into()))?;
        if length as u64 > maximum_bytes {
            return Err(Error::Invalid("expanded command exceeds byte bound".into()));
        }
        text.push_str(argument);
        text.push_str(part);
    }
    if !body.contains("$ARGUMENTS") && !arguments.is_empty() {
        if text.len() as u64 + 1 + arguments.len() as u64 > maximum_bytes {
            return Err(Error::Invalid("expanded command exceeds byte bound".into()));
        }
        text.push('\n');
        text.push_str(arguments);
    }
    Ok(text)
}

pub(super) fn validate_name(name: &str) -> Result<()> {
    if name.split(':').any(|part| {
        part.is_empty()
            || !part
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    }) {
        return Err(Error::Invalid("invalid command name".into()));
    }
    Ok(())
}

// Splitting returns borrowed UTF-8 slices. Composer bounds apply equally to
// chat, known commands and unknown slash commands, before any provider I/O.
pub(super) fn split(input: &str, maximum_bytes: u64) -> Result<Option<(&str, &str)>> {
    if maximum_bytes == 0 || input.len() as u64 > maximum_bytes {
        return Err(Error::Invalid("command input exceeds byte bound".into()));
    }
    let Some(command) = input.trim_start().strip_prefix('/') else {
        return Ok(None);
    };
    let end = command.find(char::is_whitespace).unwrap_or(command.len());
    let (name, arguments) = command.split_at(end);
    Ok(Some((name, arguments.trim())))
}
