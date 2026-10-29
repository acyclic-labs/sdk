use crate::{Error, Result};

// Splitting returns borrowed UTF-8 slices. Composer bounds apply equally to
// chat, known commands and unknown slash commands, before any provider I/O.
pub(super) fn split(input: &str, maximum_bytes: Option<u64>) -> Result<Option<(&str, &str)>> {
    if maximum_bytes.is_some_and(|maximum| maximum == 0 || input.len() as u64 > maximum) {
        return Err(Error::Invalid("command input exceeds byte bound".into()));
    }
    let Some(command) = input.trim_start().strip_prefix('/') else {
        return Ok(None);
    };
    let end = command.find(char::is_whitespace).unwrap_or(command.len());
    let (name, arguments) = command.split_at(end);
    Ok(Some((name, arguments.trim())))
}
