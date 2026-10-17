use crate::{Error, Result, client::FileDiff};
use std::fmt::Write as _;

/// Render exact captured endpoints using the followed diff-mode preference.
/// Auto uses unified output in the 80-column line client; split retains both
/// columns without truncating their text. Computation and output stay bounded.
pub fn render_command_diff(
    before: &[u8],
    after: &[u8],
    mode: &str,
    maximum_bytes: usize,
    maximum_work: usize,
) -> Result<String> {
    let diff = FileDiff::between(Some(before), Some(after), maximum_bytes, maximum_work)?;
    let Some(diff) = diff else {
        return Ok("No changes\n".into());
    };
    if diff.binary || matches!(mode, "auto" | "unified") {
        return Ok(diff.patch);
    }
    if mode != "split" {
        return Err(Error::Invalid("unknown diff mode".into()));
    }
    let old = std::str::from_utf8(before).map_err(|_| Error::Unsupported("binary diff".into()))?;
    let new = std::str::from_utf8(after).map_err(|_| Error::Unsupported("binary diff".into()))?;
    let mut old = old.lines();
    let mut new = new.lines();
    let mut output = String::from("before | after\n");
    loop {
        match (old.next(), new.next()) {
            (None, None) => break,
            (left, right) => {
                writeln!(output, "{} | {}", left.unwrap_or(""), right.unwrap_or("")).unwrap();
            }
        }
        if output.len() > maximum_bytes {
            return Err(Error::Invalid("diff view exceeds display bound".into()));
        }
    }
    Ok(output)
}
