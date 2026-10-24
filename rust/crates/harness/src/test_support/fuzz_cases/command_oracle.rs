// Independent syntax oracle shared by fuzzing and shrinking mutation controls.
/// Judges command syntax using an independent spelling of the grammar.
pub fn accepts(input: &str, maximum: u64, result: &crate::Result<Option<(&str, &str)>>) -> bool {
    if maximum == 0 || input.len() as u64 > maximum {
        return result.is_err();
    }
    let trimmed = input.trim_start();
    if !trimmed.starts_with('/') {
        return matches!(result, Ok(None));
    }
    let Ok(Some((name, arguments))) = result else {
        return false;
    };
    let rest = &trimmed[1..];
    let end = rest
        .char_indices()
        .find(|(_, ch)| ch.is_whitespace())
        .map_or(rest.len(), |(index, _)| index);
    *name == &rest[..end] && *arguments == rest[end..].trim()
}
