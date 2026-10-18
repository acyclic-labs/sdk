use std::io::{Error, ErrorKind, Result};
use std::path::{Component, Path};

pub(super) fn pattern(path: &Path) -> Result<String> {
    let mut result = String::new();
    for part in path.components() {
        let Component::Normal(part) = part else {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "mount exclusion must be relative",
            ));
        };
        let name = part
            .to_str()
            .filter(|name| !name.contains(['\r', '\n', '\0']))
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::Unsupported,
                    "mount path cannot be represented in Git exclusions",
                )
            })?;
        result.push('/');
        for character in name.chars() {
            if matches!(character, '*' | '?' | '[' | '\\' | '!' | '#') || character.is_whitespace()
            {
                result.push('\\');
            }
            result.push(character);
        }
    }
    if result.is_empty() {
        return Err(Error::new(ErrorKind::InvalidInput, "empty mount exclusion"));
    }
    Ok(result)
}
