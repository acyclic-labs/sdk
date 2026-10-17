use crate::{Error, Result};

/// Splits a facade command into arguments without executing shell expansion.
pub fn parse_command(command: &str) -> Result<Vec<String>> {
    let mut arguments = Vec::new();
    let mut word = String::new();
    let mut quote = None;
    let mut escaped = false;
    let mut started = false;
    for character in command.chars() {
        if character == '\0' {
            return Err(Error::Invalid("fs command contains NUL".into()));
        }
        if escaped {
            word.push(character);
            escaped = false;
        } else if character == '\\' && quote != Some('\'') {
            escaped = true;
            started = true;
        } else if let Some(delimiter) = quote {
            if character == delimiter {
                quote = None;
            } else {
                word.push(character);
            }
        } else if matches!(character, '\'' | '"') {
            quote = Some(character);
            started = true;
        } else if character.is_whitespace() {
            if started {
                arguments.push(std::mem::take(&mut word));
                started = false;
            }
        } else if matches!(character, ';' | '|' | '&' | '<' | '>' | '`') {
            return Err(Error::Invalid("fs accepts one facade command".into()));
        } else {
            word.push(character);
            started = true;
        }
    }
    if escaped || quote.is_some() {
        return Err(Error::Invalid("unterminated fs command argument".into()));
    }
    if started {
        arguments.push(word);
    }
    if arguments.is_empty() {
        return Err(Error::Invalid("fs command is empty".into()));
    }
    Ok(arguments)
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        // Quoting is a lossless codec for every non-NUL Unicode argument,
        // including empty strings, metacharacters and whitespace.
        #[test]
        fn quoted_arguments_round_trip(
            arguments in prop::collection::vec(
                prop::collection::vec(any::<char>().prop_filter("non-NUL", |c| *c != '\0'), 0..32)
                    .prop_map(|characters| characters.into_iter().collect::<String>()),
                1..8,
            ),
        ) {
            let command = arguments.iter().map(|argument| {
                format!("\"{}\"", argument.replace('\\', "\\\\").replace('"', "\\\""))
            }).collect::<Vec<_>>().join(" ");
            prop_assert_eq!(parse_command(&command).unwrap(), arguments);
        }

        #[test]
        fn nul_is_always_rejected(prefix in ".{0,64}", suffix in ".{0,64}") {
            let command = format!("{prefix}\0{suffix}");
            prop_assert!(parse_command(&command).is_err());
        }
    }
}
