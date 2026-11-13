use crate::{Error, Result};

/// Splits a facade command into arguments without executing shell expansion.
pub fn parse_command(command: &str) -> Result<Vec<String>> {
    let mut arguments = Vec::new();
    let mut word = String::new();
    visit(command, |character| match character {
        Some(character) => word.push(character),
        None => arguments.push(std::mem::take(&mut word)),
    })?;
    Ok(arguments)
}

/// Compare decoded argument prefixes without allocating a second argument list.
pub(super) fn has_prefix<const N: usize>(command: &str, prefixes: [&[&str]; N]) -> Result<bool> {
    let mut matched = [true; N];
    let mut word = 0;
    let mut offset = 0;
    visit(command, |character| {
        for (prefix, matched) in prefixes.iter().zip(&mut matched) {
            if let Some(expected) = prefix.get(word) {
                *matched &= match character {
                    Some(character) => expected.chars().nth(offset) == Some(character),
                    None => expected.chars().count() == offset,
                };
            }
        }
        if character.is_some() {
            offset += 1;
        } else {
            word += 1;
            offset = 0;
        }
    })?;
    Ok(prefixes
        .iter()
        .zip(matched)
        .any(|(prefix, matched)| matched && word >= prefix.len()))
}

/// Select the explicit agent-scope query without allocating parsed arguments.
pub(crate) fn scoped_search(command: &str) -> Result<bool> {
    if !has_prefix(command, [&["grep"], &["git", "grep"]])? {
        return Ok(false);
    }
    const OPTIONS: [&str; 7] = [
        "--scope",
        "--",
        "--page-size",
        "--cursor",
        "--file",
        "--directory",
        "--volume",
    ];
    let mut matched = [true; OPTIONS.len()];
    let mut offset = 0;
    let mut operand = false;
    let mut literal = false;
    let mut found = false;
    visit(command, |character| {
        for (expected, matched) in OPTIONS.iter().zip(&mut matched) {
            *matched &= match character {
                Some(character) => expected.chars().nth(offset) == Some(character),
                None => expected.chars().count() == offset,
            };
        }
        if character.is_some() {
            offset += 1;
            return;
        }
        if !literal {
            if operand {
                operand = false;
            } else {
                found |= matched[0];
                literal = matched[1];
                operand = matched
                    .iter()
                    .enumerate()
                    .any(|(index, yes)| index != 1 && *yes);
            }
        }
        matched.fill(true);
        offset = 0;
    })?;
    Ok(found)
}
fn visit(command: &str, mut emit: impl FnMut(Option<char>)) -> Result<()> {
    let mut quote = None;
    let mut escaped = false;
    let mut started = false;
    let mut words = 0;
    for character in command.chars() {
        if character == '\0' {
            return Err(Error::Invalid("fs command contains NUL".into()));
        }
        if escaped {
            emit(Some(character));
            escaped = false;
        } else if character == '\\' && quote != Some('\'') {
            escaped = true;
            started = true;
        } else if let Some(delimiter) = quote {
            if character == delimiter {
                quote = None;
            } else {
                emit(Some(character));
            }
        } else if matches!(character, '\'' | '"') {
            quote = Some(character);
            started = true;
        } else if character.is_whitespace() {
            if started {
                emit(None);
                words += 1;
                started = false;
            }
        } else if matches!(character, ';' | '|' | '&' | '<' | '>' | '`') {
            return Err(Error::Invalid("fs accepts one facade command".into()));
        } else {
            emit(Some(character));
            started = true;
        }
    }
    if escaped || quote.is_some() {
        return Err(Error::Invalid("unterminated fs command argument".into()));
    }
    if started {
        emit(None);
        words += 1;
    }
    if words == 0 {
        return Err(Error::Invalid("fs command is empty".into()));
    }
    Ok(())
}

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod tests {
    use super::*;
    #[test]
    fn scope_routing_respects_option_operands_and_literal_terminators() -> Result<()> {
        for (command, expected) in [
            ("grep --scope . needle", true),
            ("'git' 'grep' --page-size 1 --scope . needle", true),
            ("grep --directory . needle", false),
            ("grep -- needle --scope .", false),
            ("grep --cursor --scope needle", false),
            ("status --scope .", false),
        ] {
            assert_eq!(scoped_search(command)?, expected, "{command}");
        }
        Ok(())
    }
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
        fn routing_and_execution_decode_the_same_quoted_prefix(
            prefix in prop::sample::select(vec![vec!["grep", "--scope"], vec!["git", "grep", "--scope"], vec!["grep", "--volume"], vec!["status"]]),
            operand in r"[^\x00]{0,64}",
            quote in any::<bool>(),
        ) {
            let command = prefix.iter().map(|word| if quote { format!("'{word}'") } else { word.to_string() }).chain([
                format!("\"{}\"", operand.replace('\\', "\\\\").replace('"', "\\\"")),
            ]).collect::<Vec<_>>().join(" ");
            let args = parse_command(&command).unwrap();
            let args = if args.first().is_some_and(|arg| arg == "git") { &args[1..] } else { &args[..] };
            prop_assert_eq!(has_prefix(&command, [&["grep"], &["git", "grep"]]).unwrap(), args.starts_with(&["grep".into()]));
        }

        #[test]
        fn nul_is_always_rejected(prefix in ".{0,64}", suffix in ".{0,64}") {
            let command = format!("{prefix}\0{suffix}");
            prop_assert!(parse_command(&command).is_err());
        }
    }
}
