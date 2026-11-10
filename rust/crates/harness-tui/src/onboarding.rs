//! Host-only setup input. Secret bytes are never echoed or retained in a
//! conversation, draft, history or diagnostic; the input allocation is wiped.
use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use std::io::{self, IsTerminal, Write};
use zeroize::Zeroizing;

// Grow by copying into a separately owned allocation. Replacing the wrapper
// wipes the old allocation before freeing it; String::reserve would not.
fn append(value: &mut Zeroizing<String>, text: &str) {
    if text.len() > value.capacity() - value.len() {
        let capacity = (value.len() + text.len()).max(value.capacity().saturating_mul(2));
        let mut grown = Zeroizing::new(String::with_capacity(capacity));
        grown.push_str(value);
        *value = grown;
    }
    value.push_str(text);
}

/// Whether stdin/stdout can participate in interactive setup.
pub fn interactive() -> bool {
    io::stdin().is_terminal()
        && io::stdout().is_terminal()
        && !crate::term::Environment::capture().is_dumb()
}

/// Reads one line on OS input events, without a readiness timer or polling.
/// The caller owns one native terminal through setup and the conversation;
/// dropping that owner restores raw mode, including on cancellation.
fn read(
    _terminal: &mut crate::term::Tty,
    label: &str,
    secret: bool,
) -> io::Result<Zeroizing<String>> {
    let mut output = io::stdout().lock();
    write!(output, "{label}: ")?;
    output.flush()?;
    let mut value = Zeroizing::new(String::new());
    loop {
        match event::read()? {
            Event::Key(key) if key.kind != KeyEventKind::Release => match key.code {
                KeyCode::Char('c' | 'd') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    return Err(io::Error::new(
                        io::ErrorKind::Interrupted,
                        "setup cancelled",
                    ));
                }
                KeyCode::Enter => {
                    write!(output, "\r\n")?;
                    output.flush()?;
                    return Ok(value);
                }
                KeyCode::Backspace => {
                    if value.pop().is_some() && !secret {
                        write!(output, "\x08 \x08")?;
                    }
                }
                KeyCode::Char(ch) if !ch.is_control() => {
                    let mut bytes = Zeroizing::new([0; 4]);
                    append(&mut value, ch.encode_utf8(&mut *bytes));
                    if !secret {
                        write!(output, "{ch}")?;
                    }
                }
                _ => {}
            },
            Event::Paste(text) => {
                let text = Zeroizing::new(text);
                append(&mut value, &text);
                if !secret {
                    write!(output, "{}", text.as_str())?;
                }
            }
            _ => {}
        }
        output.flush()?;
    }
}

/// Read a nonempty, single-line credential. Rejected input is wiped before
/// waiting for the next OS input event; cancellation never returns a key.
pub fn read_secret(terminal: &mut crate::term::Tty, label: &str) -> io::Result<Zeroizing<String>> {
    loop {
        let value = read(terminal, label, true)?;
        if !value.trim().is_empty() && !value.trim().chars().any(char::is_control) {
            return Ok(value);
        }
        print!("Paste a nonempty, single-line API key.\r\n");
    }
}

/// Numeric choices keep provider/model names out of the input parser.
pub fn choose(
    terminal: &mut crate::term::Tty,
    label: &str,
    choices: &[String],
) -> io::Result<usize> {
    for (index, choice) in choices.iter().enumerate() {
        print!("{}. {choice}\r\n", index + 1);
    }
    loop {
        let input = read(terminal, label, false)?;
        if let Ok(number) = input.trim().parse::<usize>()
            && let Some(index) = number.checked_sub(1).filter(|index| *index < choices.len())
        {
            return Ok(index);
        }
        print!("Choose a listed number.\r\n");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        #[test]
        fn input_survives_growth_and_backspace(actions in prop::collection::vec(
            prop::option::of(".{0,40}"), 0..64
        )) {
            let mut value = Zeroizing::new(String::new());
            let mut expected = String::new();
            for action in actions {
                if let Some(text) = action {
                    let capacity = value.capacity();
                    let pointer = value.as_ptr();
                    let fits = text.len() <= capacity - value.len();
                    append(&mut value, &text);
                    expected.push_str(&text);
                    if fits {
                        prop_assert_eq!(value.as_ptr(), pointer);
                    } else {
                        prop_assert_ne!(value.as_ptr(), pointer);
                    }
                } else {
                    prop_assert_eq!(value.pop(), expected.pop());
                }
                prop_assert_eq!(&*value, &expected);
            }
        }
    }
}
