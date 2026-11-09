//! Host-only setup input. Secret bytes are never echoed or retained in a
//! conversation, draft, history or diagnostic; the input allocation is wiped.
use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use std::io::{self, IsTerminal, Write};
use zeroize::Zeroizing;

/// Whether stdin/stdout can participate in interactive setup.
pub fn interactive() -> bool {
    io::stdin().is_terminal()
        && io::stdout().is_terminal()
        && !crate::term::Environment::capture().is_dumb()
}

/// Reads one line on OS input events, without a readiness timer or polling.
/// Raw mode is restored on every return, including cancellation.
pub fn read(label: &str, secret: bool) -> io::Result<Zeroizing<String>> {
    // The same native lifecycle as the conversation view owns raw mode,
    // signal restoration and Windows VT admission during setup.
    let _terminal = crate::term::Tty::open()?;
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
                    value.push(ch);
                    if !secret {
                        write!(output, "{ch}")?;
                    }
                }
                _ => {}
            },
            Event::Paste(text) => {
                let text = Zeroizing::new(text);
                value.push_str(&text);
                if !secret {
                    write!(output, "{}", text.as_str())?;
                }
            }
            _ => {}
        }
        output.flush()?;
    }
}

/// Numeric choices keep provider/model names out of the input parser.
pub fn choose(label: &str, choices: &[String]) -> io::Result<usize> {
    for (index, choice) in choices.iter().enumerate() {
        println!("{}. {choice}", index + 1);
    }
    loop {
        let input = read(label, false)?;
        if let Ok(number) = input.trim().parse::<usize>()
            && let Some(index) = number.checked_sub(1).filter(|index| *index < choices.len())
        {
            return Ok(index);
        }
        println!("Choose a listed number.");
    }
}
