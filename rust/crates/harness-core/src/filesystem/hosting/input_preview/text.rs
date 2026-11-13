//! Bounded text projection of the original, typed user-input JSON envelope.
//! This is a presentation walk, not a second admission or schema validator.
use super::Error;

#[derive(Clone, Copy)]
enum Role {
    Key,
    Text,
    Other,
}

struct Frame {
    object: bool,
    part: bool,
    key: bool,
    text: bool,
}

struct Quoted {
    role: Role,
    raw: Option<String>,
    escape: bool,
    digits: u8,
    unit: u16,
    high: bool,
}

/// Retains at most one encoded scalar beyond the source chunk and the JSON
/// nesting stack. File-reference strings are skipped, never dereferenced.
pub(super) struct Text {
    frames: Vec<Frame>,
    quoted: Option<Quoted>,
    chunk: usize,
    ended: bool,
}

impl Text {
    pub fn new(chunk: usize) -> Self {
        Self {
            frames: Vec::new(),
            quoted: None,
            chunk,
            ended: false,
        }
    }

    pub fn feed(&mut self, source: &str) -> Result<String, Error> {
        let mut output = String::new();
        for character in source.chars() {
            if let Some(quoted) = &mut self.quoted {
                if let Some(raw) = &mut quoted.raw {
                    raw.push(character);
                    // A JSON spelling of the four ASCII characters "text"
                    // needs at most 24 bytes, plus its two quotes. Longer keys
                    // cannot select text, however long the original key is.
                    if matches!(quoted.role, Role::Key) && raw.len() > 26 {
                        quoted.raw = None;
                    }
                }
                if quoted.digits != 0 {
                    let digit = character
                        .to_digit(16)
                        .ok_or(Error::Record("invalid input JSON escape"))?;
                    quoted.unit = (quoted.unit << 4) | digit as u16;
                    quoted.digits -= 1;
                    if quoted.digits == 0 {
                        if quoted.high {
                            if !(0xdc00..=0xdfff).contains(&quoted.unit) {
                                return Err(Error::Record("invalid input JSON surrogate"));
                            }
                            quoted.high = false;
                        } else if (0xd800..=0xdbff).contains(&quoted.unit) {
                            quoted.high = true;
                        } else if (0xdc00..=0xdfff).contains(&quoted.unit) {
                            return Err(Error::Record("invalid input JSON surrogate"));
                        }
                    }
                } else if quoted.escape {
                    quoted.escape = false;
                    if character == 'u' {
                        quoted.digits = 4;
                        quoted.unit = 0;
                    } else if quoted.high
                        || !matches!(character, '"' | '\\' | '/' | 'b' | 'f' | 'n' | 'r' | 't')
                    {
                        return Err(Error::Record("invalid input JSON escape"));
                    }
                } else if character == '\\' {
                    quoted.escape = true;
                } else if quoted.high {
                    return Err(Error::Record("invalid input JSON surrogate"));
                } else if character == '"' {
                    let quoted = self.quoted.take().expect("open JSON string");
                    match quoted.role {
                        Role::Key => {
                            if let Some(raw) = quoted.raw {
                                let key: String = serde_json::from_str(&raw)
                                    .map_err(|_| Error::Record("invalid input JSON key"))?;
                                self.frames.last_mut().expect("key object").text = key == "text";
                            }
                        }
                        Role::Text => output.push_str(&decode(quoted.raw.expect("text chunk"))?),
                        Role::Other => {}
                    }
                    if self.frames.is_empty() {
                        self.ended = true;
                    }
                    continue;
                } else if character < ' ' {
                    return Err(Error::Record("invalid input JSON string"));
                }
                if matches!(quoted.role, Role::Text)
                    && !quoted.escape
                    && quoted.digits == 0
                    && !quoted.high
                    && quoted
                        .raw
                        .as_ref()
                        .is_some_and(|raw| raw.len() >= self.chunk)
                {
                    let mut raw = quoted.raw.take().expect("text chunk");
                    raw.push('"');
                    output.push_str(&decode(raw)?);
                    quoted.raw = Some("\"".into());
                }
                continue;
            }
            if character.is_ascii_whitespace() {
                continue;
            }
            if self.ended {
                return Err(Error::Record("input JSON has trailing data"));
            }
            match character {
                '{' | '[' => {
                    let part = character == '{'
                        && (self.frames.is_empty()
                            || (self.frames.len() == 1 && !self.frames[0].object));
                    self.frames.push(Frame {
                        object: character == '{',
                        part,
                        key: character == '{',
                        text: false,
                    });
                }
                '}' | ']' => {
                    let frame = self
                        .frames
                        .pop()
                        .ok_or(Error::Record("input JSON is unbalanced"))?;
                    if frame.object != (character == '}') {
                        return Err(Error::Record("input JSON is unbalanced"));
                    }
                    if self.frames.is_empty() {
                        self.ended = true;
                    }
                }
                ':' => {
                    let frame = self
                        .frames
                        .last_mut()
                        .ok_or(Error::Record("input JSON has no object"))?;
                    frame.key = false;
                }
                ',' => {
                    let frame = self
                        .frames
                        .last_mut()
                        .ok_or(Error::Record("input JSON has no container"))?;
                    frame.key = frame.object;
                }
                '"' => {
                    let role = match self.frames.last_mut() {
                        None => Role::Text,
                        Some(frame) if frame.object && frame.key => {
                            frame.text = false;
                            if frame.part { Role::Key } else { Role::Other }
                        }
                        Some(frame) if frame.part && frame.text => Role::Text,
                        _ => Role::Other,
                    };
                    self.quoted = Some(Quoted {
                        role,
                        raw: (!matches!(role, Role::Other)).then(|| "\"".into()),
                        escape: false,
                        digits: 0,
                        unit: 0,
                        high: false,
                    });
                }
                _ if self.frames.is_empty() => {
                    return Err(Error::Record("input JSON has no human content"));
                }
                _ => {}
            }
        }
        Ok(output)
    }

    pub fn finish(self) -> Result<(), Error> {
        if self.quoted.is_some() || !self.frames.is_empty() || !self.ended {
            return Err(Error::Record("input JSON ended early"));
        }
        Ok(())
    }
}

fn decode(raw: String) -> Result<String, Error> {
    serde_json::from_str(&raw).map_err(|_| Error::Record("invalid input JSON text"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn text_projection_matches_typed_content_at_every_chunk_width(parts in prop::collection::vec(".{0,32}", 0..5)) {
            let contents = [
                crate::model::ModelContent::Parts(parts.iter().cloned().map(crate::model::ModelContentPart::text).collect()),
                crate::model::ModelContent::Part(crate::model::ModelContentPart::text(parts.concat())),
                crate::model::ModelContent::Text(parts.concat()),
            ];
            for content in contents {
              let encoded = serde_json::to_string(&content).expect("typed user content");
              for width in 1..=17 {
                let mut source = Text::new(width);
                let mut output = String::new();
                let mut chunk = String::new();
                for character in encoded.chars() {
                    chunk.push(character);
                    if chunk.len() >= width {
                        output.push_str(&source.feed(&chunk).expect("valid chunk"));
                        chunk.clear();
                    }
                }
                output.push_str(&source.feed(&chunk).expect("valid tail"));
                source.finish().expect("complete source");
                prop_assert_eq!(output, parts.concat());
              }
            }
        }
    }

    #[test]
    fn escaped_keys_and_surrogate_pairs_cross_source_chunks() -> Result<(), Error> {
        let mut source = Text::new(4);
        let mut output = String::new();
        for character in r#"[{"\u0074ext":"\uD83D\uDE42\n\"\\","kind":"text"}]"#.chars() {
            output.push_str(&source.feed(&character.to_string())?);
        }
        source.finish()?;
        assert_eq!(output, "🙂\n\"\\");
        Ok(())
    }
}
