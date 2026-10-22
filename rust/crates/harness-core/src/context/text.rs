//! Values inside context tags cannot author tags or extra catalog lines.

/// One scalar's framed representation, using bounded stack storage only.
pub(crate) fn escaped_character(character: char, buffer: &mut arrayvec::ArrayString<12>) -> &str {
    buffer.clear();
    match character {
        '&' => "&amp;",
        '<' => "&lt;",
        '>' => "&gt;",
        character if character.is_control() || matches!(character, '\u{2028}' | '\u{2029}') => {
            // The largest scalar is six hex digits: at most ten ASCII bytes.
            buffer.push_str("&#x");
            let code = character as u32;
            let width = (32 - code.leading_zeros()).max(1).div_ceil(4);
            for shift in (0..width).rev() {
                let digit = ((code >> (shift * 4)) & 15) as u8;
                buffer.push(char::from(if digit < 10 {
                    b'0' + digit
                } else {
                    b'A' + digit - 10
                }));
            }
            buffer.push(';');
            buffer.as_str()
        }
        character => {
            buffer.push(character);
            buffer.as_str()
        }
    }
}

#[cfg(test)]
#[doc(hidden)]
fn push_value(text: &mut String, value: &str) {
    let mut buffer = arrayvec::ArrayString::new();
    for character in value.chars() {
        text.push_str(escaped_character(character, &mut buffer));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// Unicode line terminators (UAX #14 BK/CR/LF/NL), listed independently.
    const LINE_BREAKS: [char; 7] = [
        '\n', '\u{B}', '\u{C}', '\r', '\u{85}', '\u{2028}', '\u{2029}',
    ];

    #[test]
    fn values_cannot_start_a_line() {
        for separator in LINE_BREAKS {
            let mut encoded = String::new();
            push_value(&mut encoded, &format!("a{separator}- forged: x"));
            assert!(!encoded.contains(LINE_BREAKS), "{encoded:?}");
        }
    }

    proptest! {
        // Property: all Unicode scalar values survive framing exactly, while
        // no value can author a tag or physical control character.
        #[test]
        fn escaped_values_round_trip(characters in prop::collection::vec(any::<char>(), 0..100)) {
            let value: String = characters.into_iter().collect();
            let mut encoded = String::new();
            push_value(&mut encoded, &value);
            prop_assert!(!encoded.contains(['<', '>']));
            prop_assert!(!encoded.chars().any(char::is_control));
            prop_assert!(!encoded.contains(LINE_BREAKS));
            let mut decoded = String::new();
            let mut rest = encoded.as_str();
            while let Some((plain, entity_text)) = rest.split_once('&') {
                decoded.push_str(plain);
                rest = entity_text;
                let (entity, suffix) = rest.split_once(';').unwrap();
                decoded.push(match entity {
                    "amp" => '&', "lt" => '<', "gt" => '>',
                    code => char::from_u32(u32::from_str_radix(code.strip_prefix("#x").unwrap(), 16).unwrap()).unwrap(),
                });
                rest = suffix;
            }
            decoded.push_str(rest);
            prop_assert_eq!(decoded, value);
        }
    }
}
