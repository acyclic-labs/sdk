//! Borrowed inspection: strings and replacement arguments stay in capture bytes
//! until the outcome ledger grants a reservation. Unknown values are skipped.
use super::*;
use crate::ProcessError;
use serde::{
    Serialize,
    de::{DeserializeSeed, MapAccess, Visitor},
};
use serde_json::value::RawValue;
use std::fmt;

pub(super) enum Text<'a> {
    Json(&'a RawValue),
    Plain(&'a str),
    Owned(String),
    Stderr(&'a [u8]),
    Exit(Option<i32>, &'a [u8]),
}

impl Text<'_> {
    pub(super) fn into_string(
        self,
        _reservation: &super::budget::Reservation<'_>,
    ) -> Result<String> {
        Ok(match self {
            Self::Json(raw) => serde_json::from_str(raw.get())
                .map_err(|_| Error::Invalid("invalid hook string".into()))?,
            Self::Plain(text) => text.into(),
            Self::Owned(text) => text,
            Self::Stderr(bytes) => String::from_utf8_lossy(bytes).into_owned(),
            Self::Exit(code, bytes) => format!("exit {code:?}: {}", String::from_utf8_lossy(bytes)),
        })
    }
}

impl Serialize for Text<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        match self {
            Self::Json(raw) => raw.serialize(serializer),
            Self::Plain(text) => text.serialize(serializer),
            Self::Owned(text) => text.serialize(serializer),
            // Count the escaped, lossy UTF-8 text through the serializer's
            // streaming formatter, without constructing a string.
            Self::Stderr(_) | Self::Exit(_, _) => serializer.collect_str(self),
        }
    }
}

impl fmt::Display for Text<'_> {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut remaining = match self {
            Self::Stderr(bytes) => *bytes,
            Self::Exit(code, bytes) => {
                write!(out, "exit {code:?}: ")?;
                *bytes
            }
            _ => return Err(fmt::Error),
        };
        loop {
            match std::str::from_utf8(remaining) {
                Ok(text) => return out.write_str(text),
                Err(error) => {
                    out.write_str(
                        std::str::from_utf8(&remaining[..error.valid_up_to()])
                            .map_err(|_| fmt::Error)?,
                    )?;
                    out.write_str("\u{fffd}")?;
                    remaining = &remaining[error.valid_up_to()
                        + error
                            .error_len()
                            .unwrap_or(remaining.len() - error.valid_up_to())..];
                }
            }
        }
    }
}

pub(super) struct Plan<'a> {
    pub(super) blocked: Option<Text<'a>>,
    pub(super) approval: Option<Text<'a>>,
    pub(super) context: [Option<Text<'a>>; 2],
    pub(super) input: Option<(&'a RawValue, u64)>,
    pub(super) failure: Option<(ProcessError<Text<'a>>, bool)>,
}

impl<'a> Plan<'a> {
    fn empty() -> Self {
        Self {
            blocked: None,
            approval: None,
            context: [None, None],
            input: None,
            failure: None,
        }
    }

    pub(super) fn failed(error: Error, blocking: bool) -> Self {
        let cause = match error {
            Error::Process(cause) => cause.map_message(Text::Owned),
            Error::Invalid(message) => ProcessError::InvalidOutput {
                exit_code: None,
                message: Text::Owned(message),
            },
            error => ProcessError::Io {
                exit_code: None,
                message: Text::Owned(error.to_string()),
            },
        };
        Self {
            failure: Some((cause, blocking)),
            ..Self::empty()
        }
    }

    pub(super) fn inspect(
        output: &HookOutput,
        event: HookEvent,
        code: Option<i32>,
        stdout: &'a [u8],
        stderr: &'a [u8],
    ) -> std::result::Result<Self, &'static str> {
        let mut plan = Self::empty();
        if code == Some(2) {
            if matches!(event, HookEvent::PreToolUse | HookEvent::Stop) {
                if output.blocked.is_none() {
                    plan.blocked = Some(Text::Stderr(stderr));
                }
            } else if event == HookEvent::PostToolUse {
                plan.context[0] = Some(Text::Stderr(stderr));
            } else {
                plan.failure = Some((
                    ProcessError::Exit {
                        exit_code: code,
                        message: Text::Exit(code, stderr),
                    },
                    false,
                ));
            }
            return Ok(plan);
        }
        let text = std::str::from_utf8(stdout)
            .map_err(|_| "hook stdout is not UTF-8")?
            .trim();
        if text.is_empty() {
            return Ok(plan);
        }
        let root = match fields(
            text,
            [
                "continue",
                "stopReason",
                "decision",
                "reason",
                "hookSpecificOutput",
            ],
        ) {
            Ok(fields) => fields,
            Err(_)
                if !text.starts_with('{')
                    && (!text.starts_with('[') || event == HookEvent::SessionStart) =>
            {
                // A valid non-object JSON value remains invalid structured output.
                if serde_json::from_str::<&RawValue>(text).is_ok() {
                    return Err("hook structured output must be an object");
                }
                if event == HookEvent::SessionStart {
                    plan.context[0] = Some(Text::Plain(text));
                }
                return Ok(plan);
            }
            Err(_) => return Err("hook output JSON is invalid"),
        };
        for index in [1, 2, 3] {
            string(root[index])?;
        }
        let continues = root[0]
            .map(|raw| match raw.get() {
                "true" => Ok(true),
                "false" => Ok(false),
                _ => Err("hook continue must be a boolean"),
            })
            .transpose()?;
        let decision = choice(root[2], &["approve", "block"])?;
        let specific = root[4]
            .map(|raw| {
                fields(
                    raw.get(),
                    [
                        "hookEventName",
                        "permissionDecision",
                        "permissionDecisionReason",
                        "additionalContext",
                        "updatedInput",
                    ],
                )
            })
            .transpose()
            .map_err(|_| "hookSpecificOutput must be an object")?
            .unwrap_or([None; 5]);
        for raw in &specific[..4] {
            string(*raw)?;
        }
        if specific[0].is_some() && choice(specific[0], &[event.name()])? != Some(0) {
            return Err("hook output names another event");
        }
        let permission = choice(specific[1], &["allow", "ask", "deny"])?;
        let input = specific[4]
            .map(|raw| {
                if !raw.get().starts_with('{') {
                    return Err("hook updatedInput must be an object");
                }
                canonical_size(raw)
                    .map(|size| (raw, size))
                    .ok_or("invalid hook updatedInput")
            })
            .transpose()?;
        if output.blocked.is_none() {
            if continues == Some(false) {
                plan.blocked = Some(
                    root[1]
                        .map(Text::Json)
                        .unwrap_or(Text::Plain("Stopped by hook")),
                );
            } else if matches!(event, HookEvent::PreToolUse | HookEvent::Stop)
                && (decision == Some(1) || permission == Some(2))
            {
                plan.blocked = Some(
                    specific[2]
                        .or(root[3])
                        .map(Text::Json)
                        .unwrap_or(Text::Plain("Blocked by hook")),
                );
            }
        }
        if event == HookEvent::PreToolUse && permission == Some(1) && output.approval.is_none() {
            plan.approval = Some(
                specific[2]
                    .map(Text::Json)
                    .unwrap_or(Text::Plain("Approval requested by hook")),
            );
        }
        if event == HookEvent::PostToolUse && decision == Some(1) {
            plan.context[0] = root[3].map(Text::Json);
        }
        plan.context[1] = specific[3].map(Text::Json);
        if event == HookEvent::PreToolUse {
            plan.input = input;
        }
        Ok(plan)
    }

    pub(super) fn invalid(message: &'static str, blocking: bool, code: Option<i32>) -> Self {
        Self {
            failure: Some((
                ProcessError::InvalidOutput {
                    exit_code: code,
                    message: Text::Plain(message),
                },
                blocking,
            )),
            ..Self::empty()
        }
    }

    pub(super) fn exit(code: Option<i32>, stderr: &'a [u8], blocking: bool) -> Self {
        Self {
            failure: Some((
                ProcessError::Exit {
                    exit_code: code,
                    message: Text::Exit(code, stderr),
                },
                blocking,
            )),
            ..Self::empty()
        }
    }
}

fn string(raw: Option<&RawValue>) -> std::result::Result<(), &'static str> {
    if raw.is_some_and(|raw| !raw.get().starts_with('"') || !valid_string(raw.get())) {
        return Err("hook output field must be a string");
    }
    Ok(())
}

fn choice(
    raw: Option<&RawValue>,
    names: &[&str],
) -> std::result::Result<Option<usize>, &'static str> {
    struct Choice<'a>(&'a [&'a str]);
    impl<'de> Visitor<'de> for Choice<'_> {
        type Value = usize;
        fn expecting(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
            out.write_str("a recognized hook decision")
        }
        fn visit_str<E: serde::de::Error>(self, value: &str) -> std::result::Result<usize, E> {
            self.0
                .iter()
                .position(|name| *name == value)
                .ok_or_else(|| E::custom("unknown hook decision or another event"))
        }
    }
    raw.map(|raw| {
        serde::Deserializer::deserialize_str(
            &mut serde_json::Deserializer::from_str(raw.get()),
            Choice(names),
        )
        .map_err(|_| "unknown hook decision or another event")
    })
    .transpose()
}

// A fixed field array avoids allocating object maps, unknown strings or values.
// DeserializeSeed retains raw spans; escaped JSON strings are decoded only by
// the reservation's commit. Serde's temporary key scratch is capture-bounded.
fn fields<'de, const N: usize>(
    text: &'de str,
    names: [&str; N],
) -> std::result::Result<[Option<&'de RawValue>; N], serde_json::Error> {
    struct Fields<'a, const N: usize>([&'a str; N]);
    struct Key<'a, const N: usize>([&'a str; N]);
    impl<const N: usize> Visitor<'_> for Key<'_, N> {
        type Value = Option<usize>;
        fn expecting(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
            out.write_str("an object field")
        }
        fn visit_str<E: serde::de::Error>(
            self,
            value: &str,
        ) -> std::result::Result<Self::Value, E> {
            Ok(self.0.iter().position(|name| *name == value))
        }
    }
    impl<'de, const N: usize> DeserializeSeed<'de> for Key<'_, N> {
        type Value = Option<usize>;
        fn deserialize<D: serde::Deserializer<'de>>(
            self,
            deserializer: D,
        ) -> std::result::Result<Self::Value, D::Error> {
            deserializer.deserialize_identifier(self)
        }
    }
    impl<'de, const N: usize> Visitor<'de> for Fields<'_, N> {
        type Value = [Option<&'de RawValue>; N];
        fn expecting(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
            out.write_str("a hook output object")
        }
        fn visit_map<A: MapAccess<'de>>(
            self,
            mut map: A,
        ) -> std::result::Result<Self::Value, A::Error> {
            let mut fields = [None; N];
            while let Some(index) = map.next_key_seed(Key(self.0))? {
                if let Some(index) = index {
                    fields[index] = Some(map.next_value()?);
                } else {
                    map.next_value::<serde::de::IgnoredAny>()?;
                }
            }
            Ok(fields)
        }
    }
    let mut decoder = serde_json::Deserializer::from_str(text);
    let fields = serde::Deserializer::deserialize_map(&mut decoder, Fields(names))?;
    decoder.end()?;
    Ok(fields)
}

// Raw spelling is not a size bound: 1e6 becomes 1000000.0. Inspect canonical
// scalar sizes and container punctuation without owning maps/lists/strings.
// Duplicate object keys overcount safely; serde's parser bounds recursion.
fn canonical_size(raw: &RawValue) -> Option<u64> {
    struct Size(u64);
    impl Size {
        fn scalar<E: serde::de::Error>(value: &impl Serialize) -> std::result::Result<Self, E> {
            super::budget::encoded_size(value, u64::MAX)
                .map(Self)
                .map_err(|_| E::custom("invalid hook value"))
        }
        fn add<E: serde::de::Error>(&mut self, bytes: u64) -> std::result::Result<(), E> {
            self.0 = self
                .0
                .checked_add(bytes)
                .ok_or_else(|| E::custom("hook value size overflow"))?;
            Ok(())
        }
    }
    impl<'de> serde::Deserialize<'de> for Size {
        fn deserialize<D: serde::Deserializer<'de>>(
            decoder: D,
        ) -> std::result::Result<Self, D::Error> {
            decoder.deserialize_any(Self(0))
        }
    }
    impl<'de> Visitor<'de> for Size {
        type Value = Self;
        fn expecting(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
            out.write_str("a JSON value")
        }
        fn visit_unit<E>(self) -> std::result::Result<Self, E> {
            Ok(Self(4))
        }
        fn visit_bool<E>(self, value: bool) -> std::result::Result<Self, E> {
            Ok(Self(if value { 4 } else { 5 }))
        }
        fn visit_i64<E: serde::de::Error>(self, value: i64) -> std::result::Result<Self, E> {
            Self::scalar(&value)
        }
        fn visit_u64<E: serde::de::Error>(self, value: u64) -> std::result::Result<Self, E> {
            Self::scalar(&value)
        }
        fn visit_f64<E: serde::de::Error>(self, value: f64) -> std::result::Result<Self, E> {
            Self::scalar(&value)
        }
        fn visit_str<E: serde::de::Error>(self, value: &str) -> std::result::Result<Self, E> {
            Self::scalar(&value)
        }
        fn visit_seq<A: serde::de::SeqAccess<'de>>(
            self,
            mut seq: A,
        ) -> std::result::Result<Self, A::Error> {
            let mut size = Self(2);
            let mut comma = 0;
            while let Some(item) = seq.next_element::<Size>()? {
                size.add(item.0 + comma)?;
                comma = 1;
            }
            Ok(size)
        }
        fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> std::result::Result<Self, A::Error> {
            let mut size = Self(2);
            let mut comma = 0;
            while let Some((key, value)) = map.next_entry::<Size, Size>()? {
                size.add(key.0 + value.0 + 1 + comma)?;
                comma = 1;
            }
            Ok(size)
        }
    }
    serde_json::from_str::<Size>(raw.get())
        .ok()
        .map(|size| size.0)
}

// RawValue validates JSON escape syntax, but permits unpaired Unicode
// surrogates. Validate those pairs without allocating a decoded string.
fn valid_string(text: &str) -> bool {
    let mut bytes = text.as_bytes().iter().copied();
    while let Some(byte) = bytes.next() {
        if byte != b'\\' {
            continue;
        }
        if bytes.next() != Some(b'u') {
            continue;
        }
        let unit = |bytes: &mut std::iter::Copied<std::slice::Iter<'_, u8>>| -> Option<u16> {
            let mut value = 0;
            for _ in 0..4 {
                value = value * 16 + char::from(bytes.next()?).to_digit(16)? as u16;
            }
            Some(value)
        };
        match unit(&mut bytes) {
            Some(0xd800..=0xdbff) => {
                if bytes.next() != Some(b'\\')
                    || bytes.next() != Some(b'u')
                    || !matches!(unit(&mut bytes), Some(0xdc00..=0xdfff))
                {
                    return false;
                }
            }
            Some(0xdc00..=0xdfff) | None => return false,
            Some(_) => {}
        }
    }
    true
}
