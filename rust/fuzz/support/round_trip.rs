//! Complete-result oracles shared by the SDK fuzz targets.

use std::fmt::Debug;

/// Check the complete decoder result; a failed decode always fails the oracle.
///
/// # Panics
/// Panics on a decoder error or any change to the original value.
#[track_caller]
pub fn round_trip<T: Debug + PartialEq, E: Debug>(expected: &T, decoded: Result<T, E>) -> T {
    let actual = match decoded {
        Ok(actual) => actual,
        Err(error) => ::std::unreachable!("encoded fuzz value failed to decode: {error:?}"),
    };
    ::std::assert_eq!(&actual, expected, "fuzz round trip changed the value");
    actual
}

/// Require a successful decode when the expected value is checked separately.
///
/// # Panics
/// Panics on any decode error.
#[track_caller]
pub fn must_decode<T, E: Debug>(decoded: Result<T, E>) -> T {
    match decoded {
        Ok(value) => value,
        Err(error) => ::std::unreachable!("fuzz oracle expected successful decoding: {error:?}"),
    }
}

/// Require the complete result to reject an independently invalid input.
///
/// # Panics
/// Panics when invalid input is accepted.
#[track_caller]
pub fn must_reject<T: Debug, E>(decoded: Result<T, E>) -> E {
    match decoded {
        Err(error) => error,
        Ok(value) => ::std::unreachable!("fuzz oracle admitted invalid input: {value:?}"),
    }
}

/// Check independently predicted acceptance before exposing a successful value.
///
/// # Panics
/// Panics when acceptance differs from the independent prediction.
#[track_caller]
pub fn accepts<T, E>(expected: bool, decoded: Result<T, E>) -> Option<T> {
    ::std::assert_eq!(decoded.is_ok(), expected, "fuzz oracle acceptance mismatch");
    decoded.ok()
}

/// Compare complete results, including exact expected errors.
///
/// # Errors
/// Returns the expected error after checking it matches exactly.
///
/// # Panics
/// Panics on any result mismatch.
#[track_caller]
pub fn same_result<T: Debug + PartialEq, E: Debug + PartialEq>(
    expected: &Result<T, E>,
    decoded: Result<T, E>,
) -> Result<T, E> {
    ::std::assert_eq!(&decoded, expected, "fuzz oracle result mismatch");
    decoded
}

/// A read-only view of the original fuzzer input, including subslices.
#[derive(Clone, Copy, Debug)]
#[must_use]
pub struct FuzzInput<'a> {
    bytes: &'a [u8],
}

impl<'a> FuzzInput<'a> {
    /// Borrow the original input bytes.
    pub fn bytes(self) -> &'a [u8] {
        self.bytes
    }

    /// Borrow a checked subrange of the original input.
    pub fn slice(self, range: std::ops::Range<usize>) -> Option<Self> {
        self.bytes.get(range).map(|bytes| Self { bytes })
    }

    /// Admit a byte slice only when it belongs to the original allocation.
    pub fn view(self, bytes: &'a [u8]) -> Option<Self> {
        let start = bytes
            .as_ptr()
            .addr()
            .checked_sub(self.bytes.as_ptr().addr())?;
        self.bytes.get(start..start.checked_add(bytes.len())?)?;
        Some(Self { bytes })
    }

    /// Borrow the original input as valid UTF-8.
    pub fn text(self) -> Option<&'a str> {
        std::str::from_utf8(self.bytes).ok()
    }

    /// Interpret arbitrary raw bytes with an optional parser.
    pub fn optional<T>(self, decode: fn(&'a [u8]) -> Option<T>) -> Option<T> {
        decode(self.bytes)
    }

    /// Interpret arbitrary raw subviews without capturing generated buffers.
    pub fn optional_view<T>(self, decode: fn(Self) -> Option<T>) -> Option<T> {
        decode(self)
    }

    /// Interpret valid raw UTF-8 with an optional parser.
    pub fn optional_text<T>(self, decode: fn(&'a str) -> Option<T>) -> Option<T> {
        decode(self.text()?)
    }

    /// Admit a successful parse of arbitrary raw bytes.
    ///
    /// ```
    /// use acyclic_fuzz_support::FuzzInput;
    /// fn admit(input: FuzzInput<'_>) {
    ///     let _ = input.parse(std::str::from_utf8);
    /// }
    /// ```
    ///
    /// Generated buffers cannot be captured by a parser function pointer:
    /// ```compile_fail
    /// use acyclic_fuzz_support::FuzzInput;
    /// fn bypass(input: FuzzInput<'_>, generated: Vec<u8>) {
    ///     let _ = input.parse(|_| std::str::from_utf8(&generated));
    /// }
    /// ```
    pub fn parse<T, E>(self, decode: fn(&'a [u8]) -> Result<T, E>) -> Option<T> {
        decode(self.bytes).ok()
    }

    /// Admit a raw parse with a numeric resource limit.
    ///
    /// ```compile_fail
    /// use acyclic_fuzz_support::FuzzInput;
    /// fn bypass(input: FuzzInput<'_>, generated: Vec<u8>) {
    ///     let _ = input.parse_bounded(&generated, |_, wire| Ok::<_, ()>(wire.len()));
    /// }
    /// ```
    pub fn parse_bounded<T, E>(
        self,
        maximum: u64,
        decode: fn(&'a [u8], u64) -> Result<T, E>,
    ) -> Option<T> {
        decode(self.bytes, maximum).ok()
    }

    /// Admit a successful parse of raw UTF-8.
    pub fn parse_text<T, E>(self, decode: fn(&'a str) -> Result<T, E>) -> Option<T> {
        decode(self.text()?).ok()
    }

    /// Interpret lossy raw UTF-8 without capturing generated buffers.
    pub fn parse_lossy<T, E>(self, decode: fn(&str) -> Result<T, E>) -> Option<T> {
        decode(&String::from_utf8_lossy(self.bytes)).ok()
    }

    /// Interpret raw UTF-8 with a numeric resource limit.
    pub fn parse_text_bounded<T, E>(
        self,
        maximum: u64,
        decode: fn(&'a str, u64) -> Result<T, E>,
    ) -> Option<T> {
        decode(self.text()?, maximum).ok()
    }

    #[doc(hidden)]
    pub fn __acyclic_parsed_input<T>(self, value: T) -> ParsedInput<T> {
        ParsedInput { value }
    }
}

#[doc(hidden)]
pub fn __acyclic_fuzz_entry(bytes: &[u8], run: impl FnOnce(FuzzInput<'_>)) {
    run(FuzzInput { bytes });
}

/// Enter a byte or text fuzz target with a sealed raw-input view.
#[macro_export]
macro_rules! fuzz_target {
    (|$bytes:ident : &[u8], $input:ident| $body:expr) => {
        ::libfuzzer_sys::fuzz_target!(|$bytes: &[u8]| {
            $crate::__acyclic_fuzz_entry($bytes, |$input| $body);
        });
    };
    (|$text:ident : &str, $input:ident| $body:expr) => {
        ::libfuzzer_sys::fuzz_target!(|$text: &str| {
            $crate::__acyclic_fuzz_entry($text.as_bytes(), |$input| $body);
        });
    };
}

/// A decoded untrusted input whose byte fields can themselves be fuzz inputs.
#[derive(Debug)]
#[must_use]
pub struct ParsedInput<T> {
    value: T,
}

impl<T> ParsedInput<T> {
    /// Consume the admitted untrusted input.
    #[must_use]
    pub fn into_value(self) -> T {
        self.value
    }

    /// Borrow a byte field from admitted untrusted input.
    pub fn field(&self, field: fn(&T) -> &[u8]) -> FuzzInput<'_> {
        FuzzInput {
            bytes: field(&self.value),
        }
    }

    /// Interpret arbitrary raw bytes with an optional parser.
    /// Borrow an optional byte field from admitted untrusted input.
    pub fn optional_field(&self, field: fn(&T) -> Option<&[u8]>) -> Option<FuzzInput<'_>> {
        field(&self.value).map(|bytes| FuzzInput { bytes })
    }

    /// Borrow a byte field from admitted untrusted input.
    /// Borrow a byte field with an explicit field selector.
    pub fn field_with(&self, index: usize, field: fn(&T, usize) -> &[u8]) -> FuzzInput<'_> {
        FuzzInput {
            bytes: field(&self.value, index),
        }
    }

    /// Interpret an admitted untrusted value.
    pub fn admit<U, E>(self, decode: fn(T) -> Result<U, E>) -> Option<U> {
        decode(self.value).ok()
    }
}

impl<T> std::ops::Deref for ParsedInput<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.value
    }
}

/// Decode an arbitrary raw protobuf input before checking its round trip.
#[macro_export]
macro_rules! protobuf_input {
    ($input:expr, $type:ty) => {{
        let input: $crate::FuzzInput<'_> = $input;
        input
            .parse(<$type as ::prost::Message>::decode)
            .map(|value| input.__acyclic_parsed_input(value))
    }};
}

/// Decode an arbitrary raw JSON input before checking its round trip.
#[macro_export]
macro_rules! json_input {
    ($input:expr, $type:ty) => {{
        let input: $crate::FuzzInput<'_> = $input;
        input
            .parse(::serde_json::from_slice::<$type>)
            .map(|value| input.__acyclic_parsed_input(value))
    }};
}

#[cfg(feature = "test-input")]
#[doc(hidden)]
pub fn test_input(bytes: &[u8]) -> FuzzInput<'_> {
    FuzzInput { bytes }
}

/// Preserve the complete result of interpreting an already admitted raw input.
///
/// # Errors
/// Returns the interpreter error unchanged.
pub fn input_result<T, U, E>(
    input: &ParsedInput<T>,
    decode: fn(&T) -> Result<U, E>,
) -> Result<U, E> {
    decode(&input.value)
}

/// Decode an arbitrary raw protobuf input before checking its round trip.
/// Decode two arbitrary length-delimited protobuf input messages.
#[macro_export]
macro_rules! protobuf_inputs {
    ($input:expr, $first:ty, $second:ty $(,)?) => {{
        let input: $crate::FuzzInput<'_> = $input;
        input.parse(
            |mut bytes| -> Result<($first, $second), ::prost::DecodeError> {
                Ok((
                    <$first as ::prost::Message>::decode_length_delimited(&mut bytes)?,
                    <$second as ::prost::Message>::decode_length_delimited(&mut bytes)?,
                ))
            },
        )
    }};
}

/// Apply the same strict round-trip contract to codecs returning `Option`.
///
/// # Panics
/// Panics on missing output or a changed value.
#[track_caller]
pub fn round_trip_option<T: Debug + PartialEq>(expected: &T, decoded: Option<T>) -> T {
    round_trip(expected, decoded.ok_or("encoded value was rejected"))
}

/// Preserve a decoded value while comparing its complete canonical representation.
///
/// # Panics
/// Panics on decoder or projection errors, or a changed representation.
#[track_caller]
pub fn round_trip_by<T, E: Debug, K: Debug + PartialEq, P: Debug>(
    expected: &K,
    decoded: Result<T, E>,
    project: impl FnOnce(&T) -> Result<K, P>,
) -> T {
    let actual = must_decode(decoded);
    round_trip(expected, project(&actual));
    actual
}

/// Convert a model's wire row count to the current platform's index.
pub fn index(value: u64) -> Option<usize> {
    usize::try_from(value).ok()
}

/// Check generated streaming frames through the complete-result oracle.
#[macro_export]
macro_rules! stream_round_trip {
    ($input:expr, $decoder:ident, $pending:ident, $before_chunk:expr) => {{
        let input: $crate::FuzzInput<'_> = $input;
        for (index, chunk) in input.bytes().chunks(17).enumerate() {
            ($before_chunk)(index);
            $pending.extend_from_slice(chunk);
            loop {
                match ::tokio_util::codec::Decoder::decode(&mut $decoder, &mut $pending) {
                    Ok(Some(value)) => {
                        let expected = $crate::must_decode(::serde_json::to_value(&value));
                        let mut output = ::bytes::BytesMut::new();
                        match ::tokio_util::codec::Encoder::encode(
                            &mut $decoder,
                            value,
                            &mut output,
                        ) {
                            Ok(()) => {
                                let _ = $crate::round_trip(
                                    &Some(expected),
                                    ::tokio_util::codec::Decoder::decode(
                                        &mut $decoder,
                                        &mut output,
                                    )
                                    .map(|value| {
                                        value.map(|value| {
                                            $crate::must_decode(::serde_json::to_value(value))
                                        })
                                    }),
                                );
                            }
                            Err(_) => ::std::assert!(
                                output.is_empty(),
                                "failed encoding published partial bytes"
                            ),
                        }
                    }
                    Ok(None) => break,
                    Err(_) => {
                        $pending.clear();
                        break;
                    }
                }
            }
        }
        let _ = ::tokio_util::codec::Decoder::decode_eof(&mut $decoder, &mut $pending);
    }};
}
