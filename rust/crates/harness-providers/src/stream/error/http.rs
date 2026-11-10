//! A fixed-size projection through the original JSON parser, with funded escape scratch.
use acyclic_harness_core::{NativeWork, Result};
use serde::de::{self, DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor};

/// No remote string or container survives the classification.
struct Refusal(bool);

#[derive(Clone, Copy)]
enum Position {
    Root,
    Response,
    Fields,
    Text,
}

#[derive(Default)]
struct Projection {
    present: bool,
    context: bool,
    invalid_request: bool,
    long_prompt: bool,
    response_error: Option<bool>,
}

impl<'de> DeserializeSeed<'de> for Position {
    type Value = Projection;

    fn deserialize<D: de::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> std::result::Result<Projection, D::Error> {
        deserializer.deserialize_any(self)
    }
}

#[derive(serde::Deserialize)]
#[serde(field_identifier, rename_all = "snake_case")]
enum Field {
    Code,
    #[serde(rename = "type")]
    Kind,
    Message,
    Error,
    Response,
    #[serde(other)]
    Other,
}

impl<'de> Visitor<'de> for Position {
    type Value = Projection;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a provider refusal")
    }

    fn visit_unit<E: de::Error>(self) -> std::result::Result<Projection, E> {
        Ok(Projection::default())
    }

    fn visit_bool<E: de::Error>(self, _: bool) -> std::result::Result<Projection, E> {
        Ok(Projection {
            present: true,
            ..Default::default()
        })
    }

    fn visit_i64<E: de::Error>(self, _: i64) -> std::result::Result<Projection, E> {
        self.visit_bool(false)
    }

    fn visit_u64<E: de::Error>(self, _: u64) -> std::result::Result<Projection, E> {
        self.visit_bool(false)
    }

    fn visit_f64<E: de::Error>(self, _: f64) -> std::result::Result<Projection, E> {
        self.visit_bool(false)
    }

    fn visit_str<E: de::Error>(self, text: &str) -> std::result::Result<Projection, E> {
        Ok(Projection {
            present: true,
            context: matches!(self, Self::Text)
                && super::is_context_overflow(Some(text), None, None),
            invalid_request: text == "invalid_request_error",
            long_prompt: super::is_context_overflow(
                None,
                Some("invalid_request_error"),
                Some(text),
            ),
            response_error: None,
        })
    }

    fn visit_seq<A: SeqAccess<'de>>(
        self,
        mut sequence: A,
    ) -> std::result::Result<Projection, A::Error> {
        while sequence.next_element::<IgnoredAny>()?.is_some() {}
        self.visit_bool(false)
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> std::result::Result<Projection, A::Error> {
        let mut output = Projection {
            present: true,
            ..Default::default()
        };
        let (mut error, mut response_error) = (None, None);
        let mut seen = 0u8;
        let (mut fields_valid, mut envelope_valid) = (true, true);
        while let Some(field) = map.next_key::<Field>()? {
            let selected = match (self, field) {
                (Self::Root | Self::Fields, Field::Code) => Some((1, Self::Text)),
                (Self::Root | Self::Fields, Field::Kind) => Some((2, Self::Text)),
                (Self::Root | Self::Fields, Field::Message) => Some((4, Self::Text)),
                (Self::Root | Self::Response, Field::Error) => Some((8, Self::Fields)),
                (Self::Root, Field::Response) => Some((16, Self::Response)),
                (Self::Response, Field::Response) => Some((16, Self::Text)),
                _ => None,
            };
            let Some((bit, position)) = selected else {
                map.next_value::<IgnoredAny>()?;
                continue;
            };
            if seen & bit != 0 {
                if bit < 8 {
                    fields_valid = false;
                } else {
                    envelope_valid = false;
                }
            }
            let value = map.next_value_seed(position)?;
            // Like serde's Option fields, a null does not occupy the slot.
            if value.present {
                seen |= bit;
            }
            match bit {
                1 => output.context |= value.context,
                2 => {
                    output.context |= value.context;
                    output.invalid_request = value.invalid_request;
                }
                4 => output.long_prompt = value.long_prompt,
                8 => error = value.present.then_some(value.context),
                16 => response_error = value.response_error,
                _ => unreachable!(),
            }
        }
        output.context |= output.invalid_request && output.long_prompt;
        output.context &= fields_valid;
        output.context = match self {
            Self::Root => envelope_valid && error.or(response_error).unwrap_or(output.context),
            Self::Response => false,
            Self::Fields => output.context,
            Self::Text => false,
        };
        if matches!(self, Self::Response) {
            output.response_error = envelope_valid.then_some(error).flatten();
        }
        Ok(output)
    }
}

impl<'de> serde::Deserialize<'de> for Refusal {
    fn deserialize<D: de::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        Position::Root
            .deserialize(deserializer)
            .map(|value| Self(value.context))
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl acyclic_harness_core::HeapLayout for Refusal {
    // All containers are inline projection state or IgnoredAny. The shared
    // layout walker still charges every string and the parser's escape scratch.
    const LAYOUT: &'static acyclic_harness_core::heap_layout::JsonLayout =
        &acyclic_harness_core::heap_layout::JsonLayout::Node {
            seq: Some(acyclic_harness_core::heap_layout::Alloc::INLINE),
            map: Some(acyclic_harness_core::heap_layout::Alloc::INLINE),
            tagged: false,
            fields: &[],
            elements: &[],
            flatten: &[],
        };
}

#[cfg(not(target_arch = "wasm32"))]
impl acyclic_harness_core::filesystem::UploadRecord for Refusal {
    fn logical_owned_bytes(&self) -> Result<u64> {
        Ok(0)
    }
}

pub(crate) fn context_overflow_bytes(bytes: &[u8], work: NativeWork<'_>) -> Result<bool> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(work) = work {
        use acyclic_harness_core::{Error, filesystem::PreparationIo};
        return match PreparationIo::from_native_work(work)?.decode_json::<Refusal>(bytes) {
            Ok(value) => Ok(value.0),
            Err(Error::NativeJson { .. } | Error::NativeJsonAdmission { .. }) => Ok(false),
            Err(error) => Err(error),
        };
    }
    let _ = work;
    Ok(serde_json::from_slice::<Refusal>(bytes).is_ok_and(|value| value.0))
}
