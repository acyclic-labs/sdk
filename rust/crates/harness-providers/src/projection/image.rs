use crate::Adapter;
use acyclic_harness::model::{ImageDetail, NativeMediaIntent, NativeMediaPolicy};
use serde::{Serialize, Serializer};

/// Raw image bytes remain borrowed until the final HTTP buffer is written.
pub(super) struct Image<'a> {
    pub adapter: Adapter,
    pub mime: &'a str,
    pub policy: &'a NativeMediaPolicy,
    pub bytes: &'a [u8],
}
struct Data<'a> {
    mime: Option<&'a str>,
    bytes: &'a [u8],
}
impl std::fmt::Display for Data<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(mime) = self.mime {
            write!(f, "data:{mime};base64,")?;
        }
        std::fmt::Display::fmt(
            &base64::display::Base64Display::new(
                self.bytes,
                &base64::engine::general_purpose::STANDARD,
            ),
            f,
        )
    }
}
impl Serialize for Data<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}
impl Serialize for Image<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Source<'a> {
            r#type: &'static str,
            media_type: &'a str,
            data: Data<'a>,
        }
        #[derive(Serialize)]
        struct Anthropic<'a> {
            r#type: &'static str,
            source: Source<'a>,
        }
        #[derive(Serialize)]
        struct Inline<'a> {
            #[serde(rename = "mimeType")]
            mime_type: &'a str,
            data: Data<'a>,
        }
        #[derive(Serialize)]
        struct Gemini<'a> {
            #[serde(rename = "inlineData")]
            inline_data: Inline<'a>,
        }
        #[derive(Serialize)]
        struct Url<'a> {
            url: Data<'a>,
            detail: &'static str,
        }
        #[derive(Serialize)]
        struct Chat<'a> {
            r#type: &'static str,
            image_url: Url<'a>,
        }
        #[derive(Serialize)]
        struct Responses<'a> {
            r#type: &'static str,
            image_url: Data<'a>,
            detail: &'static str,
        }
        let detail = match self.policy.intent {
            NativeMediaIntent::Image {
                detail: ImageDetail::Low,
            } => "low",
            NativeMediaIntent::Image {
                detail: ImageDetail::High,
            } => "high",
            _ => "auto",
        };
        let data = Data {
            mime: matches!(self.adapter, Adapter::Chat | Adapter::Responses).then_some(self.mime),
            bytes: self.bytes,
        };
        match self.adapter {
            Adapter::Anthropic => Anthropic {
                r#type: "image",
                source: Source {
                    r#type: "base64",
                    media_type: self.mime,
                    data,
                },
            }
            .serialize(s),
            Adapter::Gemini => Gemini {
                inline_data: Inline {
                    mime_type: self.mime,
                    data,
                },
            }
            .serialize(s),
            Adapter::Chat => Chat {
                r#type: "image_url",
                image_url: Url { url: data, detail },
            }
            .serialize(s),
            Adapter::Responses => Responses {
                r#type: "input_image",
                image_url: data,
                detail,
            }
            .serialize(s),
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::super::wire::{Body, Item, Message};
    use super::*;
    #[test]
    fn image_projection_allocates_only_the_final_wire_buffer() {
        use base64::Engine as _;
        for adapter in [
            Adapter::Chat,
            Adapter::Responses,
            Adapter::Anthropic,
            Adapter::Gemini,
        ] {
            for length in [64, 65536, 1048576] {
                let raw = vec![251u8; length];
                let policy = NativeMediaPolicy {
                    intent: NativeMediaIntent::Image {
                        detail: ImageDetail::High,
                    },
                    maximum_bytes: length as u64,
                    maximum_work: 16384,
                    configuration: None,
                };
                let mut projected = None;
                let construction = acyclic_test_support::measure(|| {
                    projected = Some(Body {
                        fields: serde_json::Map::new(),
                        messages_field: "input",
                        messages: vec![Message::Item(Item::Image(Image {
                            adapter,
                            mime: "image/png",
                            policy: &policy,
                            bytes: &raw,
                        }))],
                    });
                });
                assert!(
                    construction.bytes_total < 1024,
                    "projection copied image bytes"
                );
                let projected = projected.unwrap();
                let capacity = projected.capacity().unwrap();
                let mut output = Vec::with_capacity(capacity);
                let encoding = acyclic_test_support::measure(|| {
                    serde_json::to_writer(&mut output, &projected).unwrap()
                });
                assert_eq!(
                    encoding.count_total, 0,
                    "encoding allocated intermediate image storage"
                );
                assert_eq!(output.capacity(), capacity, "wire buffer grew");
                let expected = base64::engine::general_purpose::STANDARD.encode(&raw);
                assert!(std::str::from_utf8(&output).unwrap().contains(&expected));
                assert!(capacity <= expected.len() + 512);
                // Negative control: the removed base64 String path is visible to this oracle.
                let mut copied = None;
                let copying = acyclic_test_support::measure(|| {
                    copied = Some(base64::engine::general_purpose::STANDARD.encode(&raw))
                });
                assert!(copying.bytes_total >= expected.len() as u64);
                assert_eq!(copied.as_deref(), Some(expected.as_str()));
            }
        }
    }
}
