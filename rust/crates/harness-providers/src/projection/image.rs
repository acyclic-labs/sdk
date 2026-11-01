use crate::{Adapter, invalid};
use acyclic_harness_core::{
    Result,
    model::{NativeMediaIntent, NativeMediaPolicy},
};
use serde_json::{Value, json};

pub(crate) fn part(
    adapter: Adapter,
    media: &str,
    policy: &NativeMediaPolicy,
    data: String,
) -> Result<Value> {
    if matches!(policy.intent, NativeMediaIntent::Document { .. }) {
        if media != "application/pdf" {
            return Err(invalid("native document must be PDF"));
        }
        return Ok(match adapter {
            Adapter::Responses => {
                json!({"type":"message","role":"user","content":[{"type":"input_file","filename":"document.pdf","file_data":format!("data:application/pdf;base64,{data}")}]})
            }
            Adapter::Chat => {
                json!({"type":"file","file":{"filename":"document.pdf","file_data":format!("data:application/pdf;base64,{data}")}})
            }
            Adapter::Anthropic => {
                json!({"type":"document","source":{"type":"base64","media_type":"application/pdf","data":data}})
            }
            Adapter::Gemini => json!({"inlineData":{"mimeType":"application/pdf","data":data}}),
        });
    }
    let NativeMediaIntent::Image { detail } = policy.intent else {
        return Err(invalid("native file is not an image or PDF"));
    };
    let detail = serde_json::to_value(detail).map_err(|_| invalid("invalid image detail"))?;
    Ok(match adapter {
        Adapter::Responses => json!({"type":"message","role":"user","content":[{
            "type":"input_image","image_url":format!("data:{media};base64,{data}"),"detail":detail}]}),
        Adapter::Chat => json!({"type":"image_url","image_url":{
            "url":format!("data:{media};base64,{data}"),"detail":detail}}),
        Adapter::Anthropic => {
            json!({"type":"image","source":{"type":"base64","media_type":media,"data":data}})
        }
        Adapter::Gemini => json!({"inlineData":{"mimeType":media,"data":data}}),
    })
}

#[cfg(all(not(target_arch = "wasm32"), any(test, feature = "fuzzing")))]
#[path = "image_checks.rs"]
mod checks;

#[cfg(all(not(target_arch = "wasm32"), any(test, feature = "fuzzing")))]
pub(crate) fn assert_roundtrip(input: &[u8]) {
    checks::assert_roundtrip(input);
}
