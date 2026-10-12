use super::*;
use crate::conversation::{ContentHold, FileRef};
use crate::model::ToolResultContent;
use serde_json::Value;

#[derive(Deserialize)]
pub(super) struct ProjectedToolInvocation {
    pub call_id: String,
    pub name: String,
    pub arguments: Value,
}

pub(super) async fn read_invocation(
    resolver: &dyn ContentResidencyVerifier, file: &FileRef, maximum: u64, io: Option<&PreparationIo>,
) -> Result<ProjectedToolInvocation> {
    let (value, hold) = read_value(resolver, file, maximum, io).await?;
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let (call, hold) = convert((value, hold.expect("observed value ownership")), maximum, io)?;
        hold.into_retained();
        return Ok(call);
    }
    let _ = hold;
    serde_json::from_value(value).map_err(|error| Error::Invalid(format!("tool artifact is invalid: {error}")))
}

pub(super) async fn read_result(
    resolver: &dyn ContentResidencyVerifier, file: &FileRef, maximum: u64, io: Option<&PreparationIo>,
) -> Result<ToolResultContent> {
    let (value, hold) = read_value(resolver, file, maximum, io).await?;
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        // The Value conversion normalizes serde_json's private raw markers.
        let normalized = convert::<Value>((value, hold.expect("observed value ownership")), maximum, io)?;
        let (result, hold) = convert(normalized, maximum, io)?;
        hold.into_retained();
        return Ok(result);
    }
    let _ = hold;
    let normalized: Value = serde_json::from_value(value)
        .map_err(|error| Error::Invalid(format!("tool artifact is invalid: {error}")))?;
    serde_json::from_value(normalized).map_err(|error| Error::Invalid(error.to_string()))
}

pub(super) async fn read_value<'a>(
    resolver: &dyn ContentResidencyVerifier, file: &FileRef, maximum: u64, io: Option<&'a PreparationIo>,
) -> Result<(Value, Option<ContentHold<'a>>)> {
    #[cfg(feature = "filesystem")]
    if let Some(io) = io { io.check_cancellation()?; }
    if file.descriptor().media_type() != "application/json" || file.descriptor().byte_length() > maximum {
        return Err(stock_boundary(io, Invalid, "tool artifact type or rendering limit is invalid"));
    }
    #[cfg(feature = "filesystem")]
    let _bytes_hold;
    let bytes = resolver.read(file, maximum, io)?.await?;
    #[cfg(feature = "filesystem")]
    { _bytes_hold = match io {
        Some(io) if io.original_native_work().is_none() => bytes.retained_capacity()
            .map(|capacity| io.reserve_decode(capacity as u64)).transpose()?,
        _ => None,
    }; }
    if bytes.len() as u64 > maximum {
        return Err(stock_boundary(io, Invalid, "resolved tool artifact exceeds rendering limit"));
    }
    with_history_io(io, |work| file.descriptor().verify(&bytes, work))?;
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let (value, hold) = io.decode(std::marker::PhantomData::<Value>, &bytes, Invalid)?;
        validate_numbers(&value, Some(io))?;
        return Ok((value, Some(hold)));
    }
    let value = crate::contract::json_from_slice(&bytes)
        .map_err(|error| Error::Invalid(format!("tool artifact is invalid: {error}")))?;
    validate_numbers(&value, None)?;
    Ok((value, None))
}

fn validate_numbers(value: &Value, io: Option<&PreparationIo>) -> Result<()> {
    crate::model::charge_items(io, 1)?;
    match value {
        Value::Array(values) => values.iter().try_for_each(|value| validate_numbers(value, io)),
        Value::Object(values) => values.values().try_for_each(|value| validate_numbers(value, io)),
        Value::Number(number) if !crate::conversation::is_exact_js_integer(number)
            || number.as_f64().is_some_and(|value| !crate::conversation::is_exact_js_number(value)) =>
                Err(stock_boundary(io, Invalid, "tool artifact contains an inexact number")),
        _ => Ok(()),
    }
}

#[cfg(feature = "filesystem")]
impl crate::filesystem::UploadRecord for ProjectedToolInvocation {
    fn json_value_schema() -> crate::contract::JsonValueSchema { crate::contract::JsonValueSchema::ProjectedToolInvocation }
    fn logical_owned_bytes(&self) -> Result<u64> {
        let arguments = crate::contract::json_value_logical_owned_bytes(&self.arguments)?;
        (self.call_id.capacity() as u64).checked_add(self.name.capacity() as u64)
            .and_then(|bytes| bytes.checked_add(arguments)).ok_or_else(|| {
                #[cfg(not(target_arch = "wasm32"))]
                { Error::NativeBoundary { code: Invalid, reason: "projected tool invocation ownership overflows" } }
                #[cfg(target_arch = "wasm32")]
                { Error::Invalid("projected tool invocation ownership overflows".into()) }
            })
    }
}

#[cfg(feature = "filesystem")]
fn conversion_allowance<T: crate::filesystem::UploadRecord>(shape: crate::contract::JsonValueShape, io: &PreparationIo) -> Result<u64> {
    use serde::__private229::de::Content;
    use std::mem::size_of;
    let overflow = || stock_boundary(Some(io), Invalid, "tool projection conversion allowance overflows");
    let nodes = shape.lexical_items().checked_add(shape.virtual_items()).ok_or_else(overflow)?;
    let slots = nodes.checked_mul(10).ok_or_else(overflow)?;
    let (content, typed, boxes, strings) = if T::json_value_schema() == crate::contract::JsonValueSchema::ToolResultContent {
        let content_slot = size_of::<Content<'static>>().max(size_of::<(Content<'static>, Content<'static>)>()) as u64;
        let typed_slot = [size_of::<crate::model::ModelDataPart>(), size_of::<FileRef>(), size_of::<String>(),
            size_of::<u8>(), size_of::<(String, Value)>()].into_iter().max().expect("closed projection slots") as u64;
        (slots.checked_mul(content_slot).and_then(|bytes| bytes.checked_mul(3)).ok_or_else(overflow)?,
         slots.checked_mul(typed_slot).ok_or_else(overflow)?,
         nodes.checked_mul(size_of::<crate::model::NativeMediaPolicy>() as u64).ok_or_else(overflow)?,
         shape.input_bytes().checked_mul(3).ok_or_else(overflow)?)
    } else { (0, 0, 0, 0) };
    let labels = [
        "serde_json::Value", "a valid JSON value", "a string containing valid JSON", "ProjectedToolInvocation",
        "call_id", "name", "arguments", "ToolResultContent", "json", "value", "parts", "ModelDataPart", "kind",
        "text", "file", "policy", "reference", "bounded_full", "native", "NativeMediaPolicy", "intent",
        "maximum_bytes", "maximum_work", "configuration", "NativeMediaIntent", "image", "detail", "auto", "low",
        "high", "audio", "maximum_duration_ms", "video", "maximum_frames", "document", "maximum_pages",
        "NativeConfigurationBinding", "source", "configuration", "implementation_digest", "EventReference",
        "stream", "sequence", "digest", "ExtensionConfiguration", "extension", "schema_digest", "content",
        "ExtensionIdentity", "name", "version", "FileRefWire", "volume", "path", "version", "descriptor",
        "display_name", "FileDescriptorWire", "sha256", "byte_length", "media_type", "VolumeRefWire", "provider",
        "id", "class", "owner", "project", "agent_private", "session_shared", "VolumeOwner", "project", "agent",
        "session", "ProviderRefWire", "namespace", "family", "version", "ResourceRefWire", "provider", "kind",
        "key", "version", "AgentId", "SHA-256 digest must contain exactly 32 bytes", "provider identity is invalid",
        "resource key is empty", "resource version is invalid", "typed resource kind mismatch",
        "file or volume label is invalid", "content path is invalid", "file media type is invalid",
    ].iter().map(|label| label.len() as u64 + 4).sum();
    let diagnostics = crate::contract::audited_serde_diagnostic_bytes(shape.input_bytes(), labels)?;
    [content, typed, boxes, strings, crate::contract::json_owned_value_conversion_additional_bytes(shape)?, diagnostics, size_of::<T>() as u64]
        .into_iter().try_fold(0_u64, |bytes, next| bytes.checked_add(next).ok_or_else(overflow))
}

#[cfg(feature = "filesystem")]
fn convert<'a, T: crate::filesystem::UploadRecord>(
    source: (Value, crate::filesystem::DecodeReservation<'a>), maximum_bytes: u64, io: &'a PreparationIo,
) -> Result<(T, crate::filesystem::DecodeReservation<'a>)> {
    let mut source_hold = Some(source.1);
    #[cfg(not(target_arch = "wasm32"))]
    let mut native = None;
    let input = source.0;
    let shape = crate::contract::json_owned_value_shape_bounded(&input,
        crate::contract::JsonBounds { maximum_bytes, maximum_items: io.remaining()?.items_examined, maximum_depth: 128 },
        T::json_value_schema(), &mut |charge| match charge {
            crate::contract::JsonCodecCharge::Inspect { items } => crate::model::charge_items(Some(io), items),
            _ => Err(stock_boundary(Some(io), Invalid, "owned projection inspection reported nonresident work")),
        })?;
    let mut additional = Some(io.reserve_decode(conversion_allowance::<T>(shape, io)?)?);
    let total = source_hold.as_ref().expect("source ownership").bytes()
        .checked_add(additional.as_ref().expect("conversion ownership").bytes()).ok_or_else(||
            stock_boundary(Some(io), Invalid, "owned tool conversion allowance overflows"))?;
    #[cfg(not(target_arch = "wasm32"))]
    if io.original_native_work().is_some() {
        source_hold.take().expect("source ownership").into_retained();
        additional.take().expect("conversion ownership").into_retained();
        native = Some(io.take_native_custody(total)?);
    }
    let value: T = match serde_json::from_value(input) {
        Ok(value) => value,
        Err(error) => {
            #[cfg(not(target_arch = "wasm32"))]
            if native.is_some() { return Err(Error::NativeJson { code: Invalid, source: error, custody: native }); }
            let message = error.to_string();
            additional.take().expect("ordinary conversion ownership").into_retained();
            return Err(Error::Invalid(message));
        }
    };
    let retained = value.logical_owned_bytes()?;
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(mut custody) = native.take() {
        if let Err(cause) = custody.substitute(retained) {
            drop(value);
            return Err(Error::NativeWork { cause, custody: Some(custody) });
        }
        io.absorb_native_custody(custody)?;
        return Ok((value, io.guard_owned(retained)?));
    }
    let released = total.checked_sub(retained).ok_or_else(||
        stock_boundary(Some(io), Invalid, "owned tool conversion exceeds admission"))?;
    source_hold.take().expect("ordinary source ownership").into_retained();
    additional.take().expect("ordinary conversion ownership").into_retained();
    io.release(released)?;
    Ok((value, io.guard_owned(retained)?))
}
