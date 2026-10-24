use super::*;
use crate::conversation::{ContentHold, FileRef};
use crate::model::{ToolCall, ToolResultContent};
use serde_json::Value;

pub(crate) async fn read_tool_call(
    resolver: &dyn ContentResidencyVerifier,
    file: &FileRef,
    maximum: u64,
    io: Option<&PreparationIo>,
) -> Result<ToolCall> {
    let (mut value, hold) = read_value(resolver, file, maximum, io).await?;
    #[cfg(feature = "filesystem")]
    let source_hold = hold;
    #[cfg(not(feature = "filesystem"))]
    let _ = hold;
    crate::model::charge_items(io, 5)?;
    let object = value
        .as_object_mut()
        .ok_or_else(|| stock_boundary(io, Invalid, "tool invocation must be an object"))?;
    let metadata = object.remove("metadata").ok_or_else(|| {
        stock_boundary(io, Invalid, "recorded invocation lacks provider metadata")
    })?;
    let mut invocation = object
        .remove("invocation")
        .ok_or_else(|| stock_boundary(io, Invalid, "recorded invocation lacks its authority"))?;
    let object = invocation
        .as_object_mut()
        .ok_or_else(|| stock_boundary(io, Invalid, "tool invocation must be an object"))?;
    let Some(Value::String(call_id)) = object.remove("call_id") else {
        return Err(stock_boundary(
            io,
            Invalid,
            "tool invocation lacks its call identity",
        ));
    };
    let Some(Value::String(name)) = object.remove("name") else {
        return Err(stock_boundary(
            io,
            Invalid,
            "tool invocation lacks its tool name",
        ));
    };
    let arguments = object
        .remove("arguments")
        .ok_or_else(|| stock_boundary(io, Invalid, "tool invocation lacks its arguments"))?;
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let strings = (call_id.capacity() as u64)
            .checked_add(name.capacity() as u64)
            .ok_or_else(|| {
                stock_boundary(Some(io), Invalid, "tool identity ownership overflows")
            })?;
        let argument_bytes = crate::executor::json::json_value_logical_owned_bytes(&arguments)?;
        let metadata_bytes = crate::executor::json::json_value_logical_owned_bytes(&metadata)?;
        let mut source = source_hold.ok_or_else(|| {
            stock_boundary(
                Some(io),
                crate::wire::ErrorCode::Storage,
                "observed tool value lacks ownership",
            )
        })?;
        let retained = strings
            .checked_add(argument_bytes)
            .and_then(|bytes| bytes.checked_add(metadata_bytes))
            .ok_or_else(|| {
                stock_boundary(Some(io), Invalid, "tool fields exceed source ownership")
            })?;
        drop((value, invocation));
        // These fields are already decoded Values validated by read_value. Keep
        // their original guard; a second serde conversion only interprets markers.
        source.substitute(retained)?;
        source.into_retained()?;
    }
    Ok(ToolCall::new(call_id, name, arguments).with_metadata(metadata))
}

pub(super) async fn read_reasoning(
    resolver: &dyn ContentResidencyVerifier,
    file: &FileRef,
    maximum: u64,
    io: Option<&PreparationIo>,
) -> Result<crate::model::ReasoningBlock> {
    let (value, hold) = read_value(resolver, file, maximum, io).await?;
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let (event, hold) = convert::<crate::model::ModelEvent>(
            (
                value,
                hold.ok_or_else(|| {
                    stock_boundary(Some(io), Invalid, "reasoning lacks source custody")
                })?,
            ),
            maximum,
            128,
            io,
        )?;
        let crate::model::ModelEvent::ReasoningBlock { block } = event else {
            return Err(stock_boundary(
                Some(io),
                Invalid,
                "artifact is not canonical reasoning",
            ));
        };
        let mut hold = hold;
        hold.substitute(block.payload_bytes()?)?;
        hold.into_retained()?;
        return Ok(block);
    }
    let _ = hold;
    let event: crate::model::ModelEvent = serde_json::from_value(value)
        .map_err(|_| stock_boundary(io, Invalid, "invalid reasoning artifact"))?;
    match event {
        crate::model::ModelEvent::ReasoningBlock { block } => Ok(block),
        _ => Err(stock_boundary(
            io,
            Invalid,
            "artifact is not canonical reasoning",
        )),
    }
}

pub(super) async fn read_result(
    resolver: &dyn ContentResidencyVerifier,
    file: &FileRef,
    maximum: u64,
    io: Option<&PreparationIo>,
) -> Result<ToolResultContent> {
    let (value, hold) = read_value(resolver, file, maximum, io).await?;
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let (result, hold) = convert(
            (
                value,
                hold.ok_or_else(|| {
                    stock_boundary(
                        Some(io),
                        crate::wire::ErrorCode::Storage,
                        "observed tool value lacks ownership",
                    )
                })?,
            ),
            maximum,
            128,
            io,
        )?;
        hold.into_retained()?;
        return Ok(result);
    }
    let _ = hold;
    serde_json::from_value(value).map_err(|error| Error::Invalid(error.to_string()))
}

pub(crate) async fn read_value<'a>(
    resolver: &dyn ContentResidencyVerifier,
    file: &FileRef,
    maximum: u64,
    io: Option<&'a PreparationIo>,
) -> Result<(Value, Option<ContentHold<'a>>)> {
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        io.check_cancellation()?;
    }
    if file.descriptor().media_type() != "application/json"
        || file.descriptor().byte_length() > maximum
    {
        return Err(stock_boundary(
            io,
            Invalid,
            "tool artifact type or rendering limit is invalid",
        ));
    }
    #[cfg(feature = "filesystem")]
    let _bytes_hold;
    let bytes = resolver.read(file, maximum, None, io)?.await?;
    #[cfg(feature = "filesystem")]
    {
        _bytes_hold = match io {
            Some(io) if io.original_native_work().is_none() => bytes
                .retained_capacity()
                .map(|capacity| io.reserve_decode(capacity as u64))
                .transpose()?,
            _ => None,
        };
    }
    if bytes.len() as u64 > maximum {
        return Err(stock_boundary(
            io,
            Invalid,
            "resolved tool artifact exceeds rendering limit",
        ));
    }
    with_history_io(io, |work| file.descriptor().verify(&bytes, work))?;
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let (value, hold) = io.decode(std::marker::PhantomData::<Value>, &bytes, Invalid)?;
        with_history_io(Some(io), |work| {
            crate::conversation::validate_json_value(&value, work)
        })?;
        return Ok((value, Some(hold)));
    }
    let value = crate::contract::json_from_slice(&bytes)
        .map_err(|error| Error::Invalid(format!("tool artifact is invalid: {error}")))?;
    crate::conversation::validate_json_value(&value, &mut None)?;
    Ok((value, None))
}

/// The conversion's typed layout cost, its serde diagnostics and the record.
#[cfg(feature = "filesystem")]
fn conversion_allowance<T: crate::filesystem::UploadRecord>(
    shape: crate::executor::json::JsonValueShape,
    layout: crate::executor::json::LayoutCost,
    io: &PreparationIo,
) -> Result<u64> {
    let overflow = || {
        stock_boundary(
            Some(io),
            Invalid,
            "tool projection conversion allowance overflows",
        )
    };
    // Closed serde label widths, including diagnostic punctuation.
    let diagnostics = crate::executor::json::audited_serde_diagnostic_bytes(
        shape.diagnostic_source_bytes(),
        1378,
    )?;
    [layout.bytes(), diagnostics, std::mem::size_of::<T>() as u64]
        .into_iter()
        .try_fold(0_u64, |bytes, next| {
            bytes.checked_add(next).ok_or_else(overflow)
        })
}

#[cfg(feature = "filesystem")]
fn convert<'a, T: crate::filesystem::UploadRecord>(
    source: (Value, crate::filesystem::DecodeReservation<'a>),
    maximum_bytes: u64,
    maximum_depth: u32,
    io: &'a PreparationIo,
) -> Result<(T, crate::filesystem::DecodeReservation<'a>)> {
    let (input, mut hold) = source;
    let (shape, layout) = crate::executor::json::admit_value_layout(
        &input,
        crate::executor::json::JsonBounds {
            maximum_bytes,
            maximum_items: io.remaining()?.items_examined,
            maximum_depth,
        },
        T::json_value_schema(),
        T::LAYOUT,
        Some(Invalid),
        &mut |charge| match charge {
            crate::executor::json::JsonCodecCharge::Inspect { items } => {
                crate::model::charge_items(Some(io), items)
            }
            _ => Err(stock_boundary(
                Some(io),
                Invalid,
                "owned projection inspection reported nonresident work",
            )),
        },
    )?;
    hold.absorb(io.reserve_decode(conversion_allowance::<T>(shape, layout, io)?)?)?;
    let value: T = match serde_json::from_value(input) {
        Ok(value) => value,
        Err(source) => {
            #[cfg(not(target_arch = "wasm32"))]
            if io.original_native_work().is_some() {
                let (source, custody) = crate::filesystem::native_decoded((source, hold))?;
                return Err(Error::NativeJson {
                    code: Invalid,
                    source,
                    custody: Some(custody),
                });
            }
            let message = source.to_string();
            hold.substitute(message.capacity() as u64)?;
            hold.into_retained()?;
            return Err(Error::Invalid(message));
        }
    };
    hold.substitute(value.logical_owned_bytes()?)?;
    Ok((value, hold))
}
