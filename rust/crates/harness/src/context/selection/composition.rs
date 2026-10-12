use super::*;

/// Validates and places renderer output; shared by native stages and narrow WASM bindings.
pub fn apply_context_projection(
    context: Context,
    messages: Vec<ModelMessage>,
    mode: ContextRenderMode,
    placement: ContextPlacement,
    limits: Limits,
    io: Option<&PreparationIo>,
) -> Result<Context> {
    let placement = if mode == ContextRenderMode::Update {
        ContextPlacement::Append
    } else {
        placement
    };
    place_context(
        context,
        Context {
            messages,
            ..Context::default()
        },
        placement,
        limits,
        io,
    )
}

pub(in crate::context) fn place_context(
    context: Context,
    contribution: Context,
    placement: ContextPlacement,
    limits: Limits,
    io: Option<&PreparationIo>,
) -> Result<Context> {
    let marker = preflight_context_composition(&context, &contribution, placement, limits, io)?;
    compose_context(context, contribution, placement, marker, io)
}

pub(in crate::context) fn place_snapshot(
    context: Context,
    snapshot: &Context,
    placement: ContextPlacement,
    limits: Limits,
    io: Option<&PreparationIo>,
) -> Result<Context> {
    let marker = preflight_context_composition(&context, snapshot, placement, limits, io)?;
    let snapshot = copy_context(snapshot, io)?;
    compose_context(context, snapshot, placement, marker, io)
}

struct JoinedMetadata<'a>(
    &'a std::collections::BTreeMap<String, FileRef>,
    &'a std::collections::BTreeMap<String, FileRef>,
);

impl Serialize for JoinedMetadata<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap as _;
        let mut map = serializer.serialize_map(None)?;
        for (name, file) in self.0.iter().chain(self.1) {
            map.serialize_entry(name, file)?;
        }
        map.end()
    }
}

fn preflight_context_composition(
    context: &Context,
    contribution: &Context,
    placement: ContextPlacement,
    limits: Limits,
    io: Option<&PreparationIo>,
) -> Result<Option<u32>> {
    validate_projected_context(context, limits, io)?;
    validate_projected_context(contribution, limits, io)?;
    if context
        .messages
        .len()
        .checked_add(contribution.messages.len())
        .is_none_or(|count| count > limits.context_messages)
        || context
            .metadata
            .len()
            .checked_add(contribution.metadata.len())
            .is_none_or(|count| count > limits.attachments)
    {
        return Err(crate::executor::stock_boundary(
            io,
            crate::wire::ErrorCode::Invalid,
            "context composition exceeds declared count bounds",
        ));
    }
    if contribution
        .metadata
        .keys()
        .any(|name| context.metadata.contains_key(name))
    {
        return Err(crate::executor::stock_boundary(
            io,
            crate::wire::ErrorCode::Conflict,
            "context composition repeats a metadata key",
        ));
    }
    let current_input_index = match (
        context.current_input_index,
        contribution.current_input_index,
    ) {
        (Some(_), Some(_)) => {
            return Err(crate::executor::stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "context composition has two active inputs",
            ));
        }
        (Some(index), None) if placement == ContextPlacement::Prepend => Some(
            u32::try_from(contribution.messages.len())
                .ok()
                .and_then(|count| index.checked_add(count))
                .ok_or_else(|| {
                    crate::executor::stock_boundary(
                        io,
                        crate::wire::ErrorCode::Invalid,
                        "current input index exceeds portable count",
                    )
                })?,
        ),
        (None, Some(index)) if placement == ContextPlacement::Append => Some(
            u32::try_from(context.messages.len())
                .ok()
                .and_then(|count| index.checked_add(count))
                .ok_or_else(|| {
                    crate::executor::stock_boundary(
                        io,
                        crate::wire::ErrorCode::Invalid,
                        "current input index exceeds portable count",
                    )
                })?,
        ),
        (Some(index), None) | (None, Some(index)) => Some(index),
        (None, None) => None,
    };
    let (first, second) = match placement {
        ContextPlacement::Prepend => (
            contribution.messages.as_slice(),
            context.messages.as_slice(),
        ),
        ContextPlacement::Append => (
            context.messages.as_slice(),
            contribution.messages.as_slice(),
        ),
    };
    #[derive(Serialize)]
    struct JoinedContext<'a> {
        messages: ContextMessages<'a>,
        metadata: JoinedMetadata<'a>,
        current_input_index: Option<u32>,
    }
    crate::executor::with_history_io(io, |work| {
        crate::contract::json_byte_length(&JoinedContext {
                messages: ContextMessages {
                    first,
                    user: None,
                    second,
                },
                metadata: JoinedMetadata(&context.metadata, &contribution.metadata),
                current_input_index,
            }, limits.render_bytes, work).map(|_| ())
    })?;
    Ok(current_input_index)
}

fn compose_context(
    mut context: Context,
    mut contribution: Context,
    placement: ContextPlacement,
    marker: Option<u32>,
    io: Option<&PreparationIo>,
) -> Result<Context> {
    context.current_input_index = marker;
    context.metadata.extend(contribution.metadata);
    match placement {
        ContextPlacement::Prepend => {
            append_context_messages(&mut contribution.messages, &mut context.messages, io)?;
            context.messages = contribution.messages;
        }
        ContextPlacement::Append => {
            append_context_messages(&mut context.messages, &mut contribution.messages, io)?;
        }
    }
    Ok(context)
}
