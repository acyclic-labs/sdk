use super::*;

/// A context the one projection admission accepted under `limits`: its
/// metadata, every message and its instruction bodies are within bounds.
/// The fields are private to this module, so a holder can only read it, join
/// it with another admitted part, or extend it by one message admitted alone;
/// no holder walks an admitted message again.
#[derive(Clone, Debug)]
pub struct AdmittedContext {
    context: Context,
    limits: Limits,
}

impl AdmittedContext {
    /// The projection admission every later holder relies on.
    pub fn admit(context: Context, limits: Limits, io: Option<&PreparationIo>) -> Result<Self> {
        validate_projected_context(&context, limits, io)?;
        Ok(Self { context, limits })
    }

    /// Admits a copy of messages one bounds walk already accepted, with no
    /// metadata: only the whole-context counts and instruction budget are checked.
    pub(in crate::context) fn copy_bounded(
        bounded: crate::context::value::BoundedContext<'_>,
        current_input_index: Option<u32>,
        io: Option<&PreparationIo>,
    ) -> Result<Self> {
        let limits = bounded.limits();
        let context = Context {
            messages: bounded.copy_messages(io)?,
            metadata: std::collections::BTreeMap::new(),
            current_input_index,
        };
        validate_extended_context(&context, limits, io)?;
        Ok(Self { context, limits })
    }

    /// This context admitted under `limits`, walking it only when they differ
    /// from the limits it was admitted under.
    pub fn under(self, limits: Limits, io: Option<&PreparationIo>) -> Result<Self> {
        if self.limits == limits {
            return Ok(self);
        }
        Self::admit(self.context, limits, io)
    }

    /// Borrows the context after proving it within `limits`, walking it only
    /// when they differ from the limits it was admitted under.
    pub fn within(&self, limits: Limits, io: Option<&PreparationIo>) -> Result<&Context> {
        if self.limits != limits {
            validate_projected_context(&self.context, limits, io)?;
        }
        Ok(&self.context)
    }

    /// The admitted context.
    #[must_use]
    pub const fn context(&self) -> &Context {
        &self.context
    }

    /// Releases the context; a changed copy must be admitted again.
    #[must_use]
    pub fn into_context(self) -> Context {
        self.context
    }

    /// Marks the active input historical; removing a marker keeps every bound.
    #[must_use]
    pub(crate) fn into_history(mut self) -> Self {
        self.context.current_input_index = None;
        self
    }

    /// Appends one message: only that message and the whole-context counts
    /// and instruction budget are checked, never the admitted messages.
    pub(crate) fn push(self, message: ModelMessage, io: Option<&PreparationIo>) -> Result<Self> {
        self.extend(std::iter::once(message), io)
    }

    /// Appends messages: each is bounded alone, then the whole-context counts
    /// and instruction budget are checked once, never the admitted messages.
    pub(crate) fn extend(
        mut self,
        messages: impl IntoIterator<Item = ModelMessage>,
        io: Option<&PreparationIo>,
    ) -> Result<Self> {
        for message in messages {
            crate::executor::with_history_io(io, |work| {
                message.content.validate_limits(self.limits, work)
            })?;
            crate::executor::stock_vec_push(&mut self.context.messages, message, io)?;
        }
        validate_extended_context(&self.context, self.limits, io)?;
        Ok(self)
    }

    /// An owned copy under the same admission: copying changes no bound.
    pub(crate) fn copy(&self, io: Option<&PreparationIo>) -> Result<Self> {
        Ok(Self {
            context: copy_context(&self.context, io)?,
            limits: self.limits,
        })
    }

    /// A copy of admitted messages `range`: each is already bounded, so only
    /// the whole-context counts and instruction budget are checked.
    pub(crate) fn copy_range(
        &self,
        range: std::ops::Range<usize>,
        io: Option<&PreparationIo>,
    ) -> Result<Self> {
        let context = crate::executor::with_history_io(io, |work| {
            crate::context::clone_context_range(&self.context, range, work)
                .map(|(context, _)| context)
        })?;
        validate_extended_context(&context, self.limits, io)?;
        Ok(Self {
            context,
            limits: self.limits,
        })
    }
}

/// Two admitted contexts are equal when their contexts are: the limits only
/// record which admission proved them.
impl PartialEq for AdmittedContext {
    fn eq(&self, other: &Self) -> bool {
        self.context == other.context
    }
}

/// Reads see the admitted context; there is no mutable access.
impl std::ops::Deref for AdmittedContext {
    type Target = Context;
    fn deref(&self) -> &Context {
        &self.context
    }
}

/// An admitted context serializes as the context it admits.
impl Serialize for AdmittedContext {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        self.context.serialize(serializer)
    }
}

/// Validates and places renderer output; shared by native stages and narrow WASM bindings.
pub fn apply_context_projection(
    context: Context,
    messages: Vec<ModelMessage>,
    mode: ContextRenderMode,
    placement: ContextPlacement,
    limits: Limits,
    io: Option<&PreparationIo>,
) -> Result<Context> {
    project_messages(
        AdmittedContext::admit(context, limits, io)?,
        messages,
        mode,
        placement,
        limits,
        io,
    )
    .map(AdmittedContext::into_context)
}

/// Admits renderer output alone and places it beside an admitted context.
pub(in crate::context) fn project_messages(
    context: AdmittedContext,
    messages: Vec<ModelMessage>,
    mode: ContextRenderMode,
    placement: ContextPlacement,
    limits: Limits,
    io: Option<&PreparationIo>,
) -> Result<AdmittedContext> {
    let placement = if mode == ContextRenderMode::Update {
        ContextPlacement::Append
    } else {
        placement
    };
    let contribution = AdmittedContext::admit(
        Context {
            messages,
            ..Context::default()
        },
        limits,
        io,
    )?;
    place_context(context, contribution, placement, limits, io)
}

pub(in crate::context) fn place_context(
    context: AdmittedContext,
    contribution: AdmittedContext,
    placement: ContextPlacement,
    limits: Limits,
    io: Option<&PreparationIo>,
) -> Result<AdmittedContext> {
    let context = context.under(limits, io)?.context;
    let contribution = contribution.under(limits, io)?.context;
    let marker = preflight_context_composition(&context, &contribution, placement, limits, io)?;
    compose_context(context, contribution, placement, marker, limits, io)
}

pub(in crate::context) fn place_snapshot(
    context: AdmittedContext,
    snapshot: &AdmittedContext,
    placement: ContextPlacement,
    limits: Limits,
    io: Option<&PreparationIo>,
) -> Result<AdmittedContext> {
    let context = context.under(limits, io)?.context;
    let marker = preflight_context_composition(
        &context,
        snapshot.within(limits, io)?,
        placement,
        limits,
        io,
    )?;
    let snapshot = copy_context(&snapshot.context, io)?;
    compose_context(context, snapshot, placement, marker, limits, io)
}

/// Both parts are admitted under `limits`; only their joint bounds remain.
fn preflight_context_composition(
    context: &Context,
    contribution: &Context,
    placement: ContextPlacement,
    limits: Limits,
    io: Option<&PreparationIo>,
) -> Result<Option<u32>> {
    if context
        .messages
        .len()
        .checked_add(contribution.messages.len())
        .is_none_or(|count| {
            limits
                .context_messages
                .is_some_and(|maximum| count > maximum.get())
        })
        || context
            .metadata
            .len()
            .checked_add(contribution.metadata.len())
            .is_none_or(|count| {
                limits
                    .attachments
                    .is_some_and(|maximum| count > maximum.get())
            })
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
    let mut remaining = limits.render_bytes;
    crate::model::reserve_instruction_bodies(first, &mut remaining, io)?;
    crate::model::reserve_instruction_bodies(second, &mut remaining, io)?;
    Ok(current_input_index)
}

fn compose_context(
    mut context: Context,
    mut contribution: Context,
    placement: ContextPlacement,
    marker: Option<u32>,
    limits: Limits,
    io: Option<&PreparationIo>,
) -> Result<AdmittedContext> {
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
    // Deduplication rewrites admitted messages, so the result is admitted again.
    if crate::model::deduplicate_reads(&mut context.messages, io)? {
        return AdmittedContext::admit(context, limits, io);
    }
    Ok(AdmittedContext { context, limits })
}
