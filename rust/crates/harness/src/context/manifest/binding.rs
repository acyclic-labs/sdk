use super::*;
use crate::{executor::stock_boundary, wire::ErrorCode};

#[cfg(feature = "filesystem")]
impl crate::filesystem::PreparationRecord for ContextManifest {
    fn json_value_schema() -> &'static crate::executor::json::JsonValueSchema {
        crate::executor::json::JsonValueSchema::ContextManifest
    }
}

impl ContextManifest {
    pub(crate) async fn bind_observed(
        reference: FileRef,
        reader: &dyn ContentResidencyVerifier,
        mut tools: ToolRegistry,
        limits: Limits,
        io: Option<&PreparationIo>,
    ) -> Result<(ToolRegistry, Arc<dyn ContextStage>)> {
        crate::executor::with_history_io(io, |work| limits.validate_file(&reference, work))?;
        if reference.descriptor().media_type() != "application/json"
            || reference.descriptor().byte_length() > limits.render_bytes
        {
            return Err(stock_boundary(
                io,
                ErrorCode::Invalid,
                "manifest exceeds JSON/render bounds",
            ));
        }
        #[cfg(feature = "filesystem")]
        let content_io = io;
        #[cfg(not(feature = "filesystem"))]
        let content_io = None;
        let bytes = reader
            .read(&reference, limits.render_bytes, content_io)?
            .await?;
        crate::executor::with_history_io(io, |work| reference.descriptor().verify(&bytes, work))?;
        #[cfg(feature = "filesystem")]
        let manifest: Self = match io {
            Some(io) => crate::filesystem::retain_decoded(io.decode(
                std::marker::PhantomData,
                &bytes,
                ErrorCode::Invalid,
            )?)?,
            None => Self::decode(&bytes, limits)?,
        };
        #[cfg(not(feature = "filesystem"))]
        let manifest = Self::decode(&bytes, limits)?;
        if io.is_some() {
            manifest.validate_observed(limits, io)?;
        }
        drop(bytes);
        tools.pin_model_definitions(&manifest.tools, io)?;
        let mut messages = Vec::new();
        crate::executor::with_history_io(io, |work| {
            compaction::reserve_vector(
                &mut messages,
                manifest.conventions.len()
                    + manifest.discovery.instructions.len()
                    + usize::from(!manifest.discovery.skills.is_empty())
                    + manifest.selected_skills.len(),
                work,
            )
        })?;
        for text in manifest.conventions {
            messages.push(ModelMessage {
                role: ModelRole::System,
                content: ModelContent::Text(text),
            });
        }
        let mut discovered = manifest.discovery.messages(limits, io)?;
        for (message, file) in discovered.iter_mut().zip(&manifest.discovery.instructions) {
            let bytes = reader
                .read(file, limits.file_bytes.min(limits.render_bytes), content_io)?
                .await?;
            crate::executor::with_history_io(io, |work| file.descriptor().verify(&bytes, work))?;
            *message = instruction_message(file.display_name(), &bytes, limits.render_bytes, io)?;
        }
        append_context_messages(&mut messages, &mut discovered, io)?;
        let mut remaining = limits
            .render_bytes
            .checked_sub(crate::executor::with_history_io(io, |work| {
                crate::executor::json::json_byte_length(&messages, limits.render_bytes, work)
            })?)
            .ok_or_else(|| {
                stock_boundary(
                    io,
                    ErrorCode::Invalid,
                    "manifest exceeds context byte bound",
                )
            })?;
        for name in manifest.selected_skills {
            let skill = manifest
                .discovery
                .skills
                .iter()
                .find(|skill| skill.name == name)
                .ok_or_else(|| {
                    stock_boundary(
                        io,
                        ErrorCode::NotFound,
                        "manifest selected skill disappeared",
                    )
                })?;
            let message = skill
                .invoke_observed(reader, limits.file_bytes.min(remaining), io)
                .await?;
            let size = crate::executor::with_history_io(io, |work| {
                crate::executor::json::json_byte_length(&message, remaining, work)
            })?;
            remaining = remaining.checked_sub(size).ok_or_else(|| {
                stock_boundary(
                    io,
                    ErrorCode::Invalid,
                    "manifest exceeds context byte bound",
                )
            })?;
            messages.push(message);
        }
        let key = crate::executor::stock_string_copy("context.manifest", io)?;
        #[cfg(feature = "filesystem")]
        if let Some(io) = io {
            let bytes = std::mem::size_of::<(String, FileRef)>() as u64;
            io.retain_owned(bytes)?;
            io.allocation_attempt(bytes)?;
            let bytes =
                (std::mem::size_of::<ManifestStage>() + 2 * std::mem::size_of::<usize>()) as u64;
            io.retain_owned(bytes)?;
            io.allocation_attempt(bytes)?;
        }
        let metadata = BTreeMap::from([(key, reference)]);
        for source in &manifest.sources {
            reader
                .verify(source, limits.file_bytes, content_io)?
                .await?;
        }
        let context = Context {
            messages,
            metadata,
            current_input_index: None,
        };
        validate_projected_context(&context, limits, io)?;
        Ok((
            tools,
            Arc::new(ManifestStage {
                context,
                sources: manifest.sources,
            }),
        ))
    }
}

fn instruction_message(
    name: &str,
    bytes: &[u8],
    maximum: u64,
    io: Option<&PreparationIo>,
) -> Result<ModelMessage> {
    let text = std::str::from_utf8(bytes).map_err(|_| {
        stock_boundary(io, ErrorCode::Invalid, "project instructions are not UTF-8")
    })?;
    let size = (name.len() as u64)
        .checked_add(bytes.len() as u64)
        .and_then(|size| {
            size.checked_add("#  instructions\n\n<INSTRUCTIONS>\n\n</INSTRUCTIONS>".len() as u64)
        });
    if size.is_none_or(|size| size > maximum) {
        return Err(stock_boundary(
            io,
            ErrorCode::Invalid,
            "project instructions exceed render bound",
        ));
    }
    let text = crate::executor::with_history_io(io, |work| {
        crate::executor::preparation_format(
            format_args!("# {name} instructions\n\n<INSTRUCTIONS>\n{text}\n</INSTRUCTIONS>"),
            work,
        )
    })?;
    Ok(ModelMessage {
        role: ModelRole::User,
        content: ModelContent::Text(text),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(16))]
        #[test]
        fn project_instruction_framing_preserves_every_byte(
            chars in prop::collection::vec(any::<char>(), 0..128), claude in any::<bool>(),
        ) {
            let body: String = chars.into_iter().collect();
            let name = if claude { "CLAUDE.md" } else { "AGENTS.md" };
            let expected = ModelMessage { role: ModelRole::User, content: ModelContent::Text(
                format!("# {name} instructions\n\n<INSTRUCTIONS>\n{body}\n</INSTRUCTIONS>"),
            ) };
            let ModelContent::Text(ref text) = expected.content else { unreachable!() };
            let exact = text.len() as u64;
            prop_assert_eq!(instruction_message(name, body.as_bytes(), exact, None).unwrap(), expected.clone());
            prop_assert!(instruction_message(name, body.as_bytes(), exact - 1, None).is_err());
            let anonymous = ModelMessage { role: ModelRole::System, content: ModelContent::Text(body) };
            prop_assert_ne!(anonymous, expected, "anonymous system text passed instruction framing");
        }
    }
}
