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
            .read(&reference, limits.render_bytes, None, content_io)?
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
