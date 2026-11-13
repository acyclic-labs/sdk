use super::*;
use crate::{executor::stock_boundary, wire::ErrorCode};

impl crate::filesystem::PreparationRecord for ContextManifest {
    fn json_value_schema() -> &'static crate::executor::json::JsonValueSchema {
        crate::executor::json::JsonValueSchema::ContextManifest
    }
}

impl ContextManifest {
    pub(crate) async fn bind_observed(
        reference: FileRef,
        manifest_reader: &dyn ContentResidencyVerifier,
        reader: &dyn ContentResidencyVerifier,
        publisher: Option<&dyn crate::conversation::ContentPublisher>,
        mut tools: ToolRegistry,
        limits: Limits,
        io: Option<&PreparationIo>,
        pin_tools: bool,
    ) -> Result<(ToolRegistry, Arc<dyn ContextStage>, Vec<ToolDefinition>)> {
        crate::executor::with_history_io(io, |work| limits.validate_record(&reference, work))?;
        if reference.descriptor().media_type() != "application/json"
            || limits
                .render_bytes
                .is_some_and(|maximum| reference.descriptor().byte_length() > maximum)
        {
            return Err(stock_boundary(
                io,
                ErrorCode::Invalid,
                "manifest exceeds JSON/render bounds",
            ));
        }
        let content_io = io;
        let bytes = manifest_reader
            .read(
                &reference,
                reference.descriptor().byte_length(),
                None,
                content_io,
            )?
            .await?;
        crate::executor::with_history_io(io, |work| reference.descriptor().verify(&bytes, work))?;
        let manifest: Self = match io {
            Some(io) => crate::filesystem::retain_decoded(io.decode(
                std::marker::PhantomData,
                &bytes,
                ErrorCode::Invalid,
            )?)?,
            None => Self::decode(&bytes, limits)?,
        };
        if io.is_some() {
            manifest.validate_observed(limits, io)?;
        }
        drop(bytes);
        if pin_tools {
            tools.pin_model_definitions(&manifest.tools, io)?;
        } else {
            tools.pin_epoch_definitions(&manifest.tools, io)?;
        }
        let mut messages = Vec::new();
        crate::executor::with_history_io(io, |work| {
            compaction::reserve_vector(
                &mut messages,
                manifest.conventions.len()
                    + manifest.discovery.instructions.len()
                    + usize::from(!manifest.discovery.skills.is_empty())
                    + manifest.selected_skills.len()
                    + manifest.documents.len(),
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
        for file in manifest.documents {
            messages.push(ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Part(crate::model::ModelContentPart::File {
                    file,
                    policy: crate::model::FileProjectionPolicy::BoundedFull,
                }),
            });
        }
        let mut remaining = limits
            .render_bytes
            .map(|maximum| {
                let size = crate::executor::with_history_io(io, |work| {
                    crate::executor::json::json_byte_length(&messages, Some(maximum), work)
                })?;
                maximum.checked_sub(size).ok_or_else(|| {
                    stock_boundary(
                        io,
                        ErrorCode::Invalid,
                        "manifest exceeds context byte bound",
                    )
                })
            })
            .transpose()?;
        crate::model::reserve_instruction_bodies(&messages, &mut remaining, io)?;
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
                .invoke_observed(
                    reader,
                    publisher.ok_or_else(|| {
                        stock_boundary(
                            io,
                            ErrorCode::Unauthorized,
                            "selected skills require journal publication authority",
                        )
                    })?,
                    manifest_reader,
                    remaining.into_iter().chain(limits.file_bytes).min(),
                    io,
                )
                .await?;
            if let Some(maximum) = &mut remaining {
                let size = crate::executor::with_history_io(io, |work| {
                    crate::executor::json::json_byte_length(&message, Some(*maximum), work)
                })?;
                *maximum = maximum.checked_sub(size).ok_or_else(|| {
                    stock_boundary(
                        io,
                        ErrorCode::Invalid,
                        "manifest exceeds context byte bound",
                    )
                })?;
            }
            messages.push(message);
        }
        let key = crate::executor::stock_string_copy("context.manifest", io)?;
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
                .verify(
                    source,
                    limits
                        .file_bytes
                        .map_or(source.descriptor().byte_length(), |maximum| {
                            maximum.min(source.descriptor().byte_length())
                        }),
                    content_io,
                )?
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
                environment: manifest.environment,
                context,
                sources: manifest.sources,
            }),
            manifest.tools,
        ))
    }
}
