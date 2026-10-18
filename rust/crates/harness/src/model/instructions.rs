//! Project-instruction identity and its single provider projection.
use super::*;
use crate::conversation::ContentIo;

/// Canonical instruction identity, derived from the authenticated source path.
/// Display labels never determine a filename or directory.
#[derive(Clone, Copy)]
pub struct InstructionSource<'a> {
    file: &'a FileRef,
    volume: &'a crate::conversation::VolumeRef,
    path: &'a str,
}

impl<'a> InstructionSource<'a> {
    /// Borrows the immutable file carrying volume, source path and descriptor.
    pub fn new(file: &'a FileRef) -> Self {
        Self {
            file,
            volume: file.volume(),
            path: file.path(),
        }
    }

    /// Borrows an explicitly retained original source identity.
    pub fn with_identity(file: &'a FileRef, source: &'a InstructionIdentity) -> Self {
        Self {
            file,
            volume: source.volume(),
            path: source.path(),
        }
    }

    /// Canonical source filename.
    pub fn filename(self) -> &'a str {
        self.path.rsplit('/').next().unwrap_or(self.path)
    }

    /// Canonical volume-relative directory; a root file belongs to `.`.
    pub fn directory(self) -> &'a str {
        self.path
            .rsplit_once('/')
            .map_or(".", |(directory, _)| directory)
    }

    /// Exact UTF-8 frame length, before reading its body.
    pub fn framed_bytes(self) -> Result<u64> {
        let overhead = "#  instructions for \n\n<INSTRUCTIONS>\n\n</INSTRUCTIONS>".len() as u64;
        self.file
            .descriptor()
            .byte_length()
            .checked_add(overhead)
            .and_then(|size| size.checked_add(self.filename().len() as u64))
            .and_then(|size| size.checked_add(self.directory().len() as u64))
            .ok_or_else(|| Error::Invalid("project instruction size overflows".into()))
    }

    /// Reserves the entire frame before any body read or allocation.
    pub fn reserve(self, remaining: &mut u64, io: Option<&ContentIo>) -> Result<u64> {
        *remaining = remaining.checked_sub(self.framed_bytes()?).ok_or_else(|| {
            crate::executor::stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "project instructions exceed render bound",
            )
        })?;
        Ok(self.file.descriptor().byte_length())
    }

    /// Frames already authenticated UTF-8 bytes, preserving them verbatim.
    pub fn render(self, bytes: &[u8], io: Option<&ContentIo>) -> Result<String> {
        if bytes.len() as u64 != self.file.descriptor().byte_length() {
            return Err(crate::executor::stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "instruction body length differs from its source",
            ));
        }
        let body = std::str::from_utf8(bytes).map_err(|_| {
            crate::executor::stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "project instructions are not UTF-8",
            )
        })?;
        crate::executor::with_history_io(io, |work| {
            crate::executor::preparation_format(
                format_args!(
                    "# {} instructions for {}\n\n<INSTRUCTIONS>\n{body}\n</INSTRUCTIONS>",
                    self.filename(),
                    self.directory()
                ),
                work,
            )
        })
    }

    pub(crate) fn same_source(
        self,
        other: Self,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<bool> {
        super::instruction_index::history_step_traced(work, 1)?;
        super::instruction_index::trace_operation("instruction-source-compare", 1);
        Ok(self.volume == other.volume
            && self.path == other.path
            && self.file.descriptor().sha256() == other.file.descriptor().sha256()
            && self.file.descriptor().byte_length() == other.file.descriptor().byte_length())
    }

    pub(super) fn index_key(self, work: &mut Option<&mut dyn HistoryIo>) -> Result<[u8; 32]> {
        super::instruction_index::source_key_history(self.volume, self.path, work)
    }
}

pub(crate) fn reserve_instruction_bodies(
    messages: &[ModelMessage],
    remaining: &mut u64,
    io: Option<&PreparationIo>,
) -> Result<()> {
    for (file, policy) in messages
        .iter()
        .flat_map(|message| message.content.file_parts())
    {
        if let FileProjectionPolicy::ProjectInstructions(source) = policy {
            InstructionSource::with_identity(file, source).reserve(remaining, io)?;
        }
    }
    Ok(())
}

pub(crate) fn project_instruction(
    instruction: &ProjectInstruction,
    io: Option<&PreparationIo>,
) -> Result<ModelMessage> {
    let identity = instruction.source();
    let source = with_history_io(io, |work| {
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        quota.replace(
            quota
                .retained()
                .checked_add(std::mem::size_of::<InstructionIdentity>() as u64)
                .ok_or_else(super::inventory::overflow)?,
        )?;
        let value = InstructionIdentity {
            volume: identity.volume.clone_in_quota(&mut quota)?,
            path: crate::core::copy_text(identity.path, &mut quota)?,
        };
        quota.transfer();
        Ok::<_, Error>(Box::new(value))
    })?;
    Ok(ModelMessage {
        role: ModelRole::User,
        content: ModelContent::Part(ModelContentPart::File {
            file: clone_file(instruction, io)?,
            policy: FileProjectionPolicy::ProjectInstructions(source),
        }),
    })
}

// Both discovery entrypoints call this preflight before cloning any instruction.
impl super::instruction_sources::InstructionSources<'_> {
    pub(crate) fn reservation(&self, maximum: u64, io: Option<&PreparationIo>) -> Result<u64> {
        let mut remaining = maximum;
        for file in self.iter(io) {
            let file = file?;
            let metadata = crate::executor::with_history_io(io, |work| {
                crate::executor::json::json_byte_length(&**file, remaining, work)
            })?;
            let overhead =
            r#"{"role":"user","content":{"kind":"file","file":,"policy":{"project_instructions":}}}"#
                .len() as u64
                + 1;
            #[derive(Serialize)]
            struct Source<'a> {
                volume: &'a crate::conversation::VolumeRef,
                path: &'a str,
            }
            let source = file.source();
            let identity_bytes = with_history_io(io, |work| {
                crate::executor::json::json_byte_length(
                    &Source {
                        volume: source.volume,
                        path: source.path,
                    },
                    remaining,
                    work,
                )
            })?;
            remaining = remaining
                .checked_sub(identity_bytes)
                .and_then(|size| size.checked_sub(metadata))
                .and_then(|size| size.checked_sub(overhead))
                .ok_or_else(|| {
                    crate::executor::stock_boundary(
                        io,
                        crate::wire::ErrorCode::Invalid,
                        "instruction metadata exceeds render bound",
                    )
                })?;
            file.source().reserve(&mut remaining, io)?;
        }
        Ok(maximum - remaining)
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn fixture(lengths: [usize; 2], nested: [bool; 2]) -> (Vec<ModelMessage>, u64) {
        let mut messages = Vec::new();
        let mut total = 0;
        for (index, (length, nested)) in lengths.into_iter().zip(nested).enumerate() {
            let name = if index == 0 { "AGENTS.md" } else { "CLAUDE.md" };
            let body = "x".repeat(length);
            let frame =
                format!("# {name} instructions for .\n\n<INSTRUCTIONS>\n{body}\n</INSTRUCTIONS>");
            total += frame.len() as u64;
            let file = super::super::instruction_tests::file(name, &body, "rules");
            let projected = project_instruction(&file.into(), None).unwrap();
            if nested {
                let ModelContent::Part(ModelContentPart::File { file, policy }) = projected.content
                else {
                    unreachable!()
                };
                if let Some(ModelMessage {
                    content:
                        ModelContent::Part(ModelContentPart::ToolResult {
                            content: ToolResultContent::Parts { parts },
                            ..
                        }),
                    ..
                }) = messages.last_mut()
                {
                    parts.push(ModelDataPart::File { file, policy });
                    continue;
                }
                messages.push(ModelMessage {
                    role: ModelRole::Assistant,
                    content: ModelContent::Part(ModelContentPart::ToolCall(ToolCall::new(
                        format!("read-{index}"),
                        "read".into(),
                        serde_json::json!({}),
                    ))),
                });
                messages.push(ModelMessage {
                    role: ModelRole::Tool,
                    content: ModelContent::Part(ModelContentPart::ToolResult {
                        call_id: format!("read-{index}"),
                        name: "read".into(),
                        content: ToolResultContent::Parts {
                            parts: vec![ModelDataPart::File { file, policy }],
                        },
                    }),
                });
            } else {
                messages.push(projected);
            }
        }
        (messages, total)
    }
    // Deliberately restore the reviewed top-level-only traversal.
    fn top_level_only(messages: &[ModelMessage], remaining: &mut u64) -> Result<()> {
        for part in messages.iter().flat_map(|message| message.content.parts()) {
            if let ModelContentPart::File {
                file,
                policy: FileProjectionPolicy::ProjectInstructions(source),
            } = part
            {
                InstructionSource::with_identity(file, source).reserve(remaining, None)?;
            }
        }
        Ok(())
    }
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(16))]
        #[test]
        fn aggregate_budget_covers_direct_and_nested_instruction_files(left in 1usize..7000,right in 1usize..7000) {
            for nested in [[false,false],[true,true],[false,true],[true,false]] {
                let (messages,total)=fixture([left,right],nested);
                for budget in [0,total-1,total,total+1] {
                    let mut remaining=budget;
                    let result=reserve_instruction_bodies(&messages,&mut remaining,None);
                    prop_assert_eq!(result.is_ok(),budget>=total);
                    if result.is_ok() {prop_assert_eq!(remaining,budget-total);}
                }
                if nested.iter().any(|nested|*nested) {
                    let mut remaining=total-1;
                    prop_assert!(top_level_only(&messages,&mut remaining).is_ok(),"top-level-only negative must miss the nested aggregate");
                }
            }
        }
    }
    #[test]
    fn prepared_request_rejects_two_nested_six_thousand_byte_instructions_before_dispatch() {
        let (messages, total) = fixture([6000, 6000], [true, true]);
        assert!(total > 10_000);
        let request = ModelRequest {
            model: Model::new(
                "synthetic".into(),
                "budget".into(),
                "1".into(),
                serde_json::json!({}),
            )
            .unwrap(),
            messages,
            tools: vec![crate::tool::ToolDefinition {
                name: "read".into(),
                revision: "1".into(),
                description: "Read".into(),
                input_schema: serde_json::json!({"type":"object"}),
                output_schema: serde_json::json!({}),
                projection_schema: serde_json::json!({}),
            }],
            max_output_tokens: Some(32),
        };
        assert!(top_level_only(&request.messages, &mut 10_000).is_ok());
        PreparedModelRequest::prepare(request.clone(), Limits::default(), None).unwrap();
        let Err(error) = PreparedModelRequest::prepare(
            request,
            Limits {
                render_bytes: 10_000,
                ..Limits::default()
            },
            None,
        ) else {
            panic!("nested aggregate was admitted")
        };
        assert!(
            error
                .to_string()
                .contains("project instructions exceed render bound"),
            "{error}"
        );
    }
}
