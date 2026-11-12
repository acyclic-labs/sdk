//! One immutable canonical selection before an authored program can act.
use super::*;

#[derive(Serialize, Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
struct Selection {
    version: u8,
    source: [u8; 32],
    call: std::result::Result<ToolInvocation, ToolFailureCause>,
}

impl StockExecution<'_> {
    pub(super) async fn lazy_call(
        &self,
        invocation: ToolInvocation,
        source: (&str, &str),
    ) -> Result<std::result::Result<ToolInvocation, ToolFailureCause>> {
        let context = self.invocation_context(&invocation)?.ok_or_else(|| {
            stock_boundary(
                self.io,
                crate::wire::ErrorCode::Unsupported,
                "file programs require an issued task context",
            )
        })?;
        let mut hash = blake3::Hasher::new_derive_key("acyclic.harness.authored-call.v1");
        if let Some(io) = self.io {
            io.preflight(acyclic_fs::WorkCounters {
                bytes_hashed: (source.0.len() + source.1.len()) as u64 + 16,
                ..Default::default()
            })?;
        }
        for text in [source.0, source.1] {
            hash.update(&(text.len() as u64).to_be_bytes());
            hash.update(text.as_bytes());
        }
        if let Some(io) = self.io {
            io.spent(acyclic_fs::WorkCounters {
                bytes_hashed: (source.0.len() + source.1.len()) as u64 + 16,
                ..Default::default()
            })?;
        }
        let source = *hash.finalize().as_bytes();
        let mut identity = blake3::Hasher::new_derive_key("acyclic.harness.authored-selection.v1");
        identity.update(&invocation.operation_id.into_bytes());
        let mut operation = [0; 16];
        identity.finalize_xof().fill(&mut operation);
        let operation = OperationId::from_bytes(operation);
        if let (Some(file), _) = context.observe_staged_file_once(operation, self.io).await? {
            let bytes = context.task().read_file(&file, self.io).await?;
            let selection: Selection = match self.io {
                Some(io) => retain_preparation(io.decode(
                    std::marker::PhantomData,
                    &bytes,
                    crate::wire::ErrorCode::Invalid,
                )?)?,
                None => serde_json::from_slice(&bytes)
                    .map_err(|error| Error::Invalid(error.to_string()))?,
            };
            if selection.version != 1 || selection.source != source {
                return Err(stock_boundary(
                    self.io,
                    crate::wire::ErrorCode::Conflict,
                    "authored call changed after selection",
                ));
            }
            return Ok(selection.call);
        }
        let selected: Result<ToolInvocation> = async {
            let tool = self
                .stock
                .tools
                .load(&invocation.name)
                .await?
                .ok_or_else(|| Error::NotFound("authored tool is not available".into()))?;
            with_history_io(self.io, |work| {
                check_json_schema_value(
                    &tool.definition.input_schema,
                    &invocation.arguments,
                    "tool input",
                    work,
                )
            })??;
            let mut canonical = tool
                .executor
                .canonical(&invocation, self.native_work())?
                .ok_or_else(|| {
                    Error::Unsupported("file-authored tools require a canonical capability".into())
                })?;
            let capability = tool.executor.capability().ok_or_else(|| {
                Error::Unsupported("file-authored tools require a canonical capability".into())
            })?;
            canonical.invocation.name = with_history_io(self.io, |work| {
                preparation_format(
                    format_args!(
                        "{}@{}",
                        capability.definition.name, capability.definition.revision
                    ),
                    work,
                )
            })?;
            if let Some(io) = self.io {
                for custody in canonical.custody.into_iter().flatten() {
                    io.absorb_native_custody(custody)?;
                }
            }
            Ok(canonical.invocation)
        }
        .await;
        let call = match selected {
            Ok(call) => Ok(call),
            Err(error) if error.is_tool_failure() && !error.leaves_effect_uncertain() => {
                Err(with_history_io(self.io, |work| {
                    ToolFailureCause::of(&error, work)
                })?)
            }
            Err(error) => return Err(error),
        };
        let selection = Selection {
            version: 1,
            source,
            call,
        };
        let bytes = match self.io {
            Some(io) => io.encode(&selection, io.remaining()?.bytes_encoded, true)?,
            None => crate::contract::canonical_json_bytes(&selection)?.into(),
        };
        let _hold = self.io.map(|io| io.guard_bytes(&bytes)).transpose()?;
        let path = with_history_io(self.io, |work| {
            preparation_format(
                format_args!("tools/{}/{operation}/selection.json", context.turn_id()),
                work,
            )
        })?;
        context
            .stage_file_once(
                operation,
                &path,
                &bytes,
                "application/json",
                "selection.json",
                self.io,
            )
            .await?;
        Ok(selection.call)
    }
}
