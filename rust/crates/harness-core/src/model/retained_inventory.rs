//! Resident JSON maps retain their unused `BTreeMap` node slots, too.
use super::inventory::add;
use super::*;

#[cfg(test)]
thread_local! {
    pub(super) static REQUEST_MESSAGES_EXAMINED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

fn map_slack(value: &Value) -> Result<u64> {
    crate::executor::json::json_value_retained_owned_bytes(value)?
        .checked_sub(crate::executor::json::json_value_logical_owned_bytes(
            value,
        )?)
        .ok_or_else(super::inventory::overflow)
}

fn part_slack(part: &ModelContentPart) -> Result<u64> {
    match part {
        ModelContentPart::ToolCall(call) => call
            .retained_payload_bytes()?
            .checked_sub(call.payload_bytes()?)
            .ok_or_else(super::inventory::overflow),
        ModelContentPart::ToolResult {
            content: ToolResultContent::Json { value },
            ..
        } => map_slack(value),
        _ => Ok(0),
    }
}

#[cfg(feature = "filesystem")]
impl ToolResultContent {
    pub(crate) fn retained_payload_bytes(&self) -> Result<u64> {
        add(
            self.payload_bytes()?,
            match self {
                Self::Json { value } => map_slack(value)?,
                _ => 0,
            },
        )
    }
}

#[cfg(all(test, feature = "filesystem", not(target_arch = "wasm32")))]
mod moved_tests;

impl ModelRequest {
    pub(super) fn retained_payload_bytes(&self) -> Result<u64> {
        let mut bytes = self.payload_bytes()?;
        for tool in &self.tools {
            for schema in [
                &tool.input_schema,
                &tool.output_schema,
                &tool.projection_schema,
            ] {
                bytes = add(bytes, map_slack(schema)?)?;
            }
        }
        for message in &self.messages {
            #[cfg(test)]
            REQUEST_MESSAGES_EXAMINED.set(REQUEST_MESSAGES_EXAMINED.get() + 1);
            for part in message.content.parts() {
                bytes = add(bytes, part_slack(part)?)?;
            }
        }
        Ok(bytes)
    }
}

#[cfg(feature = "filesystem")]
impl ModelContent {
    pub(crate) fn retained_payload_bytes(&self) -> Result<u64> {
        let mut bytes = self.payload_bytes()?;
        for part in self.parts() {
            bytes = add(bytes, part_slack(part)?)?;
        }
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sparse_request() -> Result<ModelRequest> {
        let sparse = serde_json::json!({"outer": [{"inner": "value"}]});
        Ok(ModelRequest {
            model: Model::new("test".into(), "inventory".into(), "1".into())?,
            messages: vec![ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Parts(vec![
                    ModelContentPart::ToolCall(
                        ToolCall::new("call".into(), "tool".into(), sparse.clone())
                            .with_metadata(sparse.clone()),
                    ),
                    ModelContentPart::ToolResult {
                        call_id: "call".into(),
                        name: "tool".into(),
                        content: ToolResultContent::Json {
                            value: sparse.clone(),
                        },
                    },
                ]),
            }],
            tools: vec![crate::tool::ToolDefinition {
                name: "tool".into(),
                revision: "1".into(),
                description: "test".into(),
                input_schema: sparse.clone(),
                output_schema: sparse.clone(),
                projection_schema: sparse,
            }],
            max_output_tokens: None,
        })
    }

    #[test]
    fn retained_request_inventory_covers_sparse_maps_in_every_json_slot() -> Result<()> {
        let request = sparse_request()?;
        let slack = map_slack(&request.tools[0].input_schema)?;
        assert!(slack > 0, "logical entry slots omit sparse node capacity");
        // Six JSON positions; the old logical inventory is the negative control.
        assert_eq!(
            request.retained_payload_bytes()? - request.payload_bytes()?,
            6 * slack
        );
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn every_model_json_copy_retains_and_releases_its_original_map_custody() -> Result<()> {
        let source = sparse_request()?;
        for native in [false, true] {
            let cancellation = acyclic_fs::CancellationToken::new();
            let budget = acyclic_fs::performance::work_budget(64 * 1024 * 1024, 4 * 1024 * 1024);
            let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
                .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
            let baseline = work.live_allocation_bytes();
            let io = if native {
                PreparationIo::from_native_work(&work)?
            } else {
                PreparationIo::from_budget(budget, &cancellation)?
            };
            let copied = clone_request(&source, Some(&io))?;
            assert_eq!(copied, source);
            let retained = copied.retained_payload_bytes()?;
            assert_eq!(
                io.owned_bytes()?,
                retained,
                "a model JSON copy lost map custody"
            );
            // The previous logical-only callback omits the unused map slots.
            assert!(copied.payload_bytes()? < retained);
            let custody = io.guard_owned(retained)?;
            drop(copied);
            drop(custody);
            assert_eq!(io.owned_bytes()?, 0);
            drop(io);
            assert_eq!(work.live_allocation_bytes(), baseline);
        }
        Ok(())
    }
}
