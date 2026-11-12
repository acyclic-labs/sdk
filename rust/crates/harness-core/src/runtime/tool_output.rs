//! Original output and its cursor share one ordinary filesystem publication.
use super::*;
use crate::conversation::{ContentChange, ContentChangeResult};

/// Version-one original output cursor. It carries no output or credentials.
/// Its filesystem publication supplies the authoritative activity time.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ToolOutputActivity {
    /// Must be one.
    pub version: u32,
    /// Original execution operation, including its segment.
    pub turn: OperationId,
    /// Original tool effect identity.
    pub call: OperationId,
    /// Exclusive count of output batches in this job.
    pub through: u64,
}

/// Exact filesystem key followed by the original job's alarm actor.
pub fn tool_output_activity_path(turn: OperationId, call: OperationId) -> String {
    format!("tools/{turn}/{call}/activity.json")
}

/// Shared publication core for tool output on any content backend. Native
/// capture adapters retain only their byte buffering and output channel names.
pub struct ToolOutputLog {
    context: ToolContext,
    through: u64,
    activity: Option<FileRef>,
}

impl ToolOutputLog {
    /// Bind the original tool context; construction performs no I/O.
    pub fn new(context: ToolContext) -> Self {
        Self {
            context,
            through: 0,
            activity: None,
        }
    }

    /// Publish one immutable chunk. Quiet supervision adds a conditional cursor
    /// update to that same commit. No state advances before its receipt, so an
    /// unknown acknowledgement can replay the exact original batch and identity.
    pub async fn publish_chunk(
        &mut self,
        name: &str,
        bytes: &[u8],
        operation: OperationId,
    ) -> Result<FileRef> {
        let path = format!(
            "tools/{}/{}/{name}",
            self.context.turn_id(),
            self.context.operation_id()
        );
        let display = name.split('/').next().unwrap_or(name);
        if !self.context.output_activity {
            return self
                .context
                .stage_file_once(
                    operation,
                    &path,
                    bytes,
                    "application/octet-stream",
                    display,
                    None,
                )
                .await
                .map(|(file, _)| file);
        }
        let next = self
            .through
            .checked_add(1)
            .ok_or_else(|| Error::Storage("tool output cursor overflow".into()))?;
        let cursor = ToolOutputActivity {
            version: 1,
            turn: self.context.turn_id(),
            call: self.context.operation_id(),
            through: next,
        };
        let encoded =
            serde_json::to_vec(&cursor).map_err(|error| Error::Invalid(error.to_string()))?;
        let activity_path = tool_output_activity_path(cursor.turn, cursor.call);
        let volume = &self
            .context
            .task()
            .content_binding(None)
            .ok_or_else(|| Error::Unsupported("tool output writer is not bound".into()))?
            .volume;
        let changes = [
            ContentChange::CreateAbsent {
                path: &path,
                bytes,
                media_type: "application/octet-stream",
                display_name: display,
            },
            match &self.activity {
                Some(source) => ContentChange::UpdateExpected {
                    source,
                    bytes: &encoded,
                },
                None => ContentChange::CreateAbsent {
                    path: &activity_path,
                    bytes: &encoded,
                    media_type: "application/json",
                    display_name: "activity.json",
                },
            },
        ];
        let (published, _) = self
            .context
            .stage_tool_changes(operation, volume, &changes, None)
            .await?;
        let mut members = published.members.into_iter();
        let Some(ContentChangeResult::Created(file)) = members.next() else {
            return Err(Error::Storage(
                "tool output publication omitted its original chunk".into(),
            ));
        };
        let Some(ContentChangeResult::Created(activity) | ContentChangeResult::Updated(activity)) =
            members.next()
        else {
            return Err(Error::Storage(
                "tool output publication omitted its original activity".into(),
            ));
        };
        self.activity = Some(activity);
        self.through = next;
        Ok(file)
    }
}
