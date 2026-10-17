//! Child lifecycle intents stored in the parent's task volume.
use super::{Component, Error, Result};
use serde::{Deserialize, Serialize};

/// Harness acknowledgement of a child's execution state.
/// Results remain in the named task's ordinary completion and chunk files.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct ChildStatus {
    /// Current or most recently settled turn.
    #[cfg_attr(feature = "wasm", tsify(type = "string"))]
    pub task: crate::TaskId,
    /// Lifecycle state reached by the worker.
    pub state: ChildState,
    /// All effects of the current turn have stopped or completed.
    pub settled: bool,
    /// Immutable request/inbox files included in this turn.
    pub inputs: u64,
}

impl ChildStatus {
    /// Decodes a status acknowledgement; terminal and interrupted workers must
    /// acknowledge quiescence before reporting those states.
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        let status: Self = serde_json::from_slice(bytes).map_err(|_| Error::InvalidSegment)?;
        if status.inputs == 0
            || (matches!(
                status.state,
                ChildState::Cancelled | ChildState::Archived | ChildState::Interrupted
            ) && !status.settled)
        {
            return Err(Error::InvalidSegment);
        }
        Ok(status)
    }
}

pub(crate) fn child_scope(
    parent: &crate::runtime::RuntimeScope,
    tools: Option<&[String]>,
    grants: Option<&[String]>,
) -> crate::Result<crate::runtime::RuntimeScope> {
    let selected = match grants {
        Some(grants) => parent.narrow(crate::Capabilities::new(grants), parent.limits())?,
        None => parent.clone(),
    };
    let parent = &selected;
    let Some(tools) = tools else {
        return Ok(selected);
    };
    let requested = crate::Capabilities::new(tools.iter().map(|name| format!("tool:call:{name}")));
    if requested
        .iter()
        .any(|grant| !parent.grants().contains(grant))
    {
        return Err(crate::Error::Unauthorized(
            "child tool selection widens its parent".into(),
        ));
    }
    let grants = crate::Capabilities::new(
        parent
            .grants()
            .iter()
            .filter(|grant| !grant.starts_with("tool:call:") || requested.contains(grant)),
    );
    parent.narrow(grants, parent.limits())
}

/// Spawn request read from `agents/<ref>/request.md`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildRequest {
    /// Markdown prompt, excluding the optional frontmatter.
    pub prompt: String,
    /// Model name within the parent's configured provider.
    pub model: Option<String>,
    /// Requested subset of the parent's tool grants.
    pub tools: Option<Vec<String>>,
    /// Optional attenuated capability selection, before child resource bindings.
    pub grants: Option<Vec<String>>,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RequestMetadata {
    model: Option<String>,
    tools: Option<Vec<String>>,
    grants: Option<Vec<String>>,
}

impl ChildRequest {
    /// Parses a bounded Markdown request with flat YAML metadata. Aliases,
    /// duplicate keys, tags and recursive metadata cannot grant authority.
    pub fn parse(bytes: &[u8], maximum_bytes: u64) -> crate::Result<Self> {
        let invalid = || crate::Error::Invalid("invalid child request".into());
        if bytes.is_empty() || bytes.len() as u64 > maximum_bytes {
            return Err(invalid());
        }
        let text = std::str::from_utf8(bytes).map_err(|_| invalid())?;
        let (metadata, prompt) = if text.starts_with("---\n") || text.starts_with("---\r\n") {
            let (start, end, body) = crate::context::frontmatter_end(bytes)?;
            let header = text.get(start..end).ok_or_else(invalid)?;
            let options = serde_saphyr::options! {
                budget: serde_saphyr::budget! {
                    max_events: header.len().saturating_add(1),
                    max_nodes: header.len().saturating_add(1), max_depth: 3,
                    flow_nesting_limit: 3, max_documents: 1,
                    max_aliases: 0, max_anchors: 0, max_merge_keys: 0,
                    max_total_scalar_bytes: header.len(),
                },
                duplicate_keys: serde_saphyr::DuplicateKeyPolicy::Error,
                merge_keys: serde_saphyr::MergeKeyPolicy::Error,
                reject_unsupported_tags: true, emit_comments: false, with_snippet: false,
            };
            let metadata = if header.trim().is_empty() {
                RequestMetadata::default()
            } else {
                serde_saphyr::from_str_with_options(header, options).map_err(|_| invalid())?
            };
            (metadata, text.get(body..).ok_or_else(invalid)?)
        } else {
            (RequestMetadata::default(), text)
        };
        if prompt.is_empty()
            || metadata.model.as_ref().is_some_and(String::is_empty)
            || metadata
                .tools
                .as_ref()
                .is_some_and(|tools| tools.iter().any(String::is_empty))
            || metadata
                .grants
                .as_ref()
                .is_some_and(|grants| grants.iter().any(String::is_empty))
        {
            return Err(invalid());
        }
        Ok(Self {
            prompt: prompt.into(),
            model: metadata.model,
            tools: metadata.tools,
            grants: metadata.grants,
        })
    }

    /// Renders metadata as JSON within YAML frontmatter, preserving the literal
    /// prompt even when that prompt itself begins with a frontmatter delimiter.
    pub fn encode(&self) -> Vec<u8> {
        if self.model.is_some()
            || self.tools.is_some()
            || self.grants.is_some()
            || self.prompt.starts_with("---\n")
            || self.prompt.starts_with("---\r\n")
        {
            format!(
                "---\n{}\n---\n{}",
                serde_json::json!({
                    "model":self.model,"tools":self.tools,"grants":self.grants,
                }),
                self.prompt
            )
            .into_bytes()
        } else {
            self.prompt.as_bytes().to_vec()
        }
    }
}

/// Files owned by a parent below `agents/<ref>/`.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[cfg_attr(feature = "wasm", tsify(large_number_types_as_bigints))]
pub enum ChildFile {
    /// Immutable spawn prompt with optional model and tool frontmatter.
    Request,
    /// Immutable message, identified by the sender's operation identity.
    Inbox(Component),
    /// Latest lifecycle intent. Publication is fenced by the task-volume cut.
    Control,
    /// Harness-owned execution status, followed by the parent and UI.
    Status,
    /// Authenticated delivered-input frontier, advanced after mailbox acceptance.
    Delivered,
    /// Immutable outcome of one completed mailbox input.
    Result(u64),
}

impl ChildFile {
    pub(super) fn parse(parts: &[&str]) -> Result<Self> {
        match parts {
            ["request.md"] => Ok(Self::Request),
            ["inbox", name] => Ok(Self::Inbox(Component::new(
                name.strip_suffix(".md").ok_or(Error::InvalidPath)?,
            )?)),
            ["control"] => Ok(Self::Control),
            ["status.json"] => Ok(Self::Status),
            ["delivered.json"] => Ok(Self::Delivered),
            ["results", name] => {
                let number = name.strip_suffix(".json").ok_or(Error::InvalidPath)?;
                let turn: u64 = number.parse().map_err(|_| Error::InvalidPath)?;
                if turn == 0 || turn.to_string() != number {
                    return Err(Error::InvalidPath);
                }
                Ok(Self::Result(turn))
            }
            _ => Err(Error::InvalidPath),
        }
    }

    pub(super) fn path(&self) -> String {
        match self {
            Self::Request => "request.md".into(),
            Self::Inbox(id) => format!("inbox/{}.md", id.as_str()),
            Self::Control => "control".into(),
            Self::Status => "status.json".into(),
            Self::Delivered => "delivered.json".into(),
            Self::Result(turn) => format!("results/{turn}.json"),
        }
    }
}

/// Exact text accepted in a child's control file.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Control {
    /// Stop starting work until resumed.
    Pause,
    /// Continue paused or interrupted work.
    Resume,
    /// Stop the current turn, retaining the child for further messages.
    Interrupt,
    /// Permanently cancel this child identity.
    Cancel,
    /// Permanently freeze this child identity, retaining its result view.
    Archive,
}

impl Control {
    /// Parses canonical file bytes; aliases and whitespace are rejected.
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        match bytes {
            b"pause" => Ok(Self::Pause),
            b"resume" => Ok(Self::Resume),
            b"interrupt" => Ok(Self::Interrupt),
            b"cancel" => Ok(Self::Cancel),
            b"archive" => Ok(Self::Archive),
            _ => Err(Error::InvalidSegment),
        }
    }

    /// Canonical file content.
    pub const fn bytes(self) -> &'static [u8] {
        match self {
            Self::Pause => b"pause",
            Self::Resume => b"resume",
            Self::Interrupt => b"interrupt",
            Self::Cancel => b"cancel",
            Self::Archive => b"archive",
        }
    }
}

/// Desired execution state, folded from accepted control-file publications.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum ChildState {
    /// Child may start or continue work.
    #[default]
    Running,
    /// Child retains work but may not advance it.
    Paused,
    /// Current turn has been interrupted.
    Interrupted,
    /// Child has been cancelled permanently.
    Cancelled,
    /// Child is frozen permanently and omitted from active lists.
    Archived,
}

impl ChildState {
    /// Pure lifecycle reducer. Cancelled identities can only advance to archive;
    /// neither terminal state can be reused or resumed.
    #[must_use]
    pub const fn apply(self, control: Control) -> Self {
        if matches!(self, Self::Archived) {
            return self;
        }
        if matches!(self, Self::Cancelled) {
            return if matches!(control, Control::Archive) {
                Self::Archived
            } else {
                self
            };
        }
        match control {
            Control::Pause => Self::Paused,
            Control::Resume => Self::Running,
            Control::Interrupt => Self::Interrupted,
            Control::Cancel => Self::Cancelled,
            Control::Archive => Self::Archived,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    const CONTROLS: [Control; 5] = [
        Control::Pause,
        Control::Resume,
        Control::Interrupt,
        Control::Cancel,
        Control::Archive,
    ];

    #[test]
    fn every_transition_is_idempotent_and_terminal_states_never_thaw() {
        // Exhaust the complete bounded reducer domain rather than sample it.
        for state in [
            ChildState::Running,
            ChildState::Paused,
            ChildState::Interrupted,
            ChildState::Cancelled,
            ChildState::Archived,
        ] {
            for control in CONTROLS {
                let next = state.apply(control);
                assert_eq!(next.apply(control), next);
                if state == ChildState::Archived {
                    assert_eq!(next, state);
                }
                if state == ChildState::Cancelled {
                    assert_eq!(
                        next,
                        if control == Control::Archive {
                            ChildState::Archived
                        } else {
                            ChildState::Cancelled
                        }
                    );
                }
                assert_eq!(Control::parse(control.bytes()).unwrap(), control);
            }
        }
    }

    #[test]
    fn request_metadata_rejects_aliases_duplicates_and_recursive_shapes() {
        for header in [
            "model: first\nmodel: second",
            "tools: &tools [read]\nmodel: *tools",
            "tools: [[read]]",
            "tools: [read]\ncapabilities: [admin]",
            "model: ''",
            "tools: ['']",
            "grants: ['']",
            "grants: [[read]]",
            "grants: [read]\ngrants: [write]",
        ] {
            let request = format!("---\n{header}\n---\nwork");
            assert!(
                ChildRequest::parse(request.as_bytes(), 4096).is_err(),
                "{header}"
            );
        }
        for prompt in ["---\nliteral prompt", "---\r\nliteral prompt"] {
            let request = ChildRequest {
                prompt: prompt.into(),
                model: None,
                tools: None,
                grants: None,
            };
            assert_eq!(
                ChildRequest::parse(&request.encode(), 4096).unwrap(),
                request
            );
        }
    }

    proptest! {
        #[test]
        fn execution_status_round_trips_and_requires_quiescent_terminal_states(
            task in any::<[u8;16]>(), inputs in 1u64..u64::MAX, selected in 0usize..5, settled in any::<bool>(),
        ) {
            let states = [ChildState::Running, ChildState::Paused, ChildState::Interrupted,
                ChildState::Cancelled, ChildState::Archived];
            let state = states[selected];
            let status = ChildStatus { task: crate::TaskId::from_bytes(task), state, settled, inputs };
            let decoded = ChildStatus::parse(&serde_json::to_vec(&status).unwrap());
            let terminal = matches!(state, ChildState::Interrupted | ChildState::Cancelled | ChildState::Archived);
            prop_assert_eq!(decoded.is_ok(), !terminal || settled);
            if let Ok(decoded) = decoded { prop_assert_eq!(decoded, status); }
        }

        #[test]
        fn child_tool_selection_is_attenuation_with_unchanged_numeric_limits(
            parent_tools in prop::collection::btree_set("[a-z]{1,5}", 0..10),
            requested_tools in prop::collection::btree_set("[a-z]{1,5}", 0..10),
        ) {
            use crate::{Capabilities, conversation::Limits, runtime::RuntimeScope};
            let grants = Capabilities::new(std::iter::once("model:generate".to_owned())
                .chain(parent_tools.iter().map(|name| format!("tool:call:{name}"))));
            let parent = RuntimeScope::new(grants, Limits::default()).unwrap();
            let requested: Vec<_> = requested_tools.iter().cloned().collect();
            let child = child_scope(&parent, Some(&requested), None);
            prop_assert_eq!(child.is_ok(), requested_tools.is_subset(&parent_tools));
            if let Ok(child) = child {
                prop_assert_eq!(child.limits(), parent.limits());
                prop_assert_eq!(child.run_limits(), parent.run_limits());
                prop_assert!(child.grants().iter().all(|grant| parent.grants().contains(grant)));
                prop_assert!(child.grants().contains("model:generate"));
                for tool in &parent_tools {
                    prop_assert_eq!(child.grants().contains(&format!("tool:call:{tool}")), requested_tools.contains(tool));
                }
            }
        }

        #[test]
        fn child_grants_are_a_subset_and_never_change_limits(
            parent_grants in prop::collection::btree_set("[a-z:]{1,24}", 0..10),
            requested_grants in prop::collection::btree_set("[a-z:]{1,24}", 0..10),
        ) {
            use crate::{Capabilities, conversation::Limits, runtime::RuntimeScope};
            let parent = RuntimeScope::new(Capabilities::new(&parent_grants), Limits::default()).unwrap();
            let requested: Vec<_> = requested_grants.iter().cloned().collect();
            let child = child_scope(&parent, None, Some(&requested));
            prop_assert_eq!(child.is_ok(), requested_grants.is_subset(&parent_grants));
            if let Ok(child) = child {
                prop_assert_eq!(child.grants(), &Capabilities::new(&requested_grants));
                prop_assert_eq!(child.limits(), parent.limits());
                prop_assert_eq!(child.run_limits(), parent.run_limits());
            }
            let inherited = child_scope(&parent, None, None).unwrap();
            prop_assert_eq!(inherited.grants(), parent.grants());
        }

        #[test]
        fn request_metadata_round_trips_without_changing_the_literal_prompt(
            prompt in ".{1,100}", model in prop::option::of("[a-z]{1,10}"),
            tools in prop::option::of(prop::collection::vec("[a-z]{1,10}", 0..8)),
            grants in prop::option::of(prop::collection::vec("[a-z:]{1,24}", 0..8)),
        ) {
            let request = ChildRequest { prompt, model, tools, grants };
            let encoded = request.encode();
            prop_assert_eq!(ChildRequest::parse(&encoded, encoded.len() as u64).unwrap(), request);
            prop_assert!(ChildRequest::parse(&encoded, encoded.len() as u64 - 1).is_err());
        }

        #[test]
        fn terminal_state_survives_every_suffix(commands in prop::collection::vec(0usize..5, 0..100)) {
            for terminal in [ChildState::Archived, ChildState::Cancelled] {
                let state = commands.iter().fold(terminal, |state, &i| state.apply(CONTROLS[i]));
                let expected = if terminal == ChildState::Cancelled
                    && commands.iter().any(|&i| CONTROLS[i] == Control::Archive)
                { ChildState::Archived } else { terminal };
                prop_assert_eq!(state, expected);
            }
        }

        #[test]
        fn accepted_control_bytes_round_trip(bytes in prop::collection::vec(any::<u8>(), 0..32)) {
            if let Ok(control) = Control::parse(&bytes) {
                prop_assert_eq!(control.bytes(), bytes.as_slice());
            }
        }

        #[test]
        fn child_paths_round_trip_and_queries_select_exactly_the_child(
            reference in "[a-z][a-z0-9_-]{0,20}", message in "[a-z][a-z0-9_-]{0,20}",
        ) {
            use super::super::{FileKey, Query};
            let id = Component::new(&reference).unwrap();
            for file in [ChildFile::Request, ChildFile::Control, ChildFile::Inbox(Component::new(message).unwrap())] {
                let key = FileKey::Child { reference: id.clone(), file };
                prop_assert_eq!(FileKey::parse(&key.path()).unwrap(), key.clone());
                prop_assert!(Query::Children.matches(&key));
                prop_assert!(Query::Child(id.clone()).matches(&key));
                let other = Query::Child(Component::new(format!("other-{reference}")).unwrap());
                prop_assert!(!other.matches(&key));
            }
        }
    }
}

impl ChildStatus {
    /// Terminal quiescence removes the child from active lists, retaining its files.
    pub fn active(self) -> bool {
        !self.settled || !matches!(self.state, ChildState::Archived | ChildState::Cancelled)
    }
}
