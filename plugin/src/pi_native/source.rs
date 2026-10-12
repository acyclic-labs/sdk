use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PiNativeSourceMode {
    CallerTool,
    Scratch,
}

/// Exact native source selection retained in the original child admission.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct PiNativeSourceCut {
    pub(crate) mode: PiNativeSourceMode,
    pub(crate) parent_session_id: String,
    pub(crate) parent_session_file: Option<String>,
    pub(crate) parent_cwd: String,
    pub(crate) source_leaf_id: Option<String>,
    pub(crate) owner_entry_id: Option<String>,
    pub(crate) parent_leaf_id: Option<String>,
    pub(crate) captured_at: String,
    pub(crate) entries_json: String,
    pub(crate) sha256: String,
}

/// Closed launch graph diagnostic inventory: original derives, camel-case source fields, snake-case mode tags and pinned serde primitive expectations.
pub(crate) const PI_NATIVE_LAUNCH_DIAGNOSTIC_LABEL_BYTES: u64 =
    crate::pi_root_provider::pi_serde_diagnostic_labels_bytes(&[
        "PiNativeLaunchIntent",
        "struct PiNativeLaunchIntent",
        "agent_alias",
        "child_session_id",
        "source",
        "invocation",
        "session_path",
        "session_bytes",
        "maximum_entries",
        "maximum_bytes",
        "PiNativeSourceCut",
        "struct PiNativeSourceCut",
        "mode",
        "parentSessionId",
        "parentSessionFile",
        "parentCwd",
        "sourceLeafId",
        "ownerEntryId",
        "parentLeafId",
        "capturedAt",
        "entriesJson",
        "sha256",
        "PiNativeSourceMode",
        "enum PiNativeSourceMode",
        "caller_tool",
        "scratch",
        "PiNativeHostRuntime",
        "struct PiNativeHostRuntime",
        "PiNativeInvocation",
        "struct PiNativeInvocation",
        "runtime_digest",
        "an array of length 32",
        "u8",
        "version",
        "executable",
        "executable_prefix",
        "extension",
        "provider_extension",
        "session_file",
        "cwd",
        "provider",
        "model",
        "prompt",
        "builtin_tools",
        "environment",
        "timeout_ms",
        "control_timeout_ms",
        "cancellation_poll_ms",
        "maximum_output_bytes",
        "maximum_result_bytes",
        "a string",
        "path string",
        "a sequence",
        "a map",
        "option",
        "u32",
        "u64",
    ]);

const NATIVE_HEADER_DIAGNOSTIC_LABEL_BYTES: u64 =
    crate::pi_root_provider::pi_serde_diagnostic_labels_bytes(&[
        "NativeHeader",
        "struct NativeHeader",
        "type",
        "version",
        "id",
        "timestamp",
        "cwd",
        "a string",
        "u32",
    ]);

pub(super) const NATIVE_CAPABILITIES_DIAGNOSTIC_LABEL_BYTES: u64 =
    crate::pi_root_provider::pi_serde_diagnostic_labels_bytes(&[
        "Capabilities",
        "a sequence",
        "a string",
    ]);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeHeader {
    #[serde(rename = "type")]
    kind: String,
    version: u32,
    id: String,
    timestamp: String,
    cwd: String,
}

impl PiNativeSourceCut {
    /// Strictly checks the actual native bytes before Filesystem publication.

    pub(crate) fn validate_session(
        &self,
        child: &str,
        cwd: &Path,
        bytes: &[u8],
        maximum_entries: u32,
        maximum_bytes: u64,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        work.remaining()?;
        if maximum_entries == 0
            || maximum_bytes == 0
            || bytes.len() as u64 > maximum_bytes
            || self.entries_json.len() as u64 > maximum_bytes
            || !cwd.is_absolute()
            || child.is_empty()
            || self.parent_session_id.is_empty()
            || child == self.parent_session_id
            || self.captured_at.is_empty()
            || !Path::new(&self.parent_cwd).is_absolute()
        {
            return Err(invalid(
                "Pi native session differs from its original source cut or bounds",
            ));
        }
        let mut digest = [0_u8; 64];
        work.charge(WorkCounters {
            bytes_hashed: self.entries_json.len() as u64,
            ..Default::default()
        })?;
        hex::encode_to_slice(Sha256::digest(self.entries_json.as_bytes()), &mut digest)
            .expect("SHA256 hexadecimal output has its fixed exact extent");
        if self.sha256.as_bytes() != digest {
            return Err(invalid(
                "Pi native session differs from its original source digest",
            ));
        }
        let selected = crate::pi_root_provider::guarded_pi_decode::<Vec<Value>>(
            self.entries_json.as_bytes(),
            maximum_bytes,
            std::mem::size_of::<Vec<Value>>() as u64,
            acyclic_harness::wire::ErrorCode::Invalid,
            JsonValueSchema::Root,
            0,
            work,
        )?;
        if selected.0.len() as u64 > u64::from(maximum_entries) {
            return Err(invalid("Pi native source cut exceeds its entry allowance"));
        }
        match self.mode {
            PiNativeSourceMode::CallerTool
                if self
                    .owner_entry_id
                    .as_deref()
                    .is_some_and(|owner| !owner.is_empty())
                    && self
                        .source_leaf_id
                        .as_deref()
                        .is_some_and(|leaf| !leaf.is_empty()) => {}
            PiNativeSourceMode::Scratch
                if self.owner_entry_id.is_none()
                    && self.source_leaf_id.is_none()
                    && self.parent_leaf_id.is_none()
                    && selected.0.is_empty() => {}
            _ => {
                return Err(invalid(
                    "Pi native source mode differs from its authentic SDK selection",
                ));
            }
        }
        let mut previous: Option<&str> = None;
        let mut ids = pi_native_vec(selected.0.len(), work)?;
        for entry in &selected.0 {
            let id = entry
                .get("id")
                .and_then(Value::as_str)
                .ok_or_else(|| invalid("Pi native source entry has no identity"))?;
            let parent = entry
                .get("parentId")
                .ok_or_else(|| invalid("Pi native source entry has no parent"))?;
            if id.is_empty()
                || entry
                    .get("type")
                    .and_then(Value::as_str)
                    .is_none_or(|kind| kind == "session")
                || (previous.is_none() && !parent.is_null())
                || (previous.is_some() && parent.as_str() != previous)
                || Some(id) == self.owner_entry_id.as_deref()
            {
                return Err(invalid(
                    "Pi native source is not the exact selected parent chain",
                ));
            }
            previous = Some(id);
            ids.0.push(id);
        }
        native_sort_entry_ids(&mut ids.0, work)?;
        if ids.0.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(invalid("Pi native source repeats an entry identity"));
        }
        if previous != self.parent_leaf_id.as_deref() {
            return Err(invalid("Pi native source leaf differs"));
        }
        let text =
            std::str::from_utf8(bytes).map_err(|_| invalid("Pi native session is not UTF-8"))?;
        if !text.ends_with('\n') || text.contains('\r') {
            return Err(invalid("Pi child session must be complete native JSONL"));
        }
        let mut lines = text[..text.len() - 1].split('\n');
        let header = crate::pi_root_provider::guarded_pi_decode::<NativeHeader>(
            lines
                .next()
                .ok_or_else(|| invalid("Pi child header is absent"))?
                .as_bytes(),
            maximum_bytes,
            std::mem::size_of::<NativeHeader>() as u64,
            acyclic_harness::wire::ErrorCode::Invalid,
            JsonValueSchema::None,
            NATIVE_HEADER_DIAGNOSTIC_LABEL_BYTES,
            work,
        )?;
        if header.0.kind != "session"
            || header.0.version != 3
            || header.0.id != child
            || header.0.timestamp != self.captured_at
            || Path::new(&header.0.cwd) != cwd
        {
            return Err(invalid(
                "Pi child header differs from its prepared native identity",
            ));
        }
        for entry in &selected.0 {
            let actual = crate::pi_root_provider::guarded_pi_decode::<Value>(
                lines
                    .next()
                    .ok_or_else(|| invalid("Pi child lost a source entry"))?
                    .as_bytes(),
                maximum_bytes,
                std::mem::size_of::<Value>() as u64,
                acyclic_harness::wire::ErrorCode::Invalid,
                JsonValueSchema::Root,
                0,
                work,
            )?;
            if &actual.0 != entry {
                return Err(invalid(
                    "Pi child entry differs from its original native source",
                ));
            }
        }
        if lines.next().is_some() {
            return Err(invalid(
                "Pi child session contains entries outside its source cut",
            ));
        }
        Ok(())
    }
}

fn native_sort_entry_ids(ids: &mut [&str], work: &NativeMaterializationWork) -> Result<()> {
    fn sift(
        ids: &mut [&str],
        mut root: usize,
        end: usize,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        loop {
            let mut child = root
                .checked_mul(2)
                .and_then(|value| value.checked_add(1))
                .ok_or_else(|| {
                    acyclic_harness::Error::from(acyclic_fs::WorkError::Overflow.into())
                })?;
            if child >= end {
                return Ok(());
            }
            if child + 1 < end {
                work.charge(WorkCounters {
                    items_examined: 1,
                    ..Default::default()
                })?;
                if ids[child] < ids[child + 1] {
                    child += 1;
                }
            }
            work.charge(WorkCounters {
                items_examined: 1,
                ..Default::default()
            })?;
            if ids[root] >= ids[child] {
                return Ok(());
            }
            ids.swap(root, child);
            root = child;
        }
    }
    let length = ids.len();
    for root in (0..length / 2).rev() {
        sift(ids, root, length, work)?;
    }
    for end in (1..length).rev() {
        ids.swap(0, end);
        sift(ids, 0, end, work)?;
    }
    Ok(())
}
