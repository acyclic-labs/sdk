//! Bounded presentation over the existing process stream. Overflow goes to
//! the task's scratch mount and is captured by the command's ordinary commit.
use std::{
    io::{self, Write},
    path::{Path, PathBuf},
};

pub(super) struct Tail {
    retained: crate::tools::shell::OutputTail,
    path: PathBuf,
    spill: Option<std::fs::File>,
}

impl Tail {
    pub(super) fn new(path: PathBuf, maximum: usize) -> Self {
        Self {
            retained: crate::tools::shell::OutputTail::new(maximum),
            path,
            spill: None,
        }
    }

    pub(super) fn observe(&mut self, bytes: &[u8], stderr: bool) -> io::Result<()> {
        let path = &self.path;
        let spill = &mut self.spill;
        self.retained.observe_with(bytes, stderr, |first, second| {
            ensure_spill(spill, path, first, second)?;
            if let Some(file) = spill {
                file.write_all(bytes).map_err(|error| io::Error::new(error.kind(), format!("shell spill write failed: {error}; recovery: inspect {} and reconcile the original command", path.display())))?;
            }
            Ok(())
        })
    }

    /// Before eviction this buffer is the complete output, so a presentation
    /// cut discovered at settlement can still retain every original byte.
    pub(super) fn ensure_spill(&mut self) -> io::Result<()> {
        let (first, second) = self.retained.as_slices();
        ensure_spill(&mut self.spill, &self.path, first, second)?;
        Ok(())
    }

    pub(super) fn settle(
        mut self,
        mut result: crate::tool::ToolResult,
        maximum: u64,
    ) -> crate::Result<crate::tool::ToolResult> {
        let (stdout, stderr, omitted) = self.retained.text();
        let fields = result.value.as_object_mut().ok_or_else(|| crate::Error::Invalid(
            "shell settlement requires terminal fields; recovery: bind the shared shell executor".into(),
        ))?;
        fields.insert("stdout".into(), stdout.into());
        fields.insert("stderr".into(), stderr.into());
        fields.insert("omitted_bytes".into(), omitted.into());
        let mut result = crate::tools::shell::bounded_result(result, maximum)?;
        if result.value["omitted_bytes"].as_u64().unwrap_or_default() > 0 {
            self.ensure_spill().map_err(crate::Error::process_io)?;
        } else {
            result.value["spill"] = serde_json::Value::Null;
        }
        Ok(result)
    }
}

fn ensure_spill(
    spill: &mut Option<std::fs::File>,
    path: &Path,
    first: &[u8],
    second: &[u8],
) -> io::Result<()> {
    if spill.is_none() {
        let mut file = std::fs::OpenOptions::new().write(true).create_new(true).open(path)
            .map_err(|error| io::Error::new(error.kind(), format!("shell spill creation failed: {error}; recovery: inspect {} and reconcile the original command", path.display())))?;
        file.write_all(first).and_then(|()| file.write_all(second)).map_err(|error| {
            io::Error::new(error.kind(), format!("shell spill prefix write failed: {error}; recovery: inspect {} and reconcile the original command", path.display()))
        })?;
        *spill = Some(file);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        // Arbitrary chunk boundaries cannot change the retained suffix or
        // exact spill bytes, and storage stays independent of total output.
        #[test]
        fn chunks_preserve_tail_and_complete_spill(bytes in prop::collection::vec(any::<u8>(), 0..4096), maximum in 0usize..128, chunk in 1usize..256, render in 256u64..1024) {
            let directory = tempfile::tempdir()?;
            let path = directory.path().join("output");
            let mut tail = Tail::new(path.clone(), maximum);
            for (index, bytes) in bytes.chunks(chunk).enumerate() {
                let stderr = index % 2 == 1;
                tail.observe(bytes, stderr)?;
                let (first, second) = tail.retained.as_slices();
                prop_assert!(first.len() + second.len() <= maximum);
            }
            let raw = bytes.len().saturating_sub(maximum);
            let (first, second) = tail.retained.as_slices();
            prop_assert!(first.iter().chain(second).copied().eq(bytes[raw..].iter().copied()));
            prop_assert_eq!(tail.spill.is_some(), bytes.len() > maximum);
            let result = tail.settle(crate::tool::ToolResult {
                value: serde_json::json!({"exit_code": 0, "spill": ".volumes/scratch/output"}),
            }, render)?;
            let projected = serde_json::json!({"kind":"parts", "parts":[{"kind":"text", "text":result.value["text"]}]}).to_string();
            prop_assert!(projected.len() as u64 <= render);
            let omitted = result.value["omitted_bytes"].as_u64().unwrap();
            if omitted > 0 {
                prop_assert_eq!(std::fs::read(path)?, bytes);
            } else {
                prop_assert!(!path.exists());
            }
        }
    }
}
