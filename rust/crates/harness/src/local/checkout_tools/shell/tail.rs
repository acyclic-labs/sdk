//! Bounded presentation over the existing process stream. The original spill
//! handle stays held until its bytes are published to the private output volume.
use std::{
    io::{self, Seek as _, Write},
    path::PathBuf,
};

pub(super) struct Tail {
    window: crate::tool::OutputWindow,
    maximum: usize,
    path: PathBuf,
    spill: Option<std::fs::File>,
}

impl Tail {
    pub(super) fn new(path: PathBuf, maximum: usize) -> Self {
        Self {
            window: crate::tool::OutputWindow::new(maximum),
            maximum,
            path,
            spill: None,
        }
    }

    pub(super) fn observe(&mut self, bytes: &[u8], stderr: bool) -> io::Result<()> {
        let total = self
            .window
            .total()
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| io::Error::other("shell output byte count overflow"))?;
        if self.spill.is_none() && total > self.maximum as u64 {
            self.ensure_spill()?;
        }
        if let Some(file) = &mut self.spill {
            file.write_all(bytes).map_err(|error| io::Error::new(error.kind(), format!("shell spill write failed: {error}; recovery: inspect {} and reconcile the original command", self.path.display())))?;
        }
        self.window.observe(bytes, stderr)
    }

    /// Before eviction the retained window contains the complete output.
    pub(super) fn ensure_spill(&mut self) -> io::Result<()> {
        if self.spill.is_none() {
            let mut file = std::fs::OpenOptions::new().read(true).write(true).create_new(true).open(&self.path)
                .map_err(|error| io::Error::new(error.kind(), format!("shell spill creation failed: {error}; recovery: inspect {} and reconcile the original command", self.path.display())))?;
            let bytes = self
                .window
                .retained()
                .map(|(byte, _)| byte)
                .collect::<Vec<_>>();
            file.write_all(&bytes).map_err(|error| io::Error::new(error.kind(), format!("shell spill prefix write failed: {error}; recovery: inspect {} and reconcile the original command", self.path.display())))?;
            self.spill = Some(file);
        }
        Ok(())
    }

    pub(super) fn settle(
        mut self,
        result: crate::tool::ToolResult,
        maximum: u64,
    ) -> crate::Result<(crate::tool::ToolResult, Option<std::fs::File>)> {
        let mut result = self.window.render(result, maximum)?;
        if result.value["omitted_bytes"].as_u64().unwrap_or_default() > 0 {
            self.ensure_spill().map_err(crate::Error::process_io)?;
            if let Some(file) = &mut self.spill {
                file.rewind().map_err(crate::Error::process_io)?;
            }
        } else {
            result.value["spill"] = serde_json::Value::Null;
        }
        Ok((result, self.spill))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn truncated_preview_keeps_both_ends_and_addresses_the_original_middle() -> crate::Result<()> {
        use std::io::Read as _;
        let directory = tempfile::tempdir().map_err(crate::Error::process_io)?;
        let path = directory.path().join("spill");
        let stdout = format!("beginning\n{}\nstdout end\n", "source line\n".repeat(4096));
        let stderr = "diagnostic end\n";
        let mut tail = Tail::new(path, 30_000);
        tail.observe(stdout.as_bytes(), false)
            .and_then(|()| tail.observe(stderr.as_bytes(), true))
            .map_err(crate::Error::process_io)?;
        let spill_path = "volume:///output/tools/command/spill.log";
        let (result, spill) = tail.settle(
            crate::tool::ToolResult {
                value: serde_json::json!({"exit_code": 0, "spill": spill_path}),
            },
            4096,
        )?;
        let fields = &result.value;
        let output = fields["stdout"].as_str().unwrap();
        let error = fields["stderr"].as_str().unwrap();
        assert!(!output.is_empty());
        assert!(output.starts_with("beginning\n"));
        assert!(output.ends_with("stdout end\n"));
        assert_eq!(error, stderr);
        let start = fields["omitted_range"]["start"].as_u64().unwrap();
        let end = fields["omitted_range"]["end"].as_u64().unwrap();
        assert_eq!(fields["omitted_bytes"].as_u64().unwrap(), end - start);
        assert!(
            fields.get("text").is_none(),
            "canonical channels have no duplicate text"
        );
        let projected = crate::tools::shell::project_result(&result, None)?.0;
        let text = projected["parts"][0]["text"].as_str().unwrap();
        assert!(text.find("beginning").unwrap() < text.find("bytes omitted").unwrap());
        assert!(text.find("bytes omitted").unwrap() < text.find("stdout end").unwrap());
        assert!(text.contains(spill_path));
        let mut complete = String::new();
        spill
            .unwrap()
            .read_to_string(&mut complete)
            .map_err(crate::Error::process_io)?;
        assert_eq!(complete, format!("{stdout}{stderr}"));
        assert!(text.contains(&complete[..start as usize]));
        assert!(text.ends_with(&complete[end as usize..]));
        Ok(())
    }

    proptest! {
        // Arbitrary chunk boundaries cannot change the retained ends or
        // exact spill bytes, and storage stays independent of total output.
        #[test]
        fn chunks_preserve_ends_ranges_and_complete_spill(
            directory in Just(std::sync::Arc::new(tempfile::tempdir().expect(
                "step=create shell property directory; recovery=cargo test -p acyclic-harness --lib local::checkout_tools::shell::tail::tests::chunks_preserve_tail_and_complete_spill"
            ))),
            bytes in prop::collection::vec(any::<u8>(), 0..4096), maximum in 0usize..128,
            chunk in 1usize..256, render in 256u64..1024,
        ) {
            let path = directory.path().join("output");
            // Reuse the directory, including while shrinking a failed case.
            // At most one spill survives until the next case replaces it.
            match std::fs::remove_file(&path) {
                Ok(()) => {},
                Err(error) if error.kind() == io::ErrorKind::NotFound => {},
                Err(error) => return Err(TestCaseError::fail(format!(
                    "step=retire previous shell property spill: {error}; recovery=inspect {} and rerun cargo test -p acyclic-harness --lib local::checkout_tools::shell::tail::tests::chunks_preserve_tail_and_complete_spill", path.display()
                ))),
            }
            let mut tail = Tail::new(path.clone(), maximum);
            let mut channels = Vec::new();
            for (index, bytes) in bytes.chunks(chunk).enumerate() {
                let stderr = index % 2 == 1;
                channels.extend(std::iter::repeat_n(stderr, bytes.len()));
                tail.observe(bytes, stderr)?;
                prop_assert!(tail.window.retained().count() <= maximum);
            }
            let head = bytes.len().min(maximum.div_ceil(2));
            let tail_start = bytes.len().saturating_sub(maximum/2).max(head);
            let expected = bytes[..head].iter().zip(&channels[..head])
                .chain(bytes[tail_start..].iter().zip(&channels[tail_start..]))
                .map(|(&byte,&channel)|(byte,channel)).collect::<Vec<_>>();
            prop_assert_eq!(tail.window.retained().collect::<Vec<_>>(), expected);
            prop_assert_eq!(tail.spill.is_some(), bytes.len() > maximum);
            let (result, _) = tail.settle(crate::tool::ToolResult {
                value: serde_json::json!({"exit_code": 0, "spill": "volume:///home/output"}),
            }, render)?;
            prop_assert!(result.value.get("text").is_none());
            let projected = crate::tools::shell::project_result(&result, None)?.0.to_string();
            prop_assert!(projected.len() as u64 <= render);
            let omitted = result.value["omitted_bytes"].as_u64().unwrap();
            let start = result.value["omitted_range"]["start"].as_u64().unwrap();
            let end = result.value["omitted_range"]["end"].as_u64().unwrap();
            prop_assert_eq!(end-start, omitted);
            prop_assert!(end <= bytes.len() as u64);
            if omitted > 0 {
                prop_assert_eq!(std::fs::read(path)?, bytes);
            } else {
                prop_assert!(!path.exists());
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn spill_publication_keeps_original_handle_after_symlink_substitution() -> crate::Result<()> {
        use std::io::Read as _;
        let directory = tempfile::tempdir().map_err(crate::Error::process_io)?;
        let path = directory.path().join("spill");
        let secret = directory.path().join("outside");
        std::fs::write(&secret, b"private host bytes").map_err(crate::Error::process_io)?;
        let mut tail = Tail::new(path.clone(), 1);
        tail.observe(b"command output", false)
            .map_err(crate::Error::process_io)?;
        std::fs::remove_file(&path).map_err(crate::Error::process_io)?;
        std::os::unix::fs::symlink(&secret, &path).map_err(crate::Error::process_io)?;
        let (_, spill) = tail.settle(
            crate::tool::ToolResult {
                value: serde_json::json!({"exit_code": 0, "spill": "volume:///output/spill"}),
            },
            1024,
        )?;
        let mut bytes = Vec::new();
        spill
            .expect("overflow retains its original handle")
            .read_to_end(&mut bytes)
            .map_err(crate::Error::process_io)?;
        assert_eq!(bytes, b"command output");
        Ok(())
    }
}
