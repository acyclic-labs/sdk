//! Production native file admission over a real workspace, for byte fuzzers.
use super::*;

/// Reuses one isolated workspace without retaining input or admission history.
pub struct WorkspaceInput {
    workspace: PathBuf,
    directory: PathBuf,
    requests: PathBuf,
    status: acyclic_fs::HeldRoot,
    _checkout: tempfile::TempDir,
}

impl WorkspaceInput {
    /// Creates the same canonical descriptor location used by the owner.
    pub fn new() -> Result<Self> {
        let checkout = tempfile::tempdir().map_err(storage)?;
        let workspace = std::fs::canonicalize(checkout.path()).map_err(storage)?;
        let directory = workspace.join(DIRECTORY);
        let requests = directory.join("requests");
        std::fs::create_dir_all(&requests).map_err(storage)?;
        let status = directory.join("status");
        std::fs::create_dir(&status).map_err(storage)?;
        let status = acyclic_fs::hold_root(&status).map_err(native_file_error)?;
        Ok(Self {
            workspace,
            directory,
            requests,
            status,
            _checkout: checkout,
        })
    }

    /// Checks original writer bytes and arbitrary bytes at deployed admission.
    pub fn endpoint(&self, input: &[u8]) -> Result<()> {
        let mut endpoint = Endpoint {
            version: 3,
            session: uuid::Uuid::from_bytes(identity(input)),
            workspace: self.workspace.clone(),
        };

        // These bytes use the SAME struct/serializer as serve_until's writer.
        // A reject-all decoder cannot satisfy representation invariance alone.
        let original = serde_json::to_vec(&endpoint).map_err(storage)?;
        let (_, admitted) = self.admit(&original)?;
        assert_eq!(admitted.session, endpoint.session);
        let escaped = String::from_utf8(original)
            .unwrap()
            .replace(r#""version":"#, r#""v\u0065rsion":"#)
            .replace(r#""session":"#, r#""sess\u0069on":"#)
            .replace(r#""workspace":"#, r#""worksp\u0061ce":"#);
        let (_, admitted) = self.admit(escaped.as_bytes())?;
        assert_eq!(admitted.session, endpoint.session);

        let mut version = [0; 4];
        let count = input.len().min(version.len());
        version[..count].copy_from_slice(&input[..count]);
        endpoint.version = u32::from_le_bytes(version);
        let versioned = serde_json::to_vec(&endpoint).map_err(storage)?;
        match self.admit(&versioned) {
            Ok(_) => assert_eq!(endpoint.version, 3),
            Err(Error::Unsupported(_)) => assert_ne!(endpoint.version, 3),
            Err(error) => panic!("descriptor version admission changed: {error:?}"),
        }

        endpoint.version = 3;
        endpoint.workspace.push("foreign");
        let foreign = serde_json::to_vec(&endpoint).map_err(storage)?;
        assert!(matches!(self.admit(&foreign), Err(Error::Unauthorized(_))));

        // Arbitrary bytes reach the original held-root read and decoder. Any
        // admitted descriptor must retain this workspace's exact authority.
        let raw = self.admit(input);
        if let Ok(value) = serde_json::from_slice::<Value>(input) {
            let canonical = serde_json::to_vec(&value).map_err(storage)?;
            // Value removes duplicate members. Only compare lossless parses.
            if serde_json::from_slice::<Endpoint>(input).is_ok() {
                assert_eq!(raw.is_ok(), self.admit(&canonical).is_ok());
            }
        }
        Ok(())
    }

    /// Checks identity, duplicate notification and withdrawal at actual admission.
    pub fn request(&self, input: &[u8]) -> Result<()> {
        let id = identity(input);
        let session = uuid::Uuid::from_bytes(id);
        let operation = OperationId::from_bytes(id);
        let mut foreign = id;
        foreign[0] ^= 1;
        let mut request = Request {
            session,
            operation,
            action: HostAction::Open {
                host: "codex".into(),
                session: session.to_string(),
            },
        };
        // Own wire bytes come from request_operation's original Request serde.
        let original = serde_json::to_vec(&request).map_err(storage)?;
        request.session = uuid::Uuid::from_bytes(foreign);
        let foreign_session = serde_json::to_vec(&request).map_err(storage)?;
        request.session = session;
        request.operation = OperationId::from_bytes(foreign);
        let foreign_operation = serde_json::to_vec(&request).map_err(storage)?;
        let leaf = OsString::from(format!("{operation}.json"));
        let path = self.requests.join(&leaf);
        let reply = reply_path(self.status.path(), operation);
        let schedule = input.first().copied().unwrap_or_default();
        for (index, bytes) in [
            original.as_slice(),
            original.as_slice(),
            foreign_session.as_slice(),
            foreign_operation.as_slice(),
            input,
        ]
        .into_iter()
        .enumerate()
        {
            // Keep the original happy path for every payload as well as the
            // input-selected withdrawal schedule in the second iteration.
            let schedule = if index == 0 { 0 } else { schedule };
            let mut served = Served::default();
            std::fs::write(&path, bytes).map_err(storage)?;
            for _ in 0..2 {
                served.observe(&self.requests, session, &leaf)?;
            }
            assert!(served.ready.len() <= 1, "one operation dispatches once");
            for admitted in &served.ready {
                assert_eq!(admitted.session, session);
                assert_eq!(admitted.operation, operation);
            }
            match index {
                0 | 1 => assert_eq!(served.ready.len(), 1, "original request must be admitted"),
                2 | 3 => assert!(served.ready.is_empty(), "foreign identity dispatched"),
                _ => {}
            }
            if schedule & 1 != 0 {
                remove_projection(&path)?;
                served.observe(&self.requests, session, &leaf)?;
                assert!(
                    served.start().is_empty(),
                    "withdrawn queued request dispatched"
                );
            } else {
                let started = served.start();
                if !started.is_empty() {
                    // Replacing writable bytes cannot re-admit accepted work.
                    std::fs::write(&path, input).map_err(storage)?;
                    served.observe(&self.requests, session, &leaf)?;
                    assert!(
                        served.start().is_empty(),
                        "accepted request dispatched again"
                    );
                    if schedule & 2 != 0 {
                        remove_projection(&path)?;
                        served.observe(&self.requests, session, &leaf)?;
                    }
                    // The file carrier must preserve the entire reply through
                    // the same encoder, opened-file reader and strict decoder.
                    let outcome = Value::String(String::from_utf8_lossy(input).into_owned());
                    served.answer(&self.status, operation, Ok(outcome.clone()))?;
                    assert_eq!(
                        reply.try_exists().map_err(storage)?,
                        schedule & 2 == 0,
                        "only an unwithdrawn dispatch may publish its reply"
                    );
                    if schedule & 2 == 0 {
                        assert_eq!(
                            crate::client::decode_command_result(
                                operation,
                                0,
                                &regular_bytes(&reply)?,
                            )?,
                            outcome,
                        );
                    }
                }
                remove_projection(&path)?;
                served.observe(&self.requests, session, &leaf)?;
            }
            assert!(served.known.is_empty(), "original operation did not retire");
            assert!(!reply.try_exists().map_err(storage)?);
        }
        Ok(())
    }

    fn admit(&self, bytes: &[u8]) -> Result<(PathBuf, Endpoint)> {
        std::fs::write(self.directory.join("owner.json"), bytes).map_err(storage)?;
        let result = discover(&self.workspace);
        if let Ok((directory, endpoint)) = &result {
            assert_eq!(directory, &self.directory);
            assert_eq!(endpoint.version, 3);
            assert_eq!(endpoint.workspace, self.workspace);
        }
        result
    }
}

fn identity(input: &[u8]) -> [u8; 16] {
    let mut identity = [0; 16];
    let count = input.len().min(identity.len());
    identity[..count].copy_from_slice(&input[..count]);
    identity
}
