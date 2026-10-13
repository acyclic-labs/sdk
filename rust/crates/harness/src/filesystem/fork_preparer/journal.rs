use super::*;
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AllocationClaim {
    pub(crate) operation_id: OperationId,
    pub(crate) preparation_digest: [u8; 32],
    pub(crate) parent: Authority,
    pub(crate) child: Authority,
    pub(crate) volume: VolumeRef,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SeedBinding {
    pub(super) digest: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CaptureAttempt {
    pub(crate) operation_id: OperationId,
    pub(super) selection: ForkSelection,
    pub(super) attempt: Uuid,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CaptureResult {
    pub(super) selection: ForkSelection,
    pub(super) capture: Capture,
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemForkPreparer<A, O> {
    pub(super) fn journal_ref(&self, request: &ForkRequest) -> Result<WorkspaceRef> {
        workspace_ref(
            self.host.provider.clone(),
            &format!("harness-fork-preparation-{}", request.operation_id),
        )
    }
    pub(super) async fn read_claim(
        &self,
        journal: &WorkspaceRef,
        request: &ForkRequest,
    ) -> Result<bool> {
        match read_record::<A, O, ForkRequest>(
            &self.host,
            journal,
            "/request.json",
            MAX_REQUEST_BYTES,
        )
        .await?
        {
            Some(prior) => exact(prior, request).map(|_| true),
            None => Ok(false),
        }
    }
    pub(super) async fn claim(&self, request: &ForkRequest) -> Result<WorkspaceRef> {
        let journal = self.journal_ref(request)?;
        let name = std::str::from_utf8(journal.as_resource().key())
            .map_err(|_| Error::Invalid("fork journal key is not UTF-8".into()))?;
        self.host
            .filesystem
            .create_workspace(
                name,
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .await
            .map_err(map_error)?;
        let key = IdempotencyKey::new(format!("fork:{}:claim", request.operation_id))?;
        exact(
            persist(
                &self.host,
                &journal,
                "/request.json",
                MAX_REQUEST_BYTES,
                request,
                &key,
                request.operation_id,
            )
            .await?,
            request,
        )?;
        Ok(journal)
    }
    pub(super) async fn read_report(
        &self,
        journal: &WorkspaceRef,
        request: &ForkRequest,
    ) -> Result<Option<ForkReport>> {
        let report =
            read_record::<A, O, ForkReport>(&self.host, journal, "/report.json", MAX_REPORT_BYTES)
                .await?;
        if let Some(report) = &report {
            report.validate(&mut None)?;
            exact(&report.request, &request)?;
        }
        Ok(report)
    }
    pub(super) async fn commit_report(
        &self,
        journal: &WorkspaceRef,
        report: &ForkReport,
    ) -> Result<ForkReport> {
        let key = IdempotencyKey::new(format!("fork:{}:report", report.request.operation_id))?;
        exact(
            persist(
                &self.host,
                journal,
                "/report.json",
                MAX_REPORT_BYTES,
                report,
                &key,
                report.request.operation_id,
            )
            .await?,
            report,
        )
    }
    pub(super) async fn capture_result(
        &self,
        journal: &WorkspaceRef,
        request: &ForkRequest,
        index: usize,
        selection: &ForkSelection,
    ) -> Result<Option<Capture>> {
        let result = read_record::<A, O, CaptureResult>(
            &self.host,
            journal,
            &format!("/capture-{index}-result.json"),
            MAX_CAPTURE_BYTES,
        )
        .await?;
        if let Some(result) = result {
            exact(&result.selection, &selection)?;
            let attempt = read_record::<A, O, CaptureAttempt>(
                &self.host,
                journal,
                &format!("/capture-{index}-attempt.json"),
                MAX_CAPTURE_BYTES,
            )
            .await?
            .ok_or_else(|| Error::Storage("capture result has no durable start".into()))?;
            exact(attempt.operation_id, &request.operation_id)?;
            exact(&attempt.selection, &selection)?;
            validate_capture(selection, &result.capture)?;
            Ok(Some(result.capture))
        } else {
            Ok(None)
        }
    }
    pub(super) async fn start_capture(
        &self,
        journal: &WorkspaceRef,
        request: &ForkRequest,
        index: usize,
        selection: &ForkSelection,
    ) -> Result<bool> {
        let attempt = CaptureAttempt {
            operation_id: request.operation_id,
            selection: selection.clone(),
            attempt: Uuid::new_v4(),
        };
        let key = IdempotencyKey::new(format!(
            "fork:{}:capture-attempt:{index}:{}",
            request.operation_id, attempt.attempt
        ))?;
        let prior = persist(
            &self.host,
            journal,
            &format!("/capture-{index}-attempt.json"),
            MAX_CAPTURE_BYTES,
            &attempt,
            &key,
            request.operation_id,
        )
        .await?;
        exact(prior.operation_id, &request.operation_id)?;
        exact(&prior.selection, &selection)?;
        Ok(prior == attempt)
    }
    pub(super) async fn commit_capture_result(
        &self,
        journal: &WorkspaceRef,
        request: &ForkRequest,
        index: usize,
        selection: &ForkSelection,
        capture: Capture,
    ) -> Result<Capture> {
        validate_capture(selection, &capture)?;
        let key = IdempotencyKey::new(format!(
            "fork:{}:capture-result:{index}",
            request.operation_id
        ))?;
        let result = CaptureResult {
            selection: selection.clone(),
            capture,
        };
        exact(
            persist(
                &self.host,
                journal,
                &format!("/capture-{index}-result.json"),
                MAX_CAPTURE_BYTES,
                &result,
                &key,
                request.operation_id,
            )
            .await?,
            &result,
        )?;
        self.capture_result(journal, request, index, selection)
            .await?
            .ok_or(Error::Indeterminate(request.operation_id))
    }
}
pub(super) fn exact<T: PartialEq>(actual: T, expected: &T) -> Result<T> {
    if &actual == expected {
        Ok(actual)
    } else {
        Err(Error::Conflict("fork journal identity differs".into()))
    }
}
// Read-back selects the durable CAS winner, including after a lost reply.
pub(super) async fn persist<
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    T: Serialize + DeserializeOwned,
>(
    host: &FilesystemHost<A, O>,
    journal: &WorkspaceRef,
    path: &str,
    maximum: u64,
    value: &T,
    key: &IdempotencyKey,
    operation: OperationId,
) -> Result<T> {
    if let Some(prior) = read_record(host, journal, path, maximum).await? {
        return Ok(prior);
    }
    let observed = host.resolve(journal).await?;
    let bytes = encode_record(value, maximum)?;
    match host
        .apply(
            journal,
            Some(&observed.generation),
            &[WorkspaceMutation::PutFile {
                path: path.into(),
                bytes: bytes.into(),
            }],
            key,
        )
        .await
    {
        Ok(_) | Err(Error::Conflict(_)) => {}
        Err(error) => return Err(error),
    }
    read_record(host, journal, path, maximum)
        .await?
        .ok_or(Error::Indeterminate(operation))
}
pub(super) fn seed_binding(seed: &ForkSeed) -> Result<SeedBinding> {
    seed.validate(&mut None)?;
    Ok(SeedBinding {
        digest: *blake3::hash(&encode_record(seed, MAX_REPORT_BYTES)?).as_bytes(),
    })
}

pub(super) fn validate_capture(selection: &ForkSelection, capture: &Capture) -> Result<()> {
    match capture {
        Capture::Captured(resource) if resource.source == selection.revision => {
            resource.validate(&mut None)
        }
        Capture::Captured(_) => Err(Error::Invalid(
            "capture provider substituted another source revision".into(),
        )),
        Capture::Unsupported(reason) if !reason.is_empty() => Ok(()),
        Capture::Unsupported(_) => Err(Error::Invalid("fork capture reason is invalid".into())),
        Capture::InFlight(operation) | Capture::Indeterminate(operation) => {
            Err(Error::Indeterminate(*operation))
        }
    }
}
pub(super) fn encode_record<T: Serialize>(value: &T, maximum_bytes: u64) -> Result<Vec<u8>> {
    let bytes = serde_json::to_vec(value).map_err(|error| Error::Invalid(error.to_string()))?;
    if bytes.len() as u64 > maximum_bytes {
        return Err(Error::Invalid(
            "fork journal record exceeds byte limit".into(),
        ));
    }
    Ok(bytes)
}

pub(super) async fn read_record<
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    T: DeserializeOwned,
>(
    host: &FilesystemHost<A, O>,
    journal: &WorkspaceRef,
    path: &str,
    maximum_bytes: u64,
) -> Result<Option<T>> {
    match host.read(journal, None, path, maximum_bytes).await {
        Ok(bytes) => crate::contract::json_from_slice(&bytes)
            .map(Some)
            .map_err(|error| Error::Invalid(format!("fork journal record is invalid: {error}"))),
        Err(Error::NotFound(_)) => Ok(None),
        Err(error) => Err(error),
    }
}
