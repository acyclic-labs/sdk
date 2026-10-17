fn authorization(api_key: &str) -> Result<Zeroizing<String>, Error> {
    if api_key.is_empty() || api_key.len() > 8_192 {
        return Err(Error::Invalid("invalid API key"));
    }
    let value = Zeroizing::new(format!("Bearer {api_key}"));
    MetadataValue::<Ascii>::try_from(value.as_str())
        .map_err(|_| Error::Invalid("invalid API key"))?;
    Ok(value)
}

fn nonzero<const N: usize>(value: &[u8; N]) -> Result<(), Error> {
    if *value == [0; N] {
        return Err(Error::Invalid("zero identity"));
    }
    Ok(())
}

fn fixed<const N: usize>(value: &[u8]) -> Result<[u8; N], Error> {
    let bytes = value
        .try_into()
        .map_err(|_| Error::Invalid("identity length differs"))?;
    nonzero(&bytes)?;
    Ok(bytes)
}

fn bounded<M: prost::Message>(message: &M) -> Result<(), Error> {
    if message.encoded_len() > MAXIMUM_MESSAGE_BYTES {
        return Err(Error::Invalid("message exceeds transport ceiling"));
    }
    Ok(())
}

/// Explicit customer warm-retention policy. Distribution and capacity remain
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Retention {
    latency_profile: [u8; 32],
    expires_at_ms: u64,
    idle_kv: Option<([u8; 32], u64)>,
}

impl Retention {
    /// Request an admitted warm promise through an exact published latency profile.
    #[must_use]
    pub const fn warm_until(latency_profile: [u8; 32], expires_at_ms: u64) -> Self {
        Self {
            latency_profile,
            expires_at_ms,
            idle_kv: None,
        }
    }

    /// Paid KV pin with a positive idle timeout; no capacity or latency guarantee.
    #[must_use]
    pub const fn idle_kv(profile: [u8; 32], idle_timeout_ms: u64) -> Self {
        Self {
            latency_profile: [0; 32],
            expires_at_ms: 0,
            idle_kv: Some((profile, idle_timeout_ms)),
        }
    }
}

/// Validating event-stream observation. Dropping it never cancels the Run.
pub struct RunEvents {
    stream: Option<tonic::Streaming<wire::RunEvent>>,
    state: contract::WatchRunState,
}

impl RunEvents {
    /// Read and validate the next ordered event.
    pub async fn next(&mut self) -> Result<Option<wire::RunEvent>, Error> {
        let Some(stream) = self.stream.as_mut() else {
            return Ok(None);
        };
        let Some(event) = stream.message().await? else {
            self.state.finish().map_err(Error::Invalid)?;
            return Ok(None);
        };
        contract::watch_run_event(&mut self.state, &event).map_err(contract_error)?;
        Ok(Some(event))
    }
}

fn contract_error(error: contract::Error) -> Error {
    Error::Invalid(error.message())
}

fn validate_evaluation_spec(spec: &wire::EvaluationSpec) -> Result<(), Error> {
    contract::validate_evaluation_spec(spec).map_err(contract_error)
}

fn validate_evaluation_admission(
    view: &wire::EvaluationView,
    expected: [u8; 16],
    spec: &wire::EvaluationSpec,
) -> Result<(), Error> {
    contract::validate_evaluation_admission(view, expected, spec).map_err(contract_error)
}

fn validate_evaluation_view(view: &wire::EvaluationView, expected: [u8; 16]) -> Result<(), Error> {
    contract::validate_evaluation_view(view, expected).map_err(contract_error)
}

fn validate_run_view(view: &wire::RunView, expected: [u8; 16]) -> Result<(), Error> {
    contract::validate_run_view(view, expected).map_err(contract_error)
}

fn validate_context_view(view: &wire::ContextView, expected: [u8; 32]) -> Result<(), Error> {
    contract::validate_context_view(view, expected).map_err(contract_error)
}

fn validate_warm_view(
    view: &wire::WarmView,
    expected_context: Option<[u8; 32]>,
    expected_commitment: Option<[u8; 32]>,
) -> Result<(), Error> {
    contract::validate_warm_view(view, expected_context, expected_commitment)
        .map_err(contract_error)
}

fn validate_receipt(receipt: &wire::MutationReceipt) -> Result<(), Error> {
    contract::validate_receipt(receipt).map_err(contract_error)
}

/// Allocates a fresh item identity before its exact text bytes are dispatched.
pub fn text_item(kind: wire::ItemKind, text: String) -> wire::Item {
    wire::Item {
        id: uuid::Uuid::now_v7().as_bytes().to_vec(),
        kind: kind as i32,
        payload: text.into_bytes(),
        link: Vec::new(),
        continuation_profile: Vec::new(),
    }
}
