fn authorization(api_key: &str) -> Result<MetadataValue<Ascii>, Error> {
    acyclic_auth::credential::bearer_metadata(api_key)
        .map_err(|_| Error::Invalid("invalid API key"))
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
        contract::watch_run_event(&mut self.state, &event)?;
        Ok(Some(event))
    }
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
