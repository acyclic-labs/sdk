use super::{Checkpoint, CheckpointCodec, GuestError};

/// Installed update boundary. Hosts publish this as one fenced operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GuestInstallation<Configuration, Subscriptions> {
    /// Current artifact and binding configuration.
    pub configuration: Configuration,
    /// Retained checkpoint, unchanged until a new capture commits.
    pub checkpoint: Option<Checkpoint>,
    /// Current subscription specifications and progress.
    pub subscriptions: Subscriptions,
}

impl<Configuration, Subscriptions> GuestInstallation<Configuration, Subscriptions> {
    /// Admit and decode a candidate before replacing the installed boundary.
    /// Errors preserve configuration, checkpoint and subscriptions exactly.
    pub fn update<State>(
        &mut self,
        configuration: Configuration,
        subscriptions: Subscriptions,
        codec: &CheckpointCodec<State>,
    ) -> Result<Option<State>, GuestError> {
        let state = prepare_update(self.checkpoint.as_ref(), codec)?;
        self.configuration = configuration;
        self.subscriptions = subscriptions;
        Ok(state)
    }
}

/// Validate an update before changing any configuration, checkpoint or subscription.
/// The caller publishes the complete candidate only after this returns successfully.
/// A rejection cannot mutate the prior configuration or retained checkpoint.
pub fn prepare_update<State>(
    checkpoint: Option<&Checkpoint>,
    candidate: &CheckpointCodec<State>,
) -> Result<Option<State>, GuestError> {
    if candidate.identity.name.is_empty()
        || candidate.identity.version.is_empty()
        || !candidate.readable.contains(&candidate.identity)
    {
        return Err(GuestError::IncompatibleCodec);
    }
    checkpoint
        .map(|checkpoint| {
            if !candidate.readable.contains(&checkpoint.codec) {
                return Err(GuestError::IncompatibleCodec);
            }
            (candidate.decode)(checkpoint)
        })
        .transpose()
}
