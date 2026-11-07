//! The existing pool serializes slot enrollment, withdrawal and reuse.
use super::*;

const WORDS: usize = super::super::DEFAULT_CHANNELS.div_ceil(64);
const GROUPS: usize = WORDS.div_ceil(64);
const _: () = assert!(GROUPS <= 64);

pub(super) struct Slots {
    free: [u64; WORDS],
    groups: [u64; GROUPS],
    head: u64,
    owned: [u64; WORDS],
}

impl Slots {
    pub(super) fn new() -> Self {
        let mut slots = Self {
            free: [0; WORDS],
            groups: [0; GROUPS],
            head: 0,
            owned: [0; WORDS],
        };
        for slot in 0..super::super::DEFAULT_CHANNELS {
            slots.available(slot as u32, true);
        }
        slots
    }

    pub(super) fn available(&mut self, slot: u32, free: bool) {
        let word = slot as usize / 64;
        let bit = 1_u64 << (slot % 64);
        if free {
            self.free[word] |= bit;
        } else {
            self.free[word] &= !bit;
        }
        let group = word / 64;
        let bit = 1_u64 << (word % 64);
        if self.free[word] != 0 {
            self.groups[group] |= bit;
        } else {
            self.groups[group] &= !bit;
        }
        let bit = 1_u64 << group;
        if self.groups[group] != 0 {
            self.head |= bit;
        } else {
            self.head &= !bit;
        }
    }

    pub(super) fn owned(&self, slot: u32) -> bool {
        self.owned[slot as usize / 64] & (1_u64 << (slot % 64)) != 0
    }

    pub(super) fn allocate(&mut self) -> Result<u32, StreamError> {
        if self.head == 0 {
            return Err(StreamError::Capacity);
        }
        let group = self.head.trailing_zeros() as usize;
        let word = group * 64 + self.groups[group].trailing_zeros() as usize;
        let slot = (word * 64 + self.free[word].trailing_zeros() as usize) as u32;
        self.available(slot, false);
        self.owned[word] |= 1_u64 << (slot % 64);
        Ok(slot)
    }

    pub(super) fn release(&mut self, slot: u32) {
        self.owned[slot as usize / 64] &= !(1_u64 << (slot % 64));
        self.available(slot, true);
    }
}

/// Queue credit is reserved before enrollment, so the last clone can withdraw
/// even when the command queue is full. Only the pool can free the slot, after
/// offering withdrawal on its original links and retiring its reconnect value.
pub(super) struct Owner {
    slot: u32,
    withdrawal: Option<mpsc::OwnedPermit<Command>>,
}

impl Drop for Owner {
    fn drop(&mut self) {
        if let Some(withdrawal) = self.withdrawal.take() {
            withdrawal.send(Command::ReleaseCredential { slot: self.slot });
        }
    }
}

impl Client {
    /// Enroll a credential on this pool for the lifetime of the returned client
    /// and its clones. The returned slot names it on requests. Dropping the last
    /// clone withdraws it before reuse; no transport or task is created.
    ///
    /// Queue credit and physical session slots return `Capacity` when exhausted.
    /// Admission waits only for this pool's reply; losing that producer returns
    /// `Unavailable`. Explicit registration must not overwrite a live owned slot.
    #[inline(never)]
    pub fn with_credential(
        &self,
        credential: Bytes,
    ) -> BoxProviderFuture<'_, Result<(Self, u32), StreamError>> {
        Box::pin(async move {
            if credential.is_empty() {
                return Err(StreamError::InvalidArgument);
            }
            let withdrawal = self
                .pool
                .commands
                .clone()
                .try_reserve_owned()
                .map_err(queue_error)?;
            let (ready, admitted) = oneshot::channel();
            self.pool
                .commands
                .try_send(Command::AcquireCredential {
                    credential,
                    withdrawal,
                    ready,
                })
                .map_err(queue_error)?;
            let (slot, owner) = admitted.await.map_err(|_| StreamError::Unavailable)??;
            let mut client = self.clone();
            client.credential_owner = Some(owner);
            Ok((client, slot))
        })
    }
}

pub(super) fn queue_error<T>(error: mpsc::error::TrySendError<T>) -> StreamError {
    match error {
        mpsc::error::TrySendError::Full(_) => StreamError::Capacity,
        mpsc::error::TrySendError::Closed(_) => StreamError::Unavailable,
    }
}

impl Pool {
    pub(super) fn install_credential(
        &mut self,
        slot: u32,
        credential: Bytes,
    ) -> Result<(), StreamError> {
        let total = self
            .credential_bytes
            .checked_sub(self.credentials.get(&slot).map_or(0, Bytes::len))
            .and_then(|current| current.checked_add(credential.len()))
            .filter(|total| *total <= crate::MAX_COMMAND_BYTES)
            .ok_or(StreamError::Capacity)?;
        let encoded = frame(
            0,
            0,
            Body::Credential(wire::SessionCredential {
                slot,
                credential: credential.clone(),
            }),
        );
        if credential.is_empty() {
            self.credentials.remove(&slot);
        } else {
            self.credentials.insert(slot, credential);
        }
        self.credential_bytes = total;
        let nodes = self.links.keys().cloned().collect::<Vec<_>>();
        for node in nodes {
            self.send(&node, encoded.clone())?;
        }
        Ok(())
    }

    pub(super) fn acquire_credential(
        &mut self,
        credential: Bytes,
        withdrawal: mpsc::OwnedPermit<Command>,
        ready: oneshot::Sender<Result<(u32, Arc<Owner>), StreamError>>,
    ) -> Result<(), StreamError> {
        if ready.is_closed() {
            return Ok(());
        }
        let result = self.credential_slots.allocate().and_then(|slot| {
            let encoded = frame(
                0,
                0,
                Body::Credential(wire::SessionCredential {
                    slot,
                    credential: credential.clone(),
                }),
            );
            let installed = if encoded.encoded_len() > crate::MAX_COMMAND_BYTES {
                Err(StreamError::LimitExceeded)
            } else {
                self.install_credential(slot, credential)
            };
            match installed {
                Ok(()) => Ok((
                    slot,
                    Arc::new(Owner {
                        slot,
                        withdrawal: Some(withdrawal),
                    }),
                )),
                Err(error) => {
                    self.credential_slots.release(slot);
                    Err(error)
                }
            }
        });
        // Capacity/size rejection happens before installation. Other failures
        // fence the pool, including any already offered physical registration.
        let failure = result
            .as_ref()
            .err()
            .filter(|error| !matches!(error, StreamError::Capacity | StreamError::LimitExceeded))
            .cloned();
        let _ = ready.send(result);
        match failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

#[cfg(all(test, not(loom), feature = "grpc", not(target_arch = "wasm32")))]
#[path = "credentials/tests.rs"]
mod tests;
