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
        for slot in 0..crate::width::saturate::<u32, _>(super::super::DEFAULT_CHANNELS) {
            slots.available(slot, true);
        }
        slots
    }

    /// A slot's word and its bit in that word; `None` past the last slot,
    /// which no allocation returns.
    fn position(slot: u32) -> Option<(usize, u64)> {
        let word = usize::try_from(slot / 64)
            .ok()
            .filter(|word| *word < WORDS)?;
        Some((word, 1_u64 << (slot % 64)))
    }

    pub(super) fn available(&mut self, slot: u32, free: bool) {
        let Some((word, bit)) = Self::position(slot) else {
            return;
        };
        let (Some(bits), Some(group_bits)) =
            (self.free.get_mut(word), self.groups.get_mut(word / 64))
        else {
            return;
        };
        if free {
            *bits |= bit;
        } else {
            *bits &= !bit;
        }
        let word_bit = 1_u64 << (word % 64);
        if *bits == 0 {
            *group_bits &= !word_bit;
        } else {
            *group_bits |= word_bit;
        }
        let group_bit = 1_u64 << (word / 64);
        if *group_bits == 0 {
            self.head &= !group_bit;
        } else {
            self.head |= group_bit;
        }
    }

    pub(super) fn owned(&self, slot: u32) -> bool {
        Self::position(slot)
            .is_some_and(|(word, bit)| self.owned.get(word).is_some_and(|bits| bits & bit != 0))
    }

    pub(super) fn allocate(&mut self) -> Result<u32, StreamError> {
        if self.head == 0 {
            return Err(StreamError::Capacity);
        }
        // Each nonzero summary bit names a nonzero word below it, so every
        // lookup is in range; fewer than 64^3 slots fit in a `u32`.
        let group = self.head.trailing_zeros();
        let groups = self
            .groups
            .get(group as usize)
            .ok_or(StreamError::Capacity)?;
        let word = group * 64 + groups.trailing_zeros();
        let free = self.free.get(word as usize).ok_or(StreamError::Capacity)?;
        let slot = word * 64 + free.trailing_zeros();
        self.available(slot, false);
        if let Some(owned) = self.owned.get_mut(word as usize) {
            *owned |= 1_u64 << (slot % 64);
        }
        Ok(slot)
    }

    pub(super) fn release(&mut self, slot: u32) {
        if let Some((word, bit)) = Self::position(slot)
            && let Some(owned) = self.owned.get_mut(word)
        {
            *owned &= !bit;
        }
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
    /// Retain the original slot owner until renewal is dispatched. An owned
    /// slot cannot pass through explicit registration or be reused meanwhile.
    pub(super) fn renew_credential(
        &self,
        slot: u32,
        credential: Bytes,
    ) -> Result<oneshot::Receiver<Result<(), StreamError>>, StreamError> {
        if slot as usize >= Limits::default().channels {
            return Err(StreamError::Capacity);
        }
        let owner = self
            .credential_owner
            .as_ref()
            .filter(|owner| owner.slot == slot)
            .cloned();
        let value = frame(
            0,
            0,
            Body::Credential(wire::SessionCredential {
                slot,
                credential: credential.clone(),
            }),
        );
        if value.encoded_len() > crate::MAX_COMMAND_BYTES {
            return Err(StreamError::LimitExceeded);
        }
        let (sent, result) = oneshot::channel();
        self.pool
            .commands
            .try_send(Command::RenewCredential {
                slot,
                owner,
                credential,
                sent,
            })
            .map_err(|error| queue_error(&error))?;
        Ok(result)
    }

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
                .map_err(|error| queue_error(&error))?;
            let (ready, admitted) = oneshot::channel();
            self.pool
                .commands
                .try_send(Command::AcquireCredential {
                    credential,
                    withdrawal,
                    ready,
                })
                .map_err(|error| queue_error(&error))?;
            let (slot, owner) = admitted.await.map_err(|_| StreamError::Unavailable)??;
            let mut client = self.clone();
            client.credential_owner = Some(owner);
            Ok((client, slot))
        })
    }
}

/// The stream error for a command the pool queue cannot take now.
pub(super) fn queue_error<T>(error: &mpsc::error::TrySendError<T>) -> StreamError {
    match error {
        mpsc::error::TrySendError::Full(_) => StreamError::Capacity,
        mpsc::error::TrySendError::Closed(_) => StreamError::Unavailable,
    }
}

impl Pool {
    pub(super) fn renew_credential(
        &mut self,
        slot: u32,
        owner: Option<&Owner>,
        credential: Bytes,
        sent: oneshot::Sender<Result<(), StreamError>>,
    ) -> Result<(), StreamError> {
        let owned = self.credential_slots.owned(slot);
        let admitted = match owner {
            Some(owner) => owned && owner.slot == slot,
            None => !owned,
        };
        let empty = credential.is_empty();
        let result = if admitted {
            self.install_credential(slot, credential)
        } else {
            Err(StreamError::AccessDenied)
        };
        if result.is_ok() && owner.is_none() {
            self.credential_slots.available(slot, empty);
        }
        // A command-local refusal must not fail unrelated channels. A failed
        // physical send still fences the pool, as enrollment does.
        let failure = result
            .as_ref()
            .err()
            .filter(|error| {
                !matches!(
                    error,
                    StreamError::AccessDenied | StreamError::Capacity | StreamError::LimitExceeded
                )
            })
            .cloned();
        let _ = sent.send(result);
        match failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

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
