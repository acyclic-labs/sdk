//! The existing pool serializes slot enrollment, withdrawal and reuse.
use super::*;
use crate::StreamCategory;

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
        let mut free = [u64::MAX; WORDS];
        let remainder = super::super::DEFAULT_CHANNELS % 64;
        if remainder != 0
            && let Some(last) = free.last_mut()
        {
            *last = (1_u64 << remainder) - 1;
        }
        let mut groups = [u64::MAX; GROUPS];
        let remainder = WORDS % 64;
        if remainder != 0
            && let Some(last) = groups.last_mut()
        {
            *last = (1_u64 << remainder) - 1;
        }
        let head = if GROUPS == 64 {
            u64::MAX
        } else {
            (1_u64 << GROUPS) - 1
        };
        Self {
            free,
            groups,
            head,
            owned: [0; WORDS],
        }
    }

    pub(super) fn available(&mut self, slot: u32, free: bool) -> Result<(), StreamError> {
        let word = usize::try_from(slot).map_err(|_| StreamError::InvalidArgument)? / 64;
        let bit = 1_u64 << (slot % 64);
        let word_free = self
            .free
            .get_mut(word)
            .ok_or(StreamError::InvalidArgument)?;
        if free {
            *word_free |= bit;
        } else {
            *word_free &= !bit;
        }
        let group = word / 64;
        let group_bits = self
            .groups
            .get_mut(group)
            .ok_or(StreamError::InvalidArgument)?;
        let bit = 1_u64 << (word % 64);
        if *word_free != 0 {
            *group_bits |= bit;
        } else {
            *group_bits &= !bit;
        }
        let bit = 1_u64 << group;
        if *group_bits != 0 {
            self.head |= bit;
        } else {
            self.head &= !bit;
        }
        Ok(())
    }

    pub(super) fn owned(&self, slot: u32) -> Result<bool, StreamError> {
        let word = usize::try_from(slot).map_err(|_| StreamError::InvalidArgument)? / 64;
        self.owned
            .get(word)
            .map(|word| word & (1_u64 << (slot % 64)) != 0)
            .ok_or(StreamError::InvalidArgument)
    }

    pub(super) fn allocate(&mut self) -> Result<u32, StreamError> {
        if self.head == 0 {
            return Err(StreamError::Capacity);
        }
        let group = self.head.trailing_zeros();
        let group_bits = self
            .groups
            .get(usize::try_from(group).map_err(|_| StreamError::Capacity)?)
            .ok_or(StreamError::Capacity)?;
        let word = usize::try_from(group).map_err(|_| StreamError::Capacity)? * 64
            + usize::try_from(group_bits.trailing_zeros()).map_err(|_| StreamError::Capacity)?;
        let free = self.free.get(word).ok_or(StreamError::Capacity)?;
        let slot = word * 64
            + usize::try_from(free.trailing_zeros()).map_err(|_| StreamError::Capacity)?;
        let slot = u32::try_from(slot).map_err(|_| StreamError::Capacity)?;
        self.available(slot, false)?;
        let word = usize::try_from(slot).map_err(|_| StreamError::Capacity)? / 64;
        let owned = self.owned.get_mut(word).ok_or(StreamError::Capacity)?;
        *owned |= 1_u64 << (slot % 64);
        Ok(slot)
    }

    pub(super) fn release(&mut self, slot: u32) -> Result<(), StreamError> {
        let word = usize::try_from(slot).map_err(|_| StreamError::InvalidArgument)? / 64;
        let owned = self
            .owned
            .get_mut(word)
            .ok_or(StreamError::InvalidArgument)?;
        *owned &= !(1_u64 << (slot % 64));
        self.available(slot, true)
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

impl Pool {
    pub(super) fn renew_credential(
        &mut self,
        slot: u32,
        owner: Option<&Arc<Owner>>,
        credential: Bytes,
        sent: oneshot::Sender<Result<(), StreamError>>,
    ) -> Result<(), StreamError> {
        let owned = self.credential_slots.owned(slot)?;
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
            self.credential_slots.available(slot, empty)?;
        }
        // A command-local refusal must not fail unrelated channels. A failed
        // physical send still fences the pool, as enrollment does.
        let failure = result
            .as_ref()
            .err()
            .filter(|error| {
                !matches!(
                    error.category(),
                    StreamCategory::AccessDenied
                        | StreamCategory::Capacity
                        | StreamCategory::LimitExceeded
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
                    self.credential_slots.release(slot)?;
                    Err(error)
                }
            }
        });
        // Capacity/size rejection happens before installation. Other failures
        // fence the pool, including any already offered physical registration.
        let failure = result
            .as_ref()
            .err()
            .filter(|error| {
                !matches!(
                    error.category(),
                    StreamCategory::Capacity | StreamCategory::LimitExceeded
                )
            })
            .cloned();
        let _ = ready.send(result);
        match failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}
