//! Conditional ownership of a retained intent's terminal fence.
//!
//! Every fenced retain and release changes the same medium key. The original
//! observation is consumed by the conditional commit, so a release that wins
//! cannot be crossed by an older retain, even across independent stores.
use super::*;

const MAGIC: &[u8] = b"acyclic.objects.terminal-fence.v1\0";

#[derive(Clone, Copy)]
struct Fence {
    terminal: Option<u64>,
    nonce: [u8; 16],
}

impl Fence {
    fn encode(self) -> OwnedBytes {
        let mut bytes = Vec::with_capacity(MAGIC.len() + 25);
        bytes.extend_from_slice(MAGIC);
        bytes.push(u8::from(self.terminal.is_some()));
        bytes.extend_from_slice(&self.terminal.unwrap_or_default().to_be_bytes());
        bytes.extend_from_slice(&self.nonce);
        bytes.into()
    }

    fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let bytes = bytes.strip_prefix(MAGIC).ok_or_else(corrupt)?;
        let [present, rest @ ..] = bytes else {
            return Err(corrupt());
        };
        let (number, nonce) = rest.split_at_checked(8).ok_or_else(corrupt)?;
        let number = u64::from_be_bytes(number.try_into().map_err(|_| corrupt())?);
        let terminal = match present {
            0 if number == 0 => None,
            1 => Some(number),
            _ => return Err(corrupt()),
        };
        Ok(Self {
            terminal,
            nonce: nonce.try_into().map_err(|_| corrupt())?,
        })
    }

    fn require_open(self, fence: u64) -> Result<(), Error> {
        if self.terminal.is_some_and(|terminal| fence <= terminal) {
            return Err(ErrorCode::TerminalFence.into());
        }
        Ok(())
    }
}

impl<M: Medium, P: Deref<Target = M> + ProviderPlatform> Account<M, P> {
    fn terminal_key(&self, root_hash: &str) -> String {
        format!("{}t/{root_hash}", self.state.prefix)
    }

    async fn terminal_observed(&self, key: &str) -> Result<(Fence, Observed<'_>), Error> {
        let mut observed = self.medium.observe(key.to_owned()).await.map_err(medium)?;
        if observed.is_none() {
            let initial = Fence {
                terminal: None,
                nonce: version()?,
            };
            self.medium
                .create(key, initial.encode())
                .await
                .map_err(moved)?;
            observed = self.medium.observe(key.to_owned()).await.map_err(medium)?;
        }
        let observed = observed.ok_or_else(changed)?;
        Ok((Fence::decode(&observed.bytes)?, observed))
    }

    async fn terminal_replace(
        &self,
        observed: Observed<'_>,
        terminal: Option<u64>,
    ) -> Result<bool, Error> {
        let next = Fence {
            terminal,
            nonce: version()?,
        };
        Ok(self
            .medium
            .replace(observed.observation, next.encode())
            .await
            .map_err(moved)?
            .is_some())
    }

    async fn remove_fenced_batch(
        &self,
        key: &str,
        root: &str,
        name: &str,
        references: &[Digest],
    ) -> Result<bool, Error> {
        // This batch is terminal or its admission lost the conditional commit.
        // Keep it as a cleanup receipt until its index removals are durable,
        // so interruption cannot leave a permanent orphan pin.
        let tag = Self::batch_tag("f", root, name);
        let mut indexes = Vec::with_capacity(references.len());
        self.unreference(references, &tag, Some(&mut indexes))
            .await?;
        self.medium.flush(&indexes).await.map_err(medium)?;
        let removed = self.medium.remove(key).await.map_err(moved)?;
        self.medium.flush(&[key.to_owned()]).await.map_err(medium)?;
        Ok(removed)
    }

    pub(super) async fn fenced_rooted(&self) -> Result<Vec<Digest>, Error> {
        let mut references = Vec::new();
        let mut pages = self.pages(format!("{}f/", self.state.prefix));
        while let Some(page) = pages.next().await? {
            for (key, _) in page {
                let (root, name) = key
                    .strip_prefix(&format!("{}f/", self.state.prefix))
                    .and_then(|key| key.split_once('/'))
                    .ok_or_else(corrupt)?;
                let encoded = name.split('.').next().ok_or_else(corrupt)?;
                let fence = u64::from_str_radix(encoded, 16).map_err(|_| corrupt())?;
                let terminal_key = self.terminal_key(root);
                let terminal = self
                    .medium
                    .read(&terminal_key, None)
                    .await
                    .map_err(medium)?;
                let closed = terminal
                    .as_ref()
                    .map(|bytes| Fence::decode(bytes))
                    .transpose()?
                    .and_then(|state| state.terminal)
                    .is_some_and(|terminal| fence <= terminal);
                let Some(batch) = self.batch(&key).await? else {
                    continue;
                };
                if closed {
                    // A visible terminal record must be durable before its
                    // batches or their child indexes can be removed.
                    self.medium.flush(&[terminal_key]).await.map_err(medium)?;
                    self.remove_fenced_batch(&key, root, name, &batch).await?;
                } else {
                    references.extend(batch);
                }
            }
        }
        Ok(references)
    }

    /// Retains one batch under the exact observed terminal fence.
    pub async fn retain_fenced(
        &self,
        root: &RootId,
        fence: u64,
        references: Vec<Digest>,
    ) -> Result<Retained, Error> {
        let references = canonical(references);
        if references.is_empty() {
            return Err(invalid());
        }
        let issued = self.issued().await?;
        let _gate = self.state.gate.read().await;
        let encoded = Digest::of(root.as_bytes()).encoded_hex();
        let root_hash = encoded.as_ref();
        let terminal_key = self.terminal_key(root_hash);
        let (state, observed) = self.terminal_observed(&terminal_key).await?;
        state.require_open(fence)?;
        self.medium
            .allocate(references.len() as u64 * 32)
            .map_err(medium)?;
        let batch = encode_batch(&references);
        let name = format!("{fence:016x}.{}.{}", Digest::of(&batch), nonce()?);
        let tag = Self::batch_tag("f", root_hash, &name);
        let key = self.retention_key('f', root_hash, &name);
        self.commit_retention(&references, &tag, &key, batch, 2)
            .await?;
        if !self.terminal_replace(observed, state.terminal).await? {
            self.remove_fenced_batch(&key, root_hash, &name, &references)
                .await?;
            let (current, _) = self.terminal_observed(&terminal_key).await?;
            current.require_open(fence)?;
            return Err(changed());
        }
        self.medium
            .flush(std::slice::from_ref(&terminal_key))
            .await
            .map_err(medium)?;
        let (current, _) = self.terminal_observed(&terminal_key).await?;
        if let Err(error) = current.require_open(fence) {
            self.remove_fenced_batch(&key, root_hash, &name, &references)
                .await?;
            return Err(error);
        }
        Ok(Retained {
            references: references.len() as u64,
            revision: Some(issued.commit()),
        })
    }

    /// Commits a terminal fence before releasing its retained batches.
    pub async fn release_terminal(&self, root: &RootId, fence: u64) -> Result<Released, Error> {
        let issued = self.issued().await?;
        let _gate = self.state.gate.read().await;
        let encoded = Digest::of(root.as_bytes()).encoded_hex();
        let root_hash = encoded.as_ref();
        let terminal_key = self.terminal_key(root_hash);
        let (state, observed) = self.terminal_observed(&terminal_key).await?;
        if state.terminal.is_none_or(|current| fence > current)
            && !self.terminal_replace(observed, Some(fence)).await?
        {
            return Err(changed());
        }
        self.medium
            .flush(std::slice::from_ref(&terminal_key))
            .await
            .map_err(medium)?;
        let mut pages = self.pages(self.retention_key('f', root_hash, ""));
        let mut existed = false;
        while let Some(page) = pages.next().await? {
            for (key, _) in page {
                let name = key.rsplit('/').next().ok_or_else(corrupt)?;
                let encoded_fence = name.split('.').next().ok_or_else(corrupt)?;
                let batch_fence = u64::from_str_radix(encoded_fence, 16).map_err(|_| corrupt())?;
                if batch_fence > fence {
                    continue;
                }
                if let Some(references) = self.batch(&key).await? {
                    existed |= self
                        .remove_fenced_batch(&key, root_hash, name, &references)
                        .await?;
                }
            }
        }
        Ok(Released {
            existed,
            revision: Some(issued.commit()),
        })
    }
}
