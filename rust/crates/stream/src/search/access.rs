//! Provider and allocation ownership for the single search traversal.
use super::*;
use futures::{StreamExt as _, TryStreamExt as _};

#[derive(Default)]
pub(super) struct Owner {
    #[cfg(not(target_arch = "wasm32"))]
    allocation: Option<acyclic_native_runtime::ReadAllocation>,
}

pub(super) struct Held<T> {
    pub value: T,
    pub owner: Owner,
}

pub(super) struct Access<'a, P: ?Sized> {
    provider: &'a P,
    #[cfg(not(target_arch = "wasm32"))]
    native: Option<Native<P>>,
}

#[cfg(not(target_arch = "wasm32"))]
struct Native<P: ?Sized> {
    work: crate::native::Work,
    heads: for<'a> fn(
        &'a P,
        HeadsRequest,
        std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> crate::NativeProviderFuture<'a, Heads>,
    read: for<'a> fn(
        &'a P,
        ReadRequest,
        std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> crate::NativeProviderFuture<'a, crate::NativeRecordStream>,
}

pub(super) enum Records {
    Portable(crate::RecordStream),
    #[cfg(not(target_arch = "wasm32"))]
    Native(crate::NativeStreamOutput<crate::NativeRecordStream>),
}

impl Owner {
    pub fn absorb(&mut self, other: Self) -> Result<(), StreamError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(mut incoming) = other.allocation {
            match &mut self.allocation {
                Some(owned) => owned
                    .absorb(&mut incoming)
                    .map_err(|_| StreamError::InvalidArgument)?,
                None => self.allocation = Some(incoming),
            }
        }
        #[cfg(target_arch = "wasm32")]
        let _ = other;
        Ok(())
    }

    pub fn release(&mut self, bytes: usize) -> Result<(), StreamError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(owned) = &mut self.allocation {
            owned
                .release(bytes)
                .map_err(|_| StreamError::InvalidResponse)?;
        }
        let _ = bytes;
        Ok(())
    }
}

impl<'a, P: StreamProvider + ?Sized> Access<'a, P> {
    pub fn portable(provider: &'a P) -> Self {
        Self {
            provider,
            #[cfg(not(target_arch = "wasm32"))]
            native: None,
        }
    }

    fn io<T>(&self, result: std::io::Result<T>) -> Result<T, StreamError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(native) = &self.native {
            return native.work.record(result);
        }
        result.map_err(|_| StreamError::Unavailable)
    }

    pub fn vector<T>(&self, length: usize, owner: &mut Owner) -> Result<Vec<T>, StreamError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(native) = &self.native {
            let mut allocation = acyclic_native_runtime::ReadAllocation::empty(&native.work.meter);
            let value = self.io(crate::native::vector(
                length,
                &native.work.meter,
                &mut allocation,
            ))?;
            owner.absorb(Owner {
                allocation: Some(allocation),
            })?;
            return Ok(value);
        }
        let mut value = Vec::new();
        self.io(value
            .try_reserve_exact(length)
            .map_err(|_| std::io::Error::from(std::io::ErrorKind::OutOfMemory)))?;
        Ok(value)
    }

    fn copied(&self, bytes: usize, copy: impl FnOnce()) -> Result<(), StreamError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(native) = &self.native {
            self.io(native.work.meter.admit_codec(0, bytes as u64, 0))?;
            copy();
            return self.io(native.work.meter.record_codec(0, bytes as u64, 0));
        }
        let _ = bytes;
        copy();
        Ok(())
    }

    pub fn copy_bytes(&self, bytes: &[u8], owner: &mut Owner) -> Result<Vec<u8>, StreamError> {
        let mut value = self.vector(bytes.len(), owner)?;
        self.copied(bytes.len(), || value.extend_from_slice(bytes))?;
        Ok(value)
    }

    fn path(&self, path: &StreamPath) -> Result<StreamPath, StreamError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(native) = &self.native {
            return self.io(StreamPath::new_native(path.as_str(), &native.work.meter))?;
        }
        Ok(path.clone())
    }

    pub fn cursor(&self, cursor: &Cursor, owner: &mut Owner) -> Result<Cursor, StreamError> {
        let pattern = self.copy_bytes(&cursor.pattern, owner)?;
        let mut frontier = self.vector(cursor.frontier.len(), owner)?;
        for frame in &cursor.frontier {
            let copied = Frame {
                path: self.path(&frame.path)?,
                sequence: frame.sequence,
                tail: frame.tail,
                after: frame
                    .after
                    .as_ref()
                    .map(|path| self.path(path))
                    .transpose()?,
                content: frame
                    .content
                    .as_deref()
                    .map(|bytes| self.copy_bytes(bytes, owner))
                    .transpose()?,
            };
            self.copied(std::mem::size_of::<Frame>(), || frontier.push(copied))?;
        }
        Ok(Cursor {
            pattern,
            frontier,
            at: cursor.at.clone(),
            matcher: cursor.matcher,
        })
    }

    pub fn push<T>(
        &self,
        output: &mut Vec<T>,
        value: T,
        owner: &mut Owner,
    ) -> Result<(), StreamError> {
        if output.len() == output.capacity() {
            let capacity = output
                .capacity()
                .checked_mul(2)
                .ok_or(StreamError::LimitExceeded)?
                .max(1);
            let mut grown = self.vector(capacity, owner)?;
            let bytes = std::mem::size_of_val(output.as_slice());
            self.copied(bytes, || grown.append(output))?;
            let retired = std::mem::replace(output, grown);
            let bytes = retired
                .capacity()
                .checked_mul(std::mem::size_of::<T>())
                .ok_or(StreamError::InvalidArgument)?;
            drop(retired);
            owner.release(bytes)?;
        }
        self.copied(std::mem::size_of::<T>(), || output.push(value))
    }

    pub async fn heads(&self, request: HeadsRequest) -> Result<Held<Heads>, StreamError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(native) = &self.native {
            let output = self.io((native.heads)(
                self.provider,
                request,
                std::sync::Arc::clone(&native.work.meter),
            )
            .await)??;
            let (value, allocation) = output.into_parts();
            return Ok(Held {
                value,
                owner: Owner {
                    allocation: Some(allocation),
                },
            });
        }
        Ok(Held {
            value: self.provider.heads(request).await?,
            owner: Owner::default(),
        })
    }

    pub async fn read(&self, request: ReadRequest) -> Result<Records, StreamError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(native) = &self.native {
            return self
                .io((native.read)(
                    self.provider,
                    request,
                    std::sync::Arc::clone(&native.work.meter),
                )
                .await)?
                .map(Records::Native);
        }
        self.provider.read(request).await.map(Records::Portable)
    }

    pub async fn step<M: Matcher>(
        &self,
        matcher: &M,
        record: &Record,
        pattern: &[u8],
        after: Option<&[u8]>,
    ) -> Result<Held<Step<M::Source>>, M::Error> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(native) = &self.native {
            let output = self.io(matcher
                .native_step(record, pattern, after, &native.work.meter)
                .await)??;
            let (value, allocation) = output.into_parts();
            return Ok(Held {
                value,
                owner: Owner {
                    allocation: Some(allocation),
                },
            });
        }
        Ok(Held {
            value: matcher.step(record, pattern, after).await?,
            owner: Owner::default(),
        })
    }

    pub async fn next(&self, records: &mut Records) -> Result<Option<Held<Record>>, StreamError> {
        match records {
            Records::Portable(records) => records.try_next().await.map(|record| {
                record.map(|value| Held {
                    value,
                    owner: Owner::default(),
                })
            }),
            #[cfg(not(target_arch = "wasm32"))]
            Records::Native(records) => match records.value_mut().next().await {
                None => Ok(None),
                Some(result) => {
                    let (value, allocation) = self.io(result)??.into_parts();
                    Ok(Some(Held {
                        value,
                        owner: Owner {
                            allocation: Some(allocation),
                        },
                    }))
                }
            },
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl<'a, P: crate::NativeStreamProvider + ?Sized> Access<'a, P> {
    pub fn native(
        provider: &'a P,
        meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Self {
        Self {
            provider,
            native: Some(Native {
                work: crate::native::Work::inline(meter),
                heads: P::native_heads,
                read: P::native_read,
            }),
        }
    }

    pub fn finish<T, E>(
        &self,
        result: Result<T, E>,
        owner: Owner,
    ) -> crate::NativeStreamResult<T, E> {
        self.native
            .as_ref()
            .ok_or(std::io::ErrorKind::InvalidInput)?
            .work
            .finish(result.map(|value| (value, owner.allocation)))
    }
}
