use super::*;

impl Shared {
    /// One background delivery pass, run by the hub's service pool after
    /// records were routed to this watch.
    pub(super) fn service(&self) {
        self.inbox
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .begin_service();
        if self.retiring.load(Ordering::Acquire) {
            return;
        }
        // Never wait for the cursor: its holder serves this pass on release.
        let Some(mut reading) = self.service_reading() else {
            return;
        };
        if self.stopping.load(Ordering::Acquire) {
            // A controller that could not stop leaves retirement to delivery.
            if self.delivery.original.is_some() && !self.controller_alive.load(Ordering::Acquire) {
                self.retire_in_service(reading);
            }
            return;
        }
        if reading.blocked {
            // The fence that unblocks delivery reschedules waiting records.
            return;
        }
        #[cfg(test)]
        self.polling.store(false, Ordering::Release);
        let mut work = self.delivery.callback_work();
        let started = std::time::Instant::now();
        let delivered = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            work.started_io(READ)?;
            self.read_events_and_deliver_locked(
                &mut reading,
                started,
                std::time::Duration::MAX,
                &mut work,
            )
        }));
        #[cfg(test)]
        self.polling.store(true, Ordering::Release);
        match delivered {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => {
                self.delivery.failed_with_work(error, &mut work);
                self.retire_in_service(reading);
            }
            Err(_) => {
                self.delivery.exact.store(false, Ordering::Release);
                self.retire_in_service(reading);
            }
        }
    }

    /// Moves routed records into the read buffer. Returns the exactness to
    /// report when no record waits; an idle watch then holds no buffer.
    fn refill<W: SourceDemandReadWork>(
        &self,
        reading: &mut RawReads,
        work: &mut W,
    ) -> Result<Option<bool>, DemandError> {
        let maximum = usize::try_from(
            work.remaining()?
                .source_bytes_read
                .min(READ_BUFFER_BYTES as u64),
        )
        .map_err(|_| WorkError::Overflow)?;
        if maximum < EVENT_HEADER_BYTES {
            work.admit(WorkCounters {
                source_bytes_read: EVENT_HEADER_BYTES as u64,
                ..WorkCounters::default()
            })?;
        }
        work.admit(WorkCounters {
            source_bytes_read: maximum as u64,
            ..WorkCounters::default()
        })?;
        work.started_io(READ)?;
        // Records the kernel queued reach this inbox before it is read.
        hub::hub()?.drain()?;
        let taken = self
            .inbox
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take(&mut reading.buffer, maximum);
        reading.cursor = 0;
        reading.alias_cursor = 0;
        match taken {
            Ok(read) => {
                reading.filled = read;
                work.record(WorkCounters {
                    source_bytes_read: read as u64,
                    ..WorkCounters::default()
                })?;
                Ok(None)
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                reading.buffer = Vec::new();
                reading.filled = 0;
                Ok(Some(self.delivery.is_exact()))
            }
            Err(error) => Err(error.into()),
        }
    }

    pub(super) fn read_events_and_deliver<W: SourceDemandReadWork>(
        &self,
        timeout: std::time::Duration,
        work: &mut W,
    ) -> Result<bool, DemandError> {
        work.check()?;
        if self.delivery.original.is_some() && !self.delivery.is_exact() {
            return Ok(false);
        }
        let started = std::time::Instant::now();
        let mut reading = self.reading();
        self.read_events_and_deliver_locked(&mut reading, started, timeout, work)
    }

    /// [`Self::read_events_and_deliver`] under a held cursor.
    #[allow(
        clippy::too_many_lines,
        reason = "One locked event drain retains its cursor, accounting and failure state together."
    )]
    fn read_events_and_deliver_locked<W: SourceDemandReadWork>(
        &self,
        reading: &mut RawReads,
        started: std::time::Instant,
        timeout: std::time::Duration,
        work: &mut W,
    ) -> Result<bool, DemandError> {
        work.check()?;
        if self.delivery.original.is_some() && !self.delivery.is_exact() {
            return Ok(false);
        }
        if self.stopping.load(Ordering::Acquire) {
            return Ok(false);
        }
        let result = (|| {
            loop {
                work.check()?;
                if started.elapsed() >= timeout {
                    self.delivery.invalidate(work)?;
                    return Ok(false);
                }
                while let Some(path) = reading.cleanup.last().filter(|_| reading.alias_cursor == 0)
                {
                    if started.elapsed() >= timeout {
                        self.delivery.invalidate(work)?;
                        return Ok(false);
                    }
                    self.unadmit_with_work(path, work)?;
                    let path = reading.cleanup.pop().ok_or(DemandError::InvalidRequest)?;
                    let bytes = path.capacity() as u64;

                    drop(path);
                    self.cancel_record_cleanup(work);
                    self.ownership.retire(bytes)?;
                    self.retained_variable_bytes
                        .fetch_sub(bytes, Ordering::AcqRel);
                }
                if reading.cursor == reading.filled
                    && let Some(exact) = self.refill(reading, work)?
                {
                    return Ok(exact);
                }
                work.started_io(ITEM)?;
                let raw = reading
                    .buffer
                    .get(reading.cursor..reading.filled)
                    .ok_or(DemandError::InvalidRequest)?;
                let Some(
                    &[
                        d0,
                        d1,
                        d2,
                        d3,
                        m0,
                        m1,
                        m2,
                        m3,
                        c0,
                        c1,
                        c2,
                        c3,
                        l0,
                        l1,
                        l2,
                        l3,
                    ],
                ) = raw.first_chunk::<EVENT_HEADER_BYTES>()
                else {
                    self.delivery.invalidate(work)?;
                    reading.cursor = reading.filled;
                    return Ok(false);
                };
                let descriptor = i32::from_ne_bytes([d0, d1, d2, d3]);
                let mask = u32::from_ne_bytes([m0, m1, m2, m3]);
                let cookie = u32::from_ne_bytes([c0, c1, c2, c3]) as usize;
                let end = EVENT_HEADER_BYTES
                    .checked_add(u32::from_ne_bytes([l0, l1, l2, l3]) as usize)
                    .ok_or(WorkError::Overflow)?;
                let Some(bytes) = raw.get(EVENT_HEADER_BYTES..end) else {
                    self.delivery.invalidate(work)?;
                    reading.cursor = reading.filled;
                    return Ok(false);
                };
                let mut name_length = 0;
                for byte in bytes {
                    work.started_io(ITEM)?;
                    if *byte == 0 {
                        break;
                    }
                    name_length += 1;
                }
                let name = OsStr::from_bytes(bytes.split_at(name_length).0);
                if mask & (libc::IN_Q_OVERFLOW | libc::IN_UNMOUNT) != 0 {
                    self.delivery.invalidate(work)?;
                    reading.cursor += end;
                    reading.alias_cursor = 0;
                    continue;
                }
                let mut ancestor = None;
                {
                    let ancestors = self
                        .ancestors
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner);
                    for (watched, on_the_way) in &*ancestors {
                        work.started_io(ITEM)?;
                        if *watched == descriptor {
                            ancestor = Some(
                                mask & (libc::IN_DELETE_SELF
                                    | libc::IN_MOVE_SELF
                                    | libc::IN_IGNORED)
                                    != 0
                                    || mask & REBINDING != 0 && name == on_the_way.as_os_str(),
                            );
                            break;
                        }
                    }
                }
                if let Some(invalidated) = ancestor {
                    if invalidated {
                        self.delivery.invalidate(work)?;
                        self.delivery.exact.store(false, Ordering::Release);
                        return Ok(false);
                    }
                    reading.cursor += end;
                    reading.alias_cursor = 0;
                    continue;
                }
                let cleanup_needed =
                    mask & (REBINDING | libc::IN_DELETE_SELF | libc::IN_MOVE_SELF) != 0;
                let event_needed = mask
                    & (REBINDING
                        | libc::IN_MODIFY
                        | libc::IN_ATTRIB
                        | if self.delivery.events.is_none() {
                            libc::IN_CLOSE_WRITE
                        } else {
                            0
                        })
                    != 0;
                loop {
                    if started.elapsed() >= timeout {
                        self.delivery.invalidate(work)?;
                        return Ok(false);
                    }
                    let watches = self.watches.lock().unwrap_or_else(PoisonError::into_inner);
                    let Some((alias, watched)) = watches.get(reading.alias_cursor) else {
                        break;
                    };
                    work.started_io(ITEM)?;
                    if *watched != descriptor {
                        reading.alias_cursor += 1;
                        continue;
                    }
                    if !cleanup_needed && !event_needed {
                        reading.alias_cursor += 1;
                        continue;
                    }
                    if alias.as_os_str().is_empty() && mask & libc::IN_DELETE_SELF != 0 {
                        drop(watches);
                        self.delivery.invalidate(work)?;
                        self.delivery.exact.store(false, Ordering::Release);
                        return Ok(false);
                    }
                    let bytes = alias
                        .as_os_str()
                        .as_bytes()
                        .len()
                        .checked_add(usize::from(
                            !alias.as_os_str().is_empty() && !name.is_empty(),
                        ))
                        .and_then(|length| length.checked_add(name.len()))
                        .ok_or(WorkError::Overflow)?;
                    let delta = copied(bytes as u64);
                    if cleanup_needed {
                        self.reserve_record_cleanup(work)?;
                    }
                    let claimed = if cleanup_needed {
                        self.ownership.claim_retained(bytes as u64, delta, work)
                    } else {
                        work.claim_allocation(bytes as u64, u64::from(bytes != 0), delta)
                    };
                    if let Err(error) = claimed {
                        if cleanup_needed {
                            self.cancel_record_cleanup(work);
                        }
                        return Err(error);
                    }
                    let mut path = PathBuf::with_capacity(bytes);
                    if !alias.as_os_str().is_empty() {
                        path.push(alias);
                    }
                    if !name.is_empty() {
                        path.push(name);
                    }
                    drop(watches);
                    if cleanup_needed && reading.cleanup.len() == reading.cleanup.capacity() {
                        let old_bytes = (reading.cleanup.capacity() as u64)
                            .checked_mul(std::mem::size_of::<PathBuf>() as u64)
                            .ok_or(WorkError::Overflow)?;
                        let next_capacity = reading
                            .cleanup
                            .len()
                            .checked_add(1)
                            .ok_or(WorkError::Overflow)?;
                        let next_bytes = (next_capacity as u64)
                            .checked_mul(std::mem::size_of::<PathBuf>() as u64)
                            .ok_or(WorkError::Overflow)?;
                        let delta = copied(
                            (reading.cleanup.len() as u64) * std::mem::size_of::<PathBuf>() as u64,
                        );
                        let prepared = (|| {
                            self.ownership.claim_retained(next_bytes, delta, work)?;
                            let mut replacement = Vec::with_capacity(next_capacity);
                            replacement.append(&mut reading.cleanup);
                            reading.cleanup = replacement;
                            let retained_bytes = reading.cleanup.capacity() as u64
                                * std::mem::size_of::<PathBuf>() as u64;
                            self.retained_variable_bytes
                                .fetch_add(retained_bytes - old_bytes, Ordering::AcqRel);
                            self.ownership.retire(old_bytes)?;
                            if !self.ownership.has_native_custody() {
                                work.release(next_bytes)?;
                            }
                            Ok::<(), DemandError>(())
                        })();
                        if let Err(error) = prepared {
                            drop(path);
                            self.cancel_record_cleanup(work);
                            self.ownership.discard_retained(bytes as u64, work)?;
                            return Err(error);
                        }
                    }
                    let delivered = if self.delivery.events.is_some() {
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            self.delivery.event(&path, mask, cookie, work)
                        }))
                    } else {
                        let change = if mask
                            & (REBINDING | libc::IN_DELETE_SELF | libc::IN_MOVE_SELF)
                            != 0
                        {
                            HostChange::Rebound(path)
                        } else {
                            HostChange::Altered(path)
                        };
                        let answer = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            self.delivery.deliver(std::slice::from_ref(&change), work)
                        }));
                        path = match change {
                            HostChange::Rebound(path) | HostChange::Altered(path) => path,
                            HostChange::Everything => unreachable!("constructed path change"),
                        };
                        answer
                    };
                    let delivered = match delivered {
                        Ok(result) => result,
                        Err(panic) => {
                            drop(path);
                            if cleanup_needed {
                                self.cancel_record_cleanup(work);
                            }
                            let released = if cleanup_needed {
                                self.ownership.discard_retained(bytes as u64, work)
                            } else {
                                work.release(bytes as u64)
                            };
                            if let Err(error) = released {
                                self.delivery.failed_with_work(error, work);
                            }
                            std::panic::resume_unwind(panic);
                        }
                    };
                    if delivered.is_err() {
                        drop(path);
                        if cleanup_needed {
                            self.cancel_record_cleanup(work);
                        }
                        if cleanup_needed {
                            self.ownership.discard_retained(bytes as u64, work)?;
                        } else {
                            work.release(bytes as u64)?;
                        }
                        return delivered.map(|()| false);
                    }
                    reading.alias_cursor += 1;
                    if cleanup_needed {
                        let retained_bytes = path.capacity() as u64;

                        reading.cleanup.push(path);
                        self.retained_variable_bytes
                            .fetch_add(retained_bytes, Ordering::AcqRel);
                        if !self.ownership.has_native_custody() {
                            work.release(bytes as u64)?;
                        }
                    } else {
                        drop(path);
                        work.release(bytes as u64)?;
                    }
                }
                if mask & libc::IN_IGNORED != 0 {
                    let mut watches = self.watches.lock().unwrap_or_else(PoisonError::into_inner);
                    let mut at = 0;
                    while let Some((_, watched)) = watches.get(at) {
                        work.started_io(ITEM)?;
                        if *watched == descriptor {
                            let shifted = (watches.len() - at - 1)
                                .checked_mul(std::mem::size_of::<(PathBuf, i32)>())
                                .ok_or(WorkError::Overflow)?;
                            work.started_io(copied(shifted as u64))?;
                            let (path, _) = watches.remove(at);
                            let bytes = path.capacity() as u64;

                            drop(path);
                            self.cancel_record_cleanup(work);
                            self.ownership.retire(bytes)?;
                            self.retained_variable_bytes
                                .fetch_sub(bytes, Ordering::AcqRel);
                        } else {
                            at += 1;
                        }
                    }
                }
                reading.cursor += end;
                reading.alias_cursor = 0;
            }
        })();
        reading.blocked = !matches!(result, Ok(true));
        if result.is_err() {
            self.delivery.exact.store(false, Ordering::Release);
        }
        if !reading.blocked {
            self.reading_changed.notify_all();
            // Records routed while delivery was blocked were left for this pass.
            self.reschedule();
        }
        result
    }
}
