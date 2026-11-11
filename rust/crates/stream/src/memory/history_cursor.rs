/// Incremental cursor over immutable history.
pub(crate) struct HistoryCursor {
    end: Option<u64>,
    records: FiniteRead,
    #[cfg(not(target_arch = "wasm32"))]
    work: Option<Arc<crate::native::Work>>,
    awaiting_creation: bool,
    opening_tail: Option<u64>,
    registration: Option<crate::interest::Interest<StreamPath, Signals>>,
}
impl HistoryCursor {
    async fn initial(
        path: Option<&PathState>,
        from: u64,
        anchor: Option<CommitId>,
        when_absent: crate::WhenAbsent,
    ) -> Result<Self, StreamError> {
        match path {
            Some(path) => Self::prepare(path, from, anchor).await,
            None if anchor.is_some() && when_absent == crate::WhenAbsent::Wait => {
                Err(StreamError::PrefixNotRetained)
            }
            None if when_absent == crate::WhenAbsent::Wait => {
                let mut cursor = Self::at(from);
                cursor.awaiting_creation = true;
                Ok(cursor)
            }
            None => Err(StreamError::NotFound),
        }
    }
    async fn observe_creation(
        &mut self,
        path: Option<&PathState>,
        anchor: Option<CommitId>,
    ) -> Result<bool, StreamError> {
        if self.awaiting_creation {
            let Some(path) = path else {
                return Ok(false);
            };
            let mut cursor = Self::prepare(path, self.position(), anchor).await?;
            #[cfg(not(target_arch = "wasm32"))]
            {
                cursor.work = self.work.clone();
            }
            cursor.registration = self.registration.take();
            cursor.opening_tail = self.opening_tail.take();
            *self = cursor;
        }
        Ok(true)
    }
    async fn prepare(
        path: &PathState,
        from: u64,
        anchor: Option<CommitId>,
    ) -> Result<Self, StreamError> {
        // A start beyond the tail waits there: its first delivery is `from`.
        // An anchor at `from - 1` must already exist, so it never waits.
        if anchor.is_some() && from > path.tail {
            return Err(StreamError::OutOfRange);
        }
        let start = if anchor.is_some() {
            from.checked_sub(1).ok_or(StreamError::InvalidArgument)?
        } else {
            from
        };
        let mut cursor = Self::new(path, path.tail, start);
        if let Some(anchor) = anchor {
            let record = cursor
                .next_record()
                .await?
                .ok_or(StreamError::PrefixNotRetained)?;
            if record.sequence != from - 1 || record.commit_id != anchor {
                return Err(StreamError::PrefixNotRetained);
            }
        }
        Ok(cursor)
    }
    async fn peek_record(&mut self) -> Result<Option<&crate::AppendRecord>, StreamError> {
        if self.finished() {
            return Ok(None);
        }
        // A follow renews finite chunks; the chunk never limits its history.
        if self.records.remaining == 0 {
            self.records.remaining = self
                .records
                .history
                .tail
                .saturating_sub(self.records.next)
                .min(u64::from(u32::MAX)) as u32;
        }
        #[cfg(not(target_arch = "wasm32"))]
        let work = self.work.clone();
        self.records.peek(
            |items| {
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(work) = &work {
                    return work.record(work.meter.examine(items, 0));
                }
                let _ = items;
                Ok(())
            },
            || StreamError::Unavailable,
        )
    }
    fn position(&self) -> u64 {
        self.records.next
    }
    async fn check_anchor(&self, path: &PathState, anchor: CommitId) -> Result<(), StreamError> {
        if path.tail < self.records.history.tail {
            return Err(StreamError::PrefixNotRetained);
        }
        if let Some(root) = &self.records.history.root {
            if path
                .history
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(root, current))
            {
                return Ok(());
            }
            if matches!(path.history.as_deref(), Some(History::Batch { parent: Some(parent), .. }) if Arc::ptr_eq(root, parent))
            {
                return Ok(());
            }
        }
        let from = self
            .position()
            .checked_sub(1)
            .ok_or(StreamError::InvalidArgument)?;
        let mut pinned = PinnedHistory::new(path, path.tail);
        let previous = crate::pages::PageSource::next_page(&mut pinned, from, 1).await?;
        if previous
            .and_then(|window| window.records.record(window.start))
            .is_none_or(|record| record.sequence != from || record.commit_id != anchor)
        {
            return Err(StreamError::PrefixNotRetained);
        }
        Ok(())
    }
    pub(crate) fn at(next: u64) -> Self {
        Self {
            records: FiniteRead {
                history: PinnedHistory::empty(next),
                window: None,
                key: None,
                next,
                remaining: 0,
            },
            #[cfg(not(target_arch = "wasm32"))]
            work: None,
            end: None,
            awaiting_creation: false,
            opening_tail: None,
            registration: None,
        }
    }
    fn new(path: &PathState, tail: u64, next: u64) -> Self {
        let records = FiniteRead {
            history: PinnedHistory::new(path, tail),
            window: path.prefix_window(next, tail),
            key: None,
            next,
            remaining: 0,
        };
        Self {
            end: None,
            records,
            #[cfg(not(target_arch = "wasm32"))]
            work: None,
            awaiting_creation: false,
            opening_tail: None,
            registration: None,
        }
    }
    fn sync(&mut self, path: &PathState) {
        let root = path.history.as_ref();
        let tail = path.tail;
        let history = &self.records.history;
        let same = match (&history.root, root) {
            (Some(old), Some(current)) => Arc::ptr_eq(old, current),
            (None, None) => true,
            _ => false,
        };
        if same && history.tail == tail {
            return;
        }
        let appended = matches!((&history.root, root.map(AsRef::as_ref)),
            (Some(old), Some(History::Batch { parent: Some(parent), .. })) if Arc::ptr_eq(old, parent));
        let keep_page = appended && tail >= history.tail;
        let pinned = PinnedHistory::new(path, tail);
        self.records.history = pinned;
        if !keep_page {
            self.records.window = None;
        }
        self.records.remaining = 0;
    }
    async fn next_record(&mut self) -> Result<Option<Record>, StreamError> {
        if self.finished() {
            return Ok(None);
        }
        let length = match self.peek_record().await? {
            Some(record) => record.value.len(),
            None => return Ok(None),
        };
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = &self.work {
            return work
                .record(self.records.take_native_record(&work.meter, length))
                .map(Some);
        }
        #[cfg(target_arch = "wasm32")]
        let _ = length;
        self.records
            .take_record()
            .map(Some)
            .ok_or(StreamError::Unavailable)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn with_native_work(mut self, work: Option<Arc<crate::native::Work>>) -> Self {
        self.work = work;
        self
    }
    fn observe<T>(&self, lookup: impl FnOnce() -> T) -> Result<T, StreamError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = &self.work {
            return work.record(inspect(&work.meter, lookup));
        }
        Ok(lookup())
    }

    pub(crate) fn finished(&self) -> bool {
        self.end.is_some_and(|end| self.position() >= end)
    }
}

/// Finite ordinary and funded reads share the immutable pin, key index and
/// one inline page. Admission surrounds the actual clone at the caller.
struct FiniteRead {
    history: PinnedHistory,
    window: Option<HistoryWindow>,
    key: Option<crate::ReadKeySeek>,
    next: u64,
    remaining: u32,
}

impl FiniteRead {
    fn selection(&self) -> crate::StreamSelection {
        crate::StreamSelection {
            from: self.next,
            preceding: None,
            end: Some(self.history.tail),
            bootstrap: None,
        }
    }

    fn new(path: &PathState, request: ReadRequest) -> Self {
        let tail = request.key.as_ref().map_or_else(
            || {
                path.tail
                    .min(request.from.saturating_add(u64::from(request.limit)))
            },
            |key| key.before.min(path.tail),
        );
        Self {
            history: PinnedHistory::new(path, tail),
            window: request
                .key
                .is_none()
                .then(|| path.prefix_window(request.from, tail))
                .flatten(),
            key: request.key,
            next: request.from,
            remaining: request.limit,
        }
    }

    fn peek<E>(
        &mut self,
        mut examine: impl FnMut(u64) -> Result<(), E>,
        invalid: impl Fn() -> E,
    ) -> Result<Option<&crate::AppendRecord>, E> {
        // A range cursor's `from` bounds only its current key; the index
        // seek decides whether anything later remains.
        if self.remaining == 0 || (self.key.is_none() && self.next >= self.history.tail) {
            return Ok(None);
        }
        if self
            .window
            .as_ref()
            .is_none_or(|window| window.start == window.end)
        {
            let after = if let Some(key) = &self.key {
                let Some(root) = &self.history.root else {
                    return Ok(None);
                };
                let Some(sequence) = root.keys().seek(
                    key,
                    self.next,
                    key.before.min(self.history.tail),
                    &mut examine,
                )?
                else {
                    return Ok(None);
                };
                sequence
            } else {
                self.next
            };
            self.window = self.history.page(
                after,
                if self.key.is_some() {
                    1
                } else {
                    self.remaining.min(crate::REPLAY_PAGE)
                },
                examine,
            )?;
            if self.window.as_ref().is_none_or(|window| {
                window.records.start.checked_add(window.start as u64) != Some(after)
            }) {
                return Err(invalid());
            }
        }
        let window = self.window.as_ref().ok_or_else(&invalid)?;
        if self.key.is_none()
            && window.records.start.checked_add(window.start as u64) != Some(self.next)
        {
            return Err(invalid());
        }
        let record = window
            .records
            .values
            .get(window.start)
            .ok_or_else(&invalid)?;
        Ok(Some(record))
    }

    fn take_record(&mut self) -> Option<Record> {
        let record = self.window.as_mut()?.next()?;
        match &mut self.key {
            Some(key) => key
                .advance(&mut self.next, record.sequence, record.headers.keys())
                .ok()?,
            None => self.next = record.sequence.checked_add(1)?,
        }
        self.remaining -= 1;
        Some(record)
    }

    /// Finite reads and follows admit the same shared-record delivery before
    /// cloning metadata or exposing its payload to the consumer.
    #[cfg(not(target_arch = "wasm32"))]
    fn take_native_record(
        &mut self,
        meter: &Arc<dyn acyclic_native_runtime::NativeReadMeter>,
        length: usize,
    ) -> std::io::Result<Record> {
        meter.check()?;
        let mut bytes = acyclic_native_runtime::NativeByteReservation::new(
            Arc::clone(meter),
            acyclic_native_runtime::NativeOperationKind::Read,
            length,
        )?;
        let copied = std::mem::size_of::<Record>() as u64;
        meter.admit_examination(1, copied)?;
        let record = self.take_record().ok_or(std::io::ErrorKind::InvalidData)?;
        #[cfg(test)]
        let read = crate::test_meter::trace::recordings_reordered().then(|| bytes.record(length));
        let examined = meter.record_examination(1, copied);
        #[cfg(test)]
        let read = read.unwrap_or_else(|| bytes.record(length));
        #[cfg(not(test))]
        let read = bytes.record(length);
        examined?;
        read?;
        Ok(record)
    }
}

impl Iterator for FiniteRead {
    type Item = Result<Record, StreamError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.peek(|_| Ok::<_, StreamError>(()), || StreamError::Unavailable) {
            Ok(Some(_)) => Some(self.take_record().ok_or(StreamError::Unavailable)),
            result => {
                let error = result.err();
                self.remaining = 0;
                error.map(Err)
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = if self.key.is_some() {
            u64::from(self.remaining)
        } else {
            self.history
                .tail
                .saturating_sub(self.next)
                .min(u64::from(self.remaining))
        };
        (0, usize::try_from(remaining).ok())
    }
}
