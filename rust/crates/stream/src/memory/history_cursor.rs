/// Incremental cursor over immutable history.
pub(crate) struct HistoryCursor {
    records: crate::pages::PinnedCursor<PinnedHistory>,
    awaiting_creation: bool,
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
            cursor.registration = self.registration.take();
            *self = cursor;
        }
        Ok(true)
    }
    async fn prepare(
        path: &PathState,
        from: u64,
        anchor: Option<CommitId>,
    ) -> Result<Self, StreamError> {
        if from > path.tail {
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
    async fn peek_record(&mut self) -> Result<Option<&Record>, StreamError> {
        self.records.peek().await
    }
    fn position(&self) -> u64 {
        self.records.position()
    }
    async fn check_anchor(&self, path: &PathState, anchor: CommitId) -> Result<(), StreamError> {
        if let Some(root) = &self.records.source().root {
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
        let pinned = PinnedHistory {
            root: path.history.clone(),
            tail: path.tail,
        };
        let previous = crate::pages::PageSource::next_page(&pinned, from, 1).await?;
        if previous
            .and_then(|window| window.records.records.get(window.start).cloned())
            .is_none_or(|record| record.sequence != from || record.commit_id != anchor)
        {
            return Err(StreamError::PrefixNotRetained);
        }
        Ok(())
    }
    pub(crate) fn at(next: u64) -> Self {
        Self {
            records: Self::pinned(
                PinnedHistory {
                    root: None,
                    tail: next,
                },
                next,
            ),
            awaiting_creation: false,
            registration: None,
        }
    }
    fn pinned(history: PinnedHistory, next: u64) -> crate::pages::PinnedCursor<PinnedHistory> {
        crate::pages::PinnedCursor::new(history, next, crate::REPLAY_PAGE)
            .expect("the stream replay page is nonzero")
    }
    fn new(path: &PathState, tail: u64, next: u64) -> Self {
        let mut records = Self::pinned(
            PinnedHistory {
                root: path.history.clone(),
                tail,
            },
            next,
        );
        if let Some(page) = path.prefix_window(next, tail) {
            records.seed_page(page);
        }
        Self {
            records,
            awaiting_creation: false,
            registration: None,
        }
    }
    fn sync(&mut self, root: Option<&Arc<History>>, tail: u64) {
        let history = self.records.source();
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
        self.records.repin(
            PinnedHistory {
                root: root.cloned(),
                tail,
            },
            keep_page,
        );
    }
    async fn next_record(&mut self) -> Result<Option<Record>, StreamError> {
        self.records.next().await
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
    fn new(path: &PathState, request: ReadRequest) -> Self {
        let tail = request.key.as_ref().map_or_else(
            || {
                path.tail
                    .min(request.from.saturating_add(u64::from(request.limit)))
            },
            |key| key.before.min(path.tail),
        );
        Self {
            history: PinnedHistory {
                root: path.history.clone(),
                tail,
            },
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
        mut examine: impl FnMut() -> Result<(), E>,
        invalid: impl Fn() -> E,
    ) -> Result<Option<&Record>, E> {
        if self.remaining == 0 || self.next >= self.history.tail {
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
                let Some(sequence) =
                    root.keys()
                        .seek(&key.key, self.next, self.history.tail, &mut examine)?
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
            if self
                .window
                .as_ref()
                .and_then(|window| window.records.records.get(window.start))
                .is_none_or(|record| record.sequence != after)
            {
                return Err(invalid());
            }
        }
        let record = self
            .window
            .as_ref()
            .and_then(|window| window.records.records.get(window.start))
            .ok_or_else(&invalid)?;
        if self.key.is_none() && record.sequence != self.next {
            return Err(invalid());
        }
        Ok(Some(record))
    }

    fn take_record(&mut self) -> Option<Record> {
        let record = self.window.as_mut()?.next()?;
        self.next = record.sequence.checked_add(1)?;
        self.remaining -= 1;
        Some(record)
    }
}

impl Iterator for FiniteRead {
    type Item = Result<Record, StreamError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.peek(|| Ok::<_, StreamError>(()), || StreamError::Unavailable) {
            Ok(Some(_)) => Some(self.take_record().ok_or(StreamError::Unavailable)),
            result => {
                let error = result.err();
                self.remaining = 0;
                error.map(Err)
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self
            .history
            .tail
            .saturating_sub(self.next)
            .min(u64::from(self.remaining));
        (0, usize::try_from(remaining).ok())
    }
}
