//! Shape admission belongs to one executor and journal binding. File-bearing
//! messages remain in an index and are authorized again at every dispatch.
use super::*;
use crate::executor::ExecutionJournalCacheId;

#[derive(Debug)]
pub(crate) struct ContentAdmission {
    executor: ExecutionJournalCacheId,
    journal: ExecutionJournalCacheId,
    messages: usize,
    files: Vec<usize>,
}

impl ContentAdmission {
    pub(super) fn new(executor: ExecutionJournalCacheId, journal: ExecutionJournalCacheId) -> Self {
        Self {
            executor,
            journal,
            messages: 0,
            files: Vec::new(),
        }
    }

    pub(super) fn reset_binding(
        &mut self,
        executor: ExecutionJournalCacheId,
        journal: ExecutionJournalCacheId,
    ) {
        if self.executor != executor || self.journal != journal {
            self.executor = executor;
            self.journal = journal;
            self.messages = 0;
            self.files.clear();
        }
    }

    #[cfg(feature = "filesystem")]
    pub(super) fn owned_bytes(&self) -> Result<u64> {
        slots(self.files.capacity(), std::mem::size_of::<usize>())
    }

    pub(crate) const fn verified_messages(&self) -> usize {
        self.messages
    }

    pub(crate) fn previous_files<'a>(
        &'a self,
        request: &'a PreparedModelRequest,
        io: Option<&PreparationIo>,
    ) -> Result<impl Iterator<Item = &'a ModelContent> + Clone + 'a> {
        if self.messages > request.request.messages.len()
            || self.files.iter().any(|index| *index >= self.messages)
        {
            return Err(crate::executor::stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "model content admission exceeds its immutable prefix",
            ));
        }
        // Every index was checked against the verified prefix above. The
        // iterator cannot manufacture a reference outside this exact request.
        Ok(self
            .files
            .iter()
            .flat_map(move |index| request.request.messages.get(*index))
            .map(|message| &message.content))
    }

    /// Called only after all limits, grants and original bindings succeeded.
    pub(crate) fn extend(
        mut self,
        request: &PreparedModelRequest,
        io: Option<&PreparationIo>,
    ) -> Result<Self> {
        let suffix = request
            .request
            .messages
            .get(self.messages..)
            .ok_or_else(|| {
                crate::executor::stock_boundary(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "model content admission exceeds its immutable prefix",
                )
            })?;
        for (offset, message) in suffix.iter().enumerate() {
            charge_items(io, 1)?;
            if message.content.file_bindings().next().is_some() {
                if self.files.len() == self.files.capacity() {
                    let capacity = self
                        .files
                        .capacity()
                        .checked_mul(2)
                        .map(|capacity| capacity.max(1))
                        .ok_or_else(|| {
                            crate::executor::stock_boundary(
                                io,
                                crate::wire::ErrorCode::Invalid,
                                "model file index capacity overflows",
                            )
                        })?;
                    crate::executor::stock_vec_reserve(&mut self.files, capacity, io)?;
                }
                let index = self.messages.checked_add(offset).ok_or_else(|| {
                    crate::executor::stock_boundary(
                        io,
                        crate::wire::ErrorCode::Invalid,
                        "model file index position overflows",
                    )
                })?;
                crate::executor::stock_vec_push(&mut self.files, index, io)?;
            }
        }
        self.messages = request.request.messages.len();
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_admission_cannot_cross_executor_or_journal_bindings() {
        let executor = ExecutionJournalCacheId::default();
        let journal = ExecutionJournalCacheId::default();
        let other_executor = ExecutionJournalCacheId::default();
        let other_journal = ExecutionJournalCacheId::default();
        for count in [1, 10, 100, 1_000] {
            for (next_executor, next_journal, retained) in [
                (executor, journal, true),
                (other_executor, journal, false),
                (executor, other_journal, false),
                (other_executor, other_journal, false),
            ] {
                let mut admission = ContentAdmission {
                    executor,
                    journal,
                    messages: count,
                    files: if count == 1 {
                        vec![0]
                    } else {
                        vec![0, count - 1]
                    },
                };
                let capacity = admission.files.capacity();
                admission.reset_binding(next_executor, next_journal);
                assert_eq!(
                    admission.verified_messages(),
                    if retained { count } else { 0 },
                    "content admission crossed executor or journal bindings"
                );
                assert_eq!(admission.files.is_empty(), !retained);
                assert_eq!(
                    admission.files.capacity(),
                    capacity,
                    "changing authority need not discard already funded index storage"
                );
            }
        }
    }
}
