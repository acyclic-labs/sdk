use super::{Revision, RevisionScope, StreamError};
#[cfg(loom)]
use loom::sync::atomic::{AtomicBool, AtomicU64, Ordering};
#[cfg(not(loom))]
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

#[derive(Debug)]
pub(super) struct ObservedAuthority {
    pub(super) scope: RevisionScope,
    progress: std::sync::OnceLock<AuthorityProgress>,
}

#[derive(Debug)]
struct AuthorityProgress {
    incarnation: u128,
    index: AtomicU64,
    conflicting_incarnation: AtomicBool,
}

impl ObservedAuthority {
    pub(super) fn new(scope: RevisionScope) -> Self {
        Self {
            scope,
            progress: std::sync::OnceLock::new(),
        }
    }

    pub(super) fn observe(&self, revision: &Revision) {
        let progress = self.progress.get_or_init(|| AuthorityProgress {
            incarnation: revision.incarnation,
            index: AtomicU64::new(revision.index),
            conflicting_incarnation: AtomicBool::new(false),
        });
        if progress.incarnation != revision.incarnation {
            progress
                .conflicting_incarnation
                .store(true, Ordering::Release);
        } else if progress.index.load(Ordering::Relaxed) < revision.index {
            progress.index.fetch_max(revision.index, Ordering::Release);
        }
    }

    pub(super) fn revision(&self) -> Result<Option<Revision>, StreamError> {
        let Some(progress) = self.progress.get() else {
            return Ok(None);
        };
        if progress.conflicting_incarnation.load(Ordering::Acquire) {
            return Err(StreamError::RevisionForeign);
        }
        Ok(Some(Revision {
            scope: self.scope.clone(),
            incarnation: progress.incarnation,
            index: progress.index.load(Ordering::Acquire),
        }))
    }
}

#[cfg(loom)]
pub(super) fn verify_concurrent_frontier(replace_maximum: bool) {
    loom::model(move || {
        let first = Revision {
            scope: RevisionScope::new(b"authority".to_vec()),
            incarnation: 1,
            index: 1,
        };
        let authority = std::sync::Arc::new(ObservedAuthority::new(first.scope.clone()));
        authority.observe(&first);
        let writer = std::sync::Arc::clone(&authority);
        let higher = Revision {
            index: 7,
            ..first.clone()
        };
        let published = loom::thread::spawn(move || writer.observe(&higher));
        let reader = std::sync::Arc::clone(&authority);
        let lower = Revision {
            index: 3,
            ..first.clone()
        };
        let observed = loom::thread::spawn(move || {
            reader.observe(&lower);
            if replace_maximum {
                // Negative control: last reply wins instead of atomic maximum.
                reader
                    .progress
                    .get()
                    .unwrap()
                    .index
                    .store(3, Ordering::Release);
            }
        });
        published.join().unwrap();
        observed.join().unwrap();
        let held = authority.revision().unwrap().unwrap();
        assert_eq!(held.incarnation, 1);
        assert_eq!(held.index, 7, "a late reply lowered the required frontier");
        authority.observe(&Revision {
            incarnation: 2,
            index: 99,
            ..first.clone()
        });
        assert_eq!(authority.revision(), Err(StreamError::RevisionForeign));
        authority.observe(&first);
        assert_eq!(authority.revision(), Err(StreamError::RevisionForeign));
    });
}
