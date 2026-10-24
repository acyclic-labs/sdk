pub(super) use crate::sync::Arc as Shared;
use crate::sync::Mutex;

use crate::ObjectId;

// Disposable immutable directory indexes. Edits produce a different tree id.
const SLOTS: usize = 64;

pub(super) enum DirectoryNames<T> {
    Unique,
    Collisions(Shared<T>),
}

impl<T> Clone for DirectoryNames<T> {
    fn clone(&self) -> Self {
        match self {
            Self::Unique => Self::Unique,
            Self::Collisions(keys) => Self::Collisions(Shared::clone(keys)),
        }
    }
}

pub(super) struct FoldedDirectoryCache<T> {
    // Lock order: view gate -> shared checkout -> this terminal lock. No guard
    // escapes; only Arc clones/moves occur inside. Evicted payloads drop outside.
    state: Mutex<(usize, [Option<(ObjectId, DirectoryNames<T>)>; SLOTS])>,
}

impl<T> FoldedDirectoryCache<T> {
    pub(super) fn new() -> Self {
        Self {
            state: Mutex::new((0, std::array::from_fn(|_| None))),
        }
    }

    pub(super) fn get(&self, key: ObjectId) -> Option<DirectoryNames<T>> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .1
            .iter()
            .flatten()
            .find_map(|(tree, value)| (*tree == key).then(|| value.clone()))
    }

    pub(super) fn insert(&self, key: ObjectId, value: DirectoryNames<T>) {
        let previous = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.1.iter().flatten().any(|(tree, _)| *tree == key) {
                return;
            }
            let position = state.0;
            state.0 = (position + 1) % SLOTS;
            state.1[position].replace((key, value))
        };
        drop(previous);
    }
}
