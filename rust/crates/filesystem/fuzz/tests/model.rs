//! Exhaustive schedules through the production native directory cache.
#![cfg(acyclic_fs_loom)]
use acyclic_fs::{Digest, ObjectId, ObjectKind};

#[path = "../../src/sync.rs"]
mod sync;

fn key(byte: u8) -> ObjectId {
    ObjectId {
        kind: ObjectKind::TreePage,
        digest: Digest::from_bytes([byte; 32]),
    }
}

mod cache {
    include!("../../src/native_mount/adapter/folded_cache.rs");
    impl<T> FoldedDirectoryCache<T> {
        // Negative control: writers can select the same slot without one section.
        pub(super) fn insert_split(&self, key: ObjectId, value: DirectoryNames<T>) {
            let position = {
                let state = self
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if state.1.iter().flatten().any(|(tree, _)| *tree == key) {
                    return;
                }
                state.0
            };
            let previous = {
                let mut state = self
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                state.0 = (position + 1) % SLOTS;
                state.1[position].replace((key, value))
            };
            drop(previous);
        }

        pub(super) fn unlocked(&self) -> bool {
            self.state.try_lock().is_ok()
        }

        // Negative control: payload retirement may acquire an accounting lock.
        pub(super) fn insert_dropping_locked(&self, key: ObjectId, value: DirectoryNames<T>) {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.1.iter().flatten().any(|(tree, _)| *tree == key) {
                return;
            }
            let position = state.0;
            state.0 = (position + 1) % SLOTS;
            let previous = state.1[position].replace((key, value));
            drop(previous);
        }
    }
}

#[test]
fn immutable_directory_results_are_never_torn_or_lost() {
    admission_model(insert);
}

#[test]
#[should_panic(expected = "cache admission lost")]
fn split_admission_reproduces_a_lost_result() {
    admission_model(insert_split);
}

fn admission_model(insert: fn(&cache::FoldedDirectoryCache<bool>, ObjectId, bool)) {
    loom::model(move || {
        let cache = loom::sync::Arc::new(cache::FoldedDirectoryCache::<bool>::new());
        let writer = loom::sync::Arc::clone(&cache);
        let thread = loom::thread::spawn(move || {
            insert(&writer, key(1), true);
            insert(&writer, key(2), false);
        });
        insert(&cache, key(3), true);
        assert!(cache.get(key(1)).map(result).is_none_or(|value| value));
        assert!(cache.get(key(2)).map(result).is_none_or(|value| !value));
        assert_eq!(cache.get(key(4)).map(result), None);
        thread.join().expect("writer completes without deadlock");
        assert_eq!(
            cache.get(key(1)).map(result),
            Some(true),
            "cache admission lost"
        );
        assert_eq!(
            cache.get(key(2)).map(result),
            Some(false),
            "cache admission lost"
        );
        assert_eq!(
            cache.get(key(3)).map(result),
            Some(true),
            "cache admission lost"
        );
    });
}

#[test]
fn eviction_and_duplicate_admission_keep_other_results_intact() {
    eviction_model(insert);
}

#[test]
#[should_panic(expected = "cache eviction invariant")]
fn split_eviction_reproduces_a_lost_result() {
    eviction_model(insert_split);
}

fn eviction_model(insert: fn(&cache::FoldedDirectoryCache<bool>, ObjectId, bool)) {
    loom::model(move || {
        let cache = loom::sync::Arc::new(cache::FoldedDirectoryCache::<bool>::new());
        for index in 0..64 {
            insert(&cache, key(index), index % 2 == 0);
        }
        let writer = loom::sync::Arc::clone(&cache);
        let thread = loom::thread::spawn(move || {
            insert(&writer, key(65), true);
            insert(&writer, key(65), true);
        });
        insert(&cache, key(66), false);
        assert_eq!(cache.get(key(63)).map(result), Some(false));
        thread.join().expect("writer completes without deadlock");
        assert_eq!(
            cache.get(key(0)).map(result),
            None,
            "cache eviction invariant"
        );
        assert_eq!(
            cache.get(key(1)).map(result),
            None,
            "cache eviction invariant"
        );
        assert_eq!(
            cache.get(key(2)).map(result),
            Some(true),
            "cache eviction invariant"
        );
        assert_eq!(
            cache.get(key(65)).map(result),
            Some(true),
            "cache eviction invariant"
        );
        assert_eq!(
            cache.get(key(66)).map(result),
            Some(false),
            "cache eviction invariant"
        );
    });
}

fn insert(cache: &cache::FoldedDirectoryCache<bool>, key: ObjectId, value: bool) {
    cache.insert(
        key,
        if value {
            cache::DirectoryNames::Unique
        } else {
            cache::DirectoryNames::Collisions(loom::sync::Arc::new(false))
        },
    );
}

fn insert_split(cache: &cache::FoldedDirectoryCache<bool>, key: ObjectId, value: bool) {
    cache.insert_split(
        key,
        if value {
            cache::DirectoryNames::Unique
        } else {
            cache::DirectoryNames::Collisions(loom::sync::Arc::new(false))
        },
    );
}

fn result(value: cache::DirectoryNames<bool>) -> bool {
    match value {
        cache::DirectoryNames::Unique => true,
        cache::DirectoryNames::Collisions(value) => *value,
    }
}

struct Retired(Option<Box<dyn Fn() + Send + Sync>>);

impl Drop for Retired {
    fn drop(&mut self) {
        if let Some(check) = &self.0 {
            check();
        }
    }
}

#[test]
fn payload_retirement_obeys_the_terminal_lock_order() {
    retirement_model(cache::FoldedDirectoryCache::insert);
}

#[test]
#[should_panic(expected = "payload retired with cache locked")]
fn locked_retirement_is_detected() {
    retirement_model(cache::FoldedDirectoryCache::insert_dropping_locked);
}

fn retirement_model(
    insert: fn(&cache::FoldedDirectoryCache<Retired>, ObjectId, cache::DirectoryNames<Retired>),
) {
    loom::model(move || {
        let cache = loom::sync::Arc::new(cache::FoldedDirectoryCache::new());
        let owner = loom::sync::Arc::clone(&cache);
        cache.insert(
            key(0),
            cache::DirectoryNames::Collisions(loom::sync::Arc::new(Retired(Some(Box::new(
                move || {
                    assert!(owner.unlocked(), "payload retired with cache locked");
                },
            ))))),
        );
        for index in 1..64 {
            cache.insert(key(index), cache::DirectoryNames::Unique);
        }
        insert(&cache, key(64), cache::DirectoryNames::Unique);
    });
}
