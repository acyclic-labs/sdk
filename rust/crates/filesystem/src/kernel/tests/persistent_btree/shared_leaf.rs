use super::*;
use crate::foundation::FileId;
use crate::kernel::tree_mutation::{TreeFormat, TreeMutation, TreeMutationError};
use crate::kernel::{FileKind, LogicalName, NameEncoding, TreeEntry};
use crate::memory::MemoryObjectStore;
use crate::{CancellationToken, WorkBudget};
use acyclic_test_support::differential::{self, pair};
use proptest::prelude::*;

thread_local! {
    static RESTORE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}
pub(super) fn restore_clones() -> bool {
    RESTORE.get()
}

#[derive(Clone)]
struct Observed(usize, TreeMutation);
impl Mutation<TreeFormat> for Observed {
    type Error = TreeMutationError;
    fn key(&self) -> &LogicalName {
        self.1.key()
    }
    fn changes_cardinality(&self) -> bool {
        self.1.changes_cardinality()
    }
    fn replacement<'a>(
        &'a self,
        current: Option<&TreeEntry>,
    ) -> Result<Option<&'a TreeEntry>, Self::Error> {
        differential::record((self.0, current.map(|entry| (entry.file_id, entry.kind))));
        self.1.replacement(current)
    }
    fn map_error(error: Error<Self::Error>) -> crate::FsError {
        TreeMutation::map_error(error)
    }
}

type Event = (usize, Option<(FileId, FileKind)>);

fn entry(key: u8, identity: u8) -> TreeEntry {
    TreeEntry {
        name: LogicalName::new(NameEncoding::PosixBytes, vec![key + 1], 255).unwrap(),
        file_id: FileId::from_bytes([identity; 16]),
        kind: FileKind::Regular,
    }
}

// Independent old ownership path. It owns a separately initialized Vec and
// deep-clones the whole leaf before applying the original ordered operations.
fn old(entries: &[TreeEntry], mutations: &[Observed]) -> Result<Vec<TreeEntry>, String> {
    let mut output = entries.to_vec();
    for mutation in mutations {
        let found = output.binary_search_by(|entry| entry.name.cmp(mutation.key()));
        let current = found.ok().and_then(|index| output.get(index));
        differential::record((mutation.0, current.map(|entry| (entry.file_id, entry.kind))));
        let next = match &mutation.1 {
            TreeMutation::Insert(_) if current.is_some() => return Err("AlreadyExists".into()),
            TreeMutation::Insert(entry) => Some(entry.clone()),
            TreeMutation::Remove {
                expected_file_id, ..
            } => {
                let current = current.ok_or("Missing")?;
                if expected_file_id.is_some_and(|id| id != current.file_id) {
                    return Err("FileIdentityConflict".into());
                }
                None
            }
            TreeMutation::Replace {
                entry,
                expected_file_id,
            } => {
                let current = current.ok_or("Missing")?;
                if current.file_id != *expected_file_id {
                    return Err("FileIdentityConflict".into());
                }
                Some(entry.clone())
            }
        };
        match (found, next) {
            (Ok(index), Some(next)) => output[index] = next,
            (Ok(index), None) => {
                output.remove(index);
            }
            (Err(index), Some(next)) => output.insert(index, next),
            (Err(_), None) => {}
        }
    }
    Ok(output)
}

// Frozen v1 wire writer: no candidate page view, codec, or decoder is used.
fn old_bytes(entries: &[TreeEntry]) -> Vec<u8> {
    let mut output = b"ACYFSTRE".to_vec();
    output.extend_from_slice(&1_u16.to_le_bytes());
    output.push(1);
    output.extend_from_slice(&u32::try_from(entries.len()).unwrap().to_le_bytes());
    for entry in entries {
        output.push(match entry.name.encoding() {
            NameEncoding::Utf8 => 1,
            NameEncoding::PosixBytes => 2,
            NameEncoding::WindowsUtf16Le => 3,
        });
        let name = entry.name.as_bytes();
        output.extend_from_slice(&u32::try_from(name.len()).unwrap().to_le_bytes());
        output.extend_from_slice(name);
        output.extend_from_slice(&entry.file_id.into_bytes());
        output.push(match entry.kind {
            FileKind::Regular => 1,
            FileKind::Directory => 2,
            FileKind::SymbolicLink => 3,
            FileKind::Fifo => 4,
            FileKind::Socket => 5,
            FileKind::CharacterDevice => 6,
            FileKind::BlockDevice => 7,
            FileKind::ReparsePoint => 8,
            FileKind::MountBoundary => 9,
        });
    }
    output
}

fn bytes(entries: Entries<'_, TreeEntry>) -> Vec<u8> {
    let cancellation = CancellationToken::new();
    let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let (encoded, allocation) =
        TreeFormat::encode_with_accounting(&PageRef::Leaf(entries), 128, &mut accounting).unwrap();
    let size = usize_to_u64(encoded.capacity());
    let output = encoded.clone();
    drop(encoded);
    retire_allocation(allocation, size, &mut accounting).unwrap();
    output
}

fn candidate(
    entries: &[TreeEntry],
    mutations: &[Observed],
    restore: bool,
) -> (Result<Vec<u8>, String>, u64) {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            RESTORE.set(false);
        }
    }
    RESTORE.set(restore);
    let _reset = Reset;
    let cancellation = CancellationToken::new();
    let store = MemoryObjectStore::default();
    let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
    let mut values = Owned::allocate(entries.len(), &mut accounting).unwrap();
    for entry in entries {
        let (value, mut allocation) =
            persistent_io::clone_value::<TreeFormat>(entry, 255, &mut accounting, None, 0).unwrap();
        values.bytes += TreeFormat::value_owned_bytes(&value);
        values.allocation.absorb(&mut allocation).unwrap();
        values.value.push(value);
    }
    let (shared, _) =
        super::super::entries::share_values(values, TreeFormat::value_owned_bytes, &mut accounting)
            .unwrap();
    let visited = VisitedObjectSet::new_with_accounting(1, &mut accounting).unwrap();
    let mut context = Context::<_, TreeFormat, Observed> {
        store: &store,
        limits: DecodeLimits::default(),
        accounting: &mut accounting,
        visited,
        maximum_seen_height: 0,
        cancellation: &cancellation,
        marker: PhantomData,
    };
    let indexed: Vec<_> = mutations.iter().enumerate().collect();
    let mut result = None;
    let measured = acyclic_test_support::measure(|| {
        result = Some(context.apply_shared_mutations(&shared.value, &indexed));
    });
    let result = match result.unwrap() {
        Ok(result) => {
            for item in &result.value {
                if let Some(prior) = shared.value.iter().find(|prior| ***prior == **item) {
                    if !restore {
                        assert!(Arc::ptr_eq(prior, item));
                    }
                }
            }
            let encoded = bytes(Entries::Shared(&result.value));
            result.retire(context.accounting).unwrap();
            Ok(encoded)
        }
        Err(error) => Err(format!(
            "{:?}",
            crate::kernel::tree_mutation::project_tree_error(error)
        )),
    };
    context.visited.retire(context.accounting).unwrap();
    shared.retire(&mut accounting).unwrap();
    assert_eq!(accounting.standalone_parts().unwrap().0.live_bytes(), 0);
    (result, measured.count_total)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]
    #[test]
    fn shared_mutations_preserve_full_bytes_errors_and_observer_trace(
        keys in prop::collection::btree_set(0_u8..32, 0..25),
        operations in prop::collection::vec((0_u8..3, 0_u8..32, 0_u8..3), 0..17),
    ) {
        let results = pair(
            || {
                let entries: Vec<_> = keys.iter().map(|key| entry(*key, 1)).collect();
                let mut mutations: Vec<_> = operations.iter().enumerate().map(|(index, &(op, key, id))| {
                    Observed(index, match op {
                        0 => TreeMutation::Insert(entry(key, id)),
                        1 => TreeMutation::Replace { entry: entry(key, id), expected_file_id: FileId::from_bytes([1; 16]) },
                        _ => TreeMutation::Remove { name: entry(key, 1).name, expected_file_id: None },
                    })
                }).collect();
                mutations.sort_by(|left, right| left.key().cmp(right.key()).then(left.0.cmp(&right.0)));
                (entries, mutations)
            },
            |(entries, mutations), baseline| {
                if baseline { old(&entries, &mutations).map(|output| old_bytes(&output)) }
                else { candidate(&entries, &mutations, false).0 }
            },
        );
        let [(old, old_trace), (new, new_trace)]: [(_, Vec<Event>); 2] = results;
        prop_assert_eq!(new, old);
        prop_assert_eq!(new_trace, old_trace);
    }
}

fn exact_replacement_allocations(restore: bool) {
    for count in [8_u8, 24] {
        let entries: Vec<_> = (0..count).map(|key| entry(key, 1)).collect();
        let mutations = [Observed(
            0,
            TreeMutation::Replace {
                entry: entry(3, 2),
                expected_file_id: FileId::from_bytes([1; 16]),
            },
        )];
        let (result, allocations) = candidate(&entries, &mutations, restore);
        result.unwrap();
        // One Arc vector, one changed name, and one changed entry Arc.
        assert_eq!(allocations, 3, "unchanged leaf entries were deep-cloned");
    }
}

#[test]
fn cached_leaf_replacement_allocates_only_for_the_changed_entry() {
    exact_replacement_allocations(false);
}

#[test]
#[should_panic(expected = "unchanged leaf entries were deep-cloned")]
fn restored_deep_clones_fail_the_same_exact_allocation_pin() {
    exact_replacement_allocations(true);
}
