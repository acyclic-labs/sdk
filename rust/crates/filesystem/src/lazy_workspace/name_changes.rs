use super::*;
use crate::demand::{SourceChange, SourceChangeSink, SourceWatch, native::NativeDemandSource};
use tokio::sync::watch;

/// Bounded native namespace delta. Overflow or uncertain delivery requires
/// discovery; it is never represented as an empty successful change set.
#[derive(Default)]
pub struct NativeNameDelta {
    /// Names whose bindings changed. Content-only notifications need no names.
    pub paths: std::collections::BTreeSet<String>,
    /// Some affected names are unknown.
    pub all: bool,
    /// Accepted replay progress, published atomically with this delta and any
    /// resulting discovery continuations. Live callback hints leave this unset.
    pub journal: Option<crate::NameJournalCheckpoint>,
}

struct NameSignal {
    revision: watch::Sender<u64>,
    pending: Arc<Mutex<PendingNames>>,
}

#[derive(Default)]
struct PendingNames {
    // Retained callback allocations keep custody on their original native
    // ledger until the async consumer takes the delta or observation stops.
    paths: Option<Owned<Vec<Owned<String>>>>,
    all: bool,
}

impl PendingNames {
    fn mark_all(&mut self) {
        self.all = true;
        self.paths = None;
    }

    fn into_delta(self) -> NativeNameDelta {
        NativeNameDelta {
            all: self.all,
            journal: None,
            paths: self.paths.map_or_else(Default::default, |paths| {
                paths.value.iter().map(|path| path.value.clone()).collect()
            }),
        }
    }
}

fn is_ignore_policy(name: &LogicalName) -> bool {
    [".gitignore", ".ignore"]
        .iter()
        .any(|policy| name_is(name, policy))
}

fn name_is(name: &LogicalName, text: &str) -> bool {
    if name.encoding() == crate::kernel::NameEncoding::WindowsUtf16Le {
        name.as_bytes()
            .iter()
            .copied()
            .eq(text.encode_utf16().flat_map(u16::to_le_bytes))
    } else {
        name.as_bytes() == text.as_bytes()
    }
}

fn root_exclude_change(path: &NamespacePath, binding: bool) -> bool {
    let parts = path.components();
    let target = [".git", "info", "exclude"];
    !parts.is_empty()
        && parts.len() <= target.len()
        && (binding || parts.len() == target.len())
        && parts
            .iter()
            .zip(target)
            .all(|(name, text)| name_is(name, text))
}

impl SourceChangeSink for NameSignal {
    fn logical_owned_bytes(&self) -> u64 {
        std::mem::size_of::<Self>() as u64
    }

    fn source_changed(
        &self,
        changes: &[SourceChange],
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), DemandError> {
        accounting.check().map_err(DemandError::from)?;
        if !changes.is_empty() {
            let mut pending = self
                .pending
                .lock()
                .map_err(|_| DemandError::SourceUnavailable)?;
            for change in changes {
                match change {
                    SourceChange::Everything | SourceChange::Unconfirmed => pending.mark_all(),
                    SourceChange::Name(path) | SourceChange::Entry(path) => {
                        if root_exclude_change(path, matches!(change, SourceChange::Name(_))) {
                            // Replacing either parent changes the root policy
                            // as surely as editing the exclude file itself.
                            pending.mark_all();
                            continue;
                        }
                        let policy = path.components().last().is_some_and(is_ignore_policy);
                        if matches!(change, SourceChange::Entry(_)) && !policy {
                            continue;
                        }
                        if pending.all {
                            continue;
                        }
                        if path.is_root() {
                            pending.mark_all();
                            continue;
                        }
                        if path
                            .components()
                            .iter()
                            .any(|name| name.unicode_utf8_bytes().is_none())
                        {
                            // The string-based mention surface cannot name this
                            // entry. Keep observation live and let discovery
                            // publish an explicit unavailable-entry count.
                            pending.mark_all();
                            continue;
                        }
                        let stop = cancellation(accounting).clone();
                        let (mut text, allocation) = path
                            .unicode_text_with_accounting(accounting, &stop)
                            .map_err(DemandError::from)?;
                        text.remove(0);
                        let bytes = text.capacity() as u64;
                        if policy {
                            if let Some(separator) = text.rfind('/') {
                                // Rebuild the affected policy's directory so
                                // both newly ignored and newly admitted names
                                // are replaced without touching its siblings.
                                text.truncate(separator);
                            } else {
                                Owned::new(text, allocation, bytes)
                                    .retire(accounting)
                                    .map_err(DemandError::from)?;
                                pending.mark_all();
                                continue;
                            }
                        }
                        let text = Owned::new(text, allocation, bytes);
                        if pending.paths.as_ref().is_some_and(|paths| {
                            paths.value.iter().any(|path| path.value == text.value)
                        }) {
                            text.retire(accounting).map_err(DemandError::from)?;
                            continue;
                        }
                        if pending
                            .paths
                            .as_ref()
                            .is_some_and(|paths| paths.value.len() == 512)
                        {
                            text.retire(accounting).map_err(DemandError::from)?;
                            pending.mark_all();
                        } else {
                            let paths = match &mut pending.paths {
                                Some(paths) => paths,
                                slot @ None => match Owned::allocate(1, accounting) {
                                    Ok(paths) => slot.insert(paths),
                                    Err(error) => {
                                        text.retire(accounting).map_err(DemandError::from)?;
                                        return Err(error.into());
                                    }
                                },
                            };
                            if let Err(error) = paths.ensure_for_push(512, accounting) {
                                text.retire(accounting).map_err(DemandError::from)?;
                                return Err(error.into());
                            }
                            paths.value.push(text);
                        }
                    }
                    SourceChange::Node(_) => {}
                }
            }
            drop(pending);
            self.revision
                .send_modify(|revision| *revision = revision.wrapping_add(1));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::facade::FsWork as _;

    fn path(text: &str, windows: bool) -> NamespacePath {
        let (encoding, bytes) = if windows {
            (
                crate::kernel::NameEncoding::WindowsUtf16Le,
                text.encode_utf16().flat_map(u16::to_le_bytes).collect(),
            )
        } else {
            (crate::kernel::NameEncoding::Utf8, text.as_bytes().to_vec())
        };
        NamespacePath::new(
            vec![LogicalName::new(encoding, bytes, 255).unwrap()],
            crate::model::VolumeLimits::default(),
        )
        .unwrap()
    }

    #[test]
    fn name_search_native_signal_retains_and_releases_original_custody() {
        let stop = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &stop)
            .map_err(|(error, _)| error)
            .unwrap();
        let baseline = original.live_allocation_bytes();
        let (revision, _receiver) = watch::channel(0);
        let signal = NameSignal {
            revision,
            pending: Arc::new(Mutex::new(PendingNames::default())),
        };
        signal
            .source_changed(
                &[SourceChange::Name(path("retained.txt", false))],
                &mut KernelAccounting::native(&original, &stop),
            )
            .unwrap();
        assert!(original.live_allocation_bytes() > baseline);
        let delta = std::mem::take(&mut *signal.pending.lock().unwrap()).into_delta();
        assert!(delta.paths.contains("retained.txt"));
        assert_eq!(original.live_allocation_bytes(), baseline);

        // Negative control: the old callback accepted names without making
        // any allocation claim, even when its allocation budget was zero.
        let budget = WorkBudget {
            peak_allocation_bytes: 0,
            ..WorkBudget::UNBOUNDED
        };
        assert!(
            signal
                .source_changed(
                    &[SourceChange::Name(path("unfunded.txt", false))],
                    &mut budget.accounting(&stop),
                )
                .is_err()
        );
        assert!(signal.pending.lock().unwrap().paths.is_none());
    }

    #[test]
    fn name_search_native_signal_bounds_pending_names_and_marks_overflow() {
        let (revision, receiver) = watch::channel(0);
        let signal = NameSignal {
            revision,
            pending: Arc::new(Mutex::new(PendingNames::default())),
        };
        let stop = CancellationToken::new();
        for i in 0..513 {
            signal
                .source_changed(
                    &[SourceChange::Name(path(&format!("file-{i}"), false))],
                    &mut WorkBudget::UNBOUNDED.accounting(&stop),
                )
                .unwrap();
            assert!(
                signal
                    .pending
                    .lock()
                    .unwrap()
                    .paths
                    .as_ref()
                    .is_none_or(|p| p.value.len() <= 512)
            );
        }
        let pending = signal.pending.lock().unwrap();
        assert!(pending.all);
        assert!(pending.paths.is_none());
        assert_eq!(*receiver.borrow(), 513);
        // Negative control: an unbounded fold retains every distinct event.
        assert!((0..513).collect::<std::collections::BTreeSet<_>>().len() > 512);
    }

    #[test]
    fn name_search_native_policy_notifications_decode_windows_names() {
        let (revision, _receiver) = watch::channel(0);
        let signal = NameSignal {
            revision,
            pending: Arc::new(Mutex::new(PendingNames::default())),
        };
        let policy = path(".gitignore", true);
        // Negative control: comparing raw bytes with UTF-8 misses this policy.
        assert_ne!(
            policy.components().first().unwrap().as_bytes(),
            b".gitignore"
        );
        signal
            .source_changed(
                &[SourceChange::Entry(policy)],
                &mut WorkBudget::UNBOUNDED.accounting(&CancellationToken::new()),
            )
            .unwrap();
        assert!(signal.pending.lock().unwrap().all);
    }

    #[test]
    fn name_search_native_root_exclude_changes_reload_policy_in_both_encodings() {
        for windows in [false, true] {
            for binding in [false, true] {
                for (text, expected) in [
                    (".git", binding),
                    (".git/info", binding),
                    (".git/info/exclude", true),
                    (".git/index", false),
                    (".git/info/other", false),
                ] {
                    let (revision, _receiver) = watch::channel(0);
                    let signal = NameSignal {
                        revision,
                        pending: Arc::new(Mutex::new(PendingNames::default())),
                    };
                    let policy = NamespacePath::new(
                        text.split('/')
                            .map(|name| path(name, windows).components()[0].clone())
                            .collect(),
                        crate::model::VolumeLimits::default(),
                    )
                    .unwrap();
                    // Negative control: basename-only policy recognition misses
                    // exclude and its replaced parents in either encoding.
                    assert!(!is_ignore_policy(policy.components().last().unwrap()));
                    let event = if binding {
                        SourceChange::Name(policy)
                    } else {
                        SourceChange::Entry(policy)
                    };
                    signal
                        .source_changed(
                            &[event],
                            &mut WorkBudget::UNBOUNDED.accounting(&CancellationToken::new()),
                        )
                        .unwrap();
                    assert_eq!(signal.pending.lock().unwrap().all, expected, "{text}");
                }
            }
        }
    }

    #[test]
    fn name_search_native_nested_policy_edits_only_dirty_their_directory() {
        for windows in [false, true] {
            for created in [false, true] {
                let (revision, _receiver) = watch::channel(0);
                let signal = NameSignal {
                    revision,
                    pending: Arc::new(Mutex::new(PendingNames::default())),
                };
                let policy = NamespacePath::new(
                    ["nested", ".gitignore"]
                        .into_iter()
                        .map(|name| path(name, windows).components()[0].clone())
                        .collect(),
                    crate::model::VolumeLimits::default(),
                )
                .unwrap();
                let event = if created {
                    SourceChange::Name(policy)
                } else {
                    SourceChange::Entry(policy)
                };
                signal
                    .source_changed(
                        &[event],
                        &mut WorkBudget::UNBOUNDED.accounting(&CancellationToken::new()),
                    )
                    .unwrap();
                let delta = std::mem::take(&mut *signal.pending.lock().unwrap()).into_delta();
                assert!(!delta.all);
                assert_eq!(delta.paths, ["nested".into()].into());
                // Negative control: the old policy handling discarded every
                // known path and forced whole-volume rediscovery.
                let mut previous = PendingNames::default();
                previous.mark_all();
                assert!(previous.into_delta().all);
            }
        }
    }

    #[test]
    fn name_search_native_signal_keeps_following_after_a_non_unicode_name() {
        let (revision, receiver) = watch::channel(0);
        let signal = NameSignal {
            revision,
            pending: Arc::new(Mutex::new(PendingNames::default())),
        };
        let name =
            LogicalName::new(crate::kernel::NameEncoding::PosixBytes, vec![0xff], 255).unwrap();
        // Negative control: projecting the original event into a string fails.
        assert!(
            name.unicode_text(WorkBudget::UNBOUNDED, &CancellationToken::new())
                .is_err()
        );
        let native = NamespacePath::new(vec![name], crate::model::VolumeLimits::default()).unwrap();
        signal
            .source_changed(
                &[SourceChange::Name(native)],
                &mut WorkBudget::UNBOUNDED.accounting(&CancellationToken::new()),
            )
            .unwrap();
        signal
            .source_changed(
                &[SourceChange::Name(path("healthy.txt", false))],
                &mut WorkBudget::UNBOUNDED.accounting(&CancellationToken::new()),
            )
            .unwrap();
        assert_eq!(*receiver.borrow(), 2);
        assert!(signal.pending.lock().unwrap().all);
    }
}

/// One shared producer's native source subscription. Notifications only wake
/// index maintenance; clients continue to follow the committed index files.
pub struct NativeNameChanges {
    source: Arc<NativeDemandSource>,
    revision: watch::Receiver<u64>,
    watch: Box<dyn SourceWatch>,
    pending: Arc<Mutex<PendingNames>>,
}

impl NativeNameChanges {
    /// Waits for a host change, then advances the source epoch. A consumer
    /// cancels its old walk before starting work at the resulting fresh cut.
    pub async fn changed(&mut self) -> Result<NativeNameDelta, LazyWorkspaceError> {
        self.revision
            .changed()
            .await
            .map_err(|_| DemandError::SourceUnavailable)?;
        self.revision.borrow_and_update();
        if !self.watch.is_exact() {
            return Err(DemandError::SourceUnavailable.into());
        }
        self.source.invalidate();
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| DemandError::SourceUnavailable)?;
        Ok(std::mem::take(&mut *pending).into_delta())
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore, S: LazyWorkspaceStore>
    LazyWorkspace<A, O, NativeDemandSource, S>
{
    /// Captures journal evidence from the source's held root before starting
    /// a fresh name walk. Reopening its path could observe a replacement root.
    ///
    /// # Errors
    /// Returns unavailable-journal, invalid native evidence, or original-work
    /// errors. A caller must not substitute a later cursor for this baseline.
    #[cfg(windows)]
    pub fn capture_indexed_native_journal(
        &self,
        work: &mut crate::NativeMaterializationWork,
    ) -> Result<crate::NameJournalCheckpoint, crate::WindowsUsnError> {
        self.source.capture_name_journal(work)
    }

    /// Rejects old pinned reads and native cursors after persisted metadata
    /// proves that the source changed while no subscription was running.
    pub fn invalidate_indexed_native_metadata(&self) {
        self.source.invalidate();
    }

    /// Reinstalls native observation from an indexed metadata name. This
    /// deliberately bypasses authored directory bindings, which can coexist
    /// with unindexed native children. It performs no directory enumeration.
    pub async fn observe_indexed_native_metadata(
        &self,
        snapshot: LazySnapshotRef,
        path: &str,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Option<crate::demand::SourceVersion>, LazyWorkspaceError> {
        self.source_metadata_version_at_snapshot(snapshot, path, accounting)
            .await
    }

    /// Subscribes before pathname discovery, so a completed index can remain
    /// live without a timer or a periodic directory scan.
    pub fn follow_native_names(
        &self,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Option<NativeNameChanges>, LazyWorkspaceError> {
        let (sender, revision) = watch::channel(0);
        let pending = Arc::new(Mutex::new(PendingNames::default()));
        let watch = self.watch_source(
            Arc::new(NameSignal {
                revision: sender,
                pending: Arc::clone(&pending),
            }),
            accounting,
        )?;
        Ok(watch.map(|watch| NativeNameChanges {
            source: Arc::clone(&self.source),
            revision,
            watch,
            pending,
        }))
    }
}
