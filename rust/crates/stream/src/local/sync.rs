fn sync_file(file: &File, durability: LocalDurability) -> std::io::Result<()> {
    acyclic_native_runtime::sync_file(file, native_durability(durability), None)
}

#[derive(Clone, Copy)]
struct DeviceFlushed;

#[derive(Clone, Copy)]
enum SyncCompletion {
    Ordered,
    Flushed(DeviceFlushed),
}

use acyclic_native_runtime::LogFlushProgress as SyncProgress;

trait SyncCompletionFor {
    fn completion(self, through: u64, durability: LocalDurability) -> Option<SyncCompletion>;
}

// The submitted producer owns the flush cut even after its caller leaves.
// Losing it must terminate every waiter on that cut.
struct FlushOwner {
    inner: Arc<LocalInner>,
    active: bool,
}

impl Drop for FlushOwner {
    fn drop(&mut self) {
        if self.active {
            self.inner
                .fail(LocalStreamFailure::new("flush", "producer_lost"));
        }
    }
}

impl SyncCompletionFor for SyncProgress {
    fn completion(self, through: u64, durability: LocalDurability) -> Option<SyncCompletion> {
        self.covers(through, native_durability(durability))
            .then_some(match durability {
                LocalDurability::FullFlush => SyncCompletion::Flushed(DeviceFlushed),
                LocalDurability::Barrier => SyncCompletion::Ordered,
            })
    }
}

fn sync_file_data(
    file: &acyclic_native_runtime::FileFlush<'_>,
    durability: LocalDurability,
) -> std::io::Result<SyncCompletion> {
    Ok(match file.sync_data(native_durability(durability))? {
        acyclic_native_runtime::Durability::Full => SyncCompletion::Flushed(DeviceFlushed),
        acyclic_native_runtime::Durability::Barrier => SyncCompletion::Ordered,
    })
}

fn native_durability(durability: LocalDurability) -> acyclic_native_runtime::Durability {
    match durability {
        LocalDurability::FullFlush => acyclic_native_runtime::Durability::Full,
        LocalDurability::Barrier => acyclic_native_runtime::Durability::Barrier,
    }
}
