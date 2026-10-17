fn sync_file(file: &File, durability: LocalDurability) -> std::io::Result<()> {
    acyclic_native_runtime::sync_file(file, native_durability(durability), None)
}

#[derive(Clone, Copy)]
struct DeviceFlushed;

enum SyncCompletion {
    Ordered,
    Flushed(DeviceFlushed),
}

#[derive(Clone, Copy, Default)]
struct SyncProgress {
    ordered: u64,
    flushed: Option<u64>,
}

impl SyncProgress {
    fn completion(self, through: u64, durability: LocalDurability) -> Option<SyncCompletion> {
        match durability {
            LocalDurability::FullFlush if self.flushed.is_some_and(|cut| cut >= through) => {
                Some(SyncCompletion::Flushed(DeviceFlushed))
            }
            LocalDurability::Barrier if self.ordered >= through => Some(SyncCompletion::Ordered),
            _ => None,
        }
    }
}

fn sync_file_data(file: &File, durability: LocalDurability) -> std::io::Result<SyncCompletion> {
    acyclic_native_runtime::sync_data(file, native_durability(durability))?;
    Ok(match durability {
        LocalDurability::FullFlush => SyncCompletion::Flushed(DeviceFlushed),
        LocalDurability::Barrier => SyncCompletion::Ordered,
    })
}

fn native_durability(durability: LocalDurability) -> acyclic_native_runtime::Durability {
    match durability {
        LocalDurability::FullFlush => acyclic_native_runtime::Durability::Full,
        LocalDurability::Barrier => acyclic_native_runtime::Durability::Barrier,
    }
}

fn sync_directory(path: &Path, durability: LocalDurability) -> Result<(), LocalStreamError> {
    acyclic_native_runtime::sync_parent(path, native_durability(durability))?;
    Ok(())
}
