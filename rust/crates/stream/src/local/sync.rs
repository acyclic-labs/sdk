fn sync_file(file: &File, durability: LocalDurability) -> std::io::Result<()> {
    acyclic_native_runtime::sync_file(file, native_durability(durability), None)
}

fn sync_file_data(file: &File, durability: LocalDurability) -> std::io::Result<()> {
    acyclic_native_runtime::sync_data(file, native_durability(durability))
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
