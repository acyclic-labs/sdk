use super::*;
use acyclic_test_support::differential;

// Always release readonly files, including on an assertion or IO failure.
struct WritableOnDrop(Vec<PathBuf>);
impl Drop for WritableOnDrop {
    fn drop(&mut self) {
        for path in &self.0 {
            if let Ok(metadata) = std::fs::metadata(path) {
                let mut permissions = metadata.permissions();
                permissions.set_readonly(false);
                let _ = std::fs::set_permissions(path, permissions);
            }
        }
    }
}

#[test]
fn restores_every_readonly_bit_without_reopening_writable_entries() -> Result<()> {
    for readonly in 0_u32..16 {
        let temporary = tempfile::tempdir().map_err(storage)?;
        let source = temporary.path().join("source");
        let restored = temporary.path().join("restored");
        let relative = ["", "nested", "nested/a&%.txt", "b.txt"];
        let _writable = WritableOnDrop(
            [&source, &restored]
                .into_iter()
                .flat_map(|root| relative.map(|relative| root.join(relative)))
                .collect(),
        );
        std::fs::create_dir_all(source.join("nested")).map_err(storage)?;
        std::fs::write(source.join(relative[2]), b"bytes\0\r\n").map_err(storage)?;
        std::fs::write(source.join(relative[3]), []).map_err(storage)?;
        for (index, relative) in relative.iter().enumerate() {
            let path = source.join(relative);
            let mut permissions = std::fs::metadata(&path).map_err(storage)?.permissions();
            permissions.set_readonly(readonly & (1 << index) != 0);
            std::fs::set_permissions(path, permissions).map_err(storage)?;
        }
        let snapshot = Snapshot::capture(&source)?;
        let (result, trace) =
            differential::observe::<_, _, PermissionWrite>(|| (), |()| snapshot.restore(&restored));
        result?;
        assert_eq!(Snapshot::capture(&restored)?, snapshot);
        assert_eq!(
            trace,
            (0..readonly.count_ones())
                .map(|_| PermissionWrite)
                .collect::<Vec<_>>(),
            "writable entries incurred redundant permission writes"
        );
    }
    Ok(())
}
