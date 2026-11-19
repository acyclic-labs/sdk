use std::path::{Path, PathBuf};

pub fn discover(root: &Path, paths: &mut Vec<PathBuf>) -> std::io::Result<()> {
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            discover(&path, paths)?;
        } else if path.extension().is_some_and(|ext| ext == "json")
            && path
                .file_name()
                .is_some_and(|name| name != "_defaults.json")
        {
            paths.push(path);
        }
    }
    Ok(())
}
