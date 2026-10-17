use std::fs;
use std::io;
use std::path::Path;
use std::time::SystemTime;

pub fn cached<K: Ord, S: Clone + Eq, V: Clone, E>(
    values: &std::cell::RefCell<std::collections::BTreeMap<K, (S, V)>>,
    key: K,
    mut observe: impl FnMut() -> Result<S, E>,
    load: impl FnOnce() -> Result<V, E>,
    changed: impl FnOnce() -> E,
) -> Result<V, E> {
    let before = observe()?;
    if let Some(value) = values
        .borrow()
        .get(&key)
        .filter(|(cached, _)| *cached == before)
        .map(|(_, value)| value.clone())
    {
        return Ok(value);
    }
    let value = load()?;
    if observe()? != before {
        return Err(changed());
    }
    values.borrow_mut().insert(key, (before, value.clone()));
    Ok(value)
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct Stamp {
    length: u64,
    modified: SystemTime,
    created: Option<SystemTime>,
    #[cfg(unix)]
    native: (u64, u64, i64, i64),
    #[cfg(windows)]
    native: (u64, [u8; 16], i64),
}

pub fn stamp(path: &Path) -> io::Result<Stamp> {
    #[cfg(windows)]
    let file = {
        use std::os::windows::fs::OpenOptionsExt;
        fs::OpenOptions::new()
            .access_mode(windows_sys::Win32::Storage::FileSystem::FILE_READ_ATTRIBUTES)
            .open(path)?
    };
    #[cfg(windows)]
    let metadata = file.metadata()?;
    #[cfg(not(windows))]
    let metadata = fs::metadata(path)?;
    if !metadata.is_file() {
        return Err(io::Error::other(format!(
            "expected a regular file: {}",
            path.display()
        )));
    }
    Ok(Stamp {
        length: metadata.len(),
        modified: metadata.modified()?,
        created: metadata.created().ok(),
        #[cfg(unix)]
        native: {
            use std::os::unix::fs::MetadataExt;
            (
                metadata.dev(),
                metadata.ino(),
                metadata.ctime(),
                metadata.ctime_nsec(),
            )
        },
        #[cfg(windows)]
        native: windows_identity(&file)?,
    })
}

#[cfg(windows)]
fn windows_identity(file: &fs::File) -> std::io::Result<(u64, [u8; 16], i64)> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{
        FileBasicInfo, FileIdInfo, GetFileInformationByHandleEx, FILE_BASIC_INFO, FILE_ID_INFO,
    };
    let mut basic = std::mem::MaybeUninit::<FILE_BASIC_INFO>::uninit();
    let mut id = std::mem::MaybeUninit::<FILE_ID_INFO>::uninit();
    // SAFETY: both buffers have the API's exact type/size and outlive the calls;
    // the file owns the live handle. Read the buffers only after both succeed.
    unsafe {
        if GetFileInformationByHandleEx(
            file.as_raw_handle(),
            FileBasicInfo,
            basic.as_mut_ptr().cast(),
            std::mem::size_of::<FILE_BASIC_INFO>() as u32,
        ) == 0
            || GetFileInformationByHandleEx(
                file.as_raw_handle(),
                FileIdInfo,
                id.as_mut_ptr().cast(),
                std::mem::size_of::<FILE_ID_INFO>() as u32,
            ) == 0
        {
            return Err(std::io::Error::last_os_error());
        }
        let id = id.assume_init();
        Ok((
            id.VolumeSerialNumber,
            id.FileId.Identifier,
            basic.assume_init().ChangeTime,
        ))
    }
}
