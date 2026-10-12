//! Executable identity and artifact hashing.

use super::*;

pub(crate) fn default_data_directory() -> PathBuf {
    #[cfg(windows)]
    if let Some(root) = env::var_os("LOCALAPPDATA") {
        return PathBuf::from(root).join("Acyclic").join("state-v5");
    }
    #[cfg(not(windows))]
    if let Some(root) = env::var_os("XDG_STATE_HOME") {
        return PathBuf::from(root).join("acyclic").join("state-v5");
    }
    #[cfg(not(windows))]
    if let Some(root) = env::var_os("HOME") {
        return PathBuf::from(root)
            .join(".local")
            .join("state")
            .join("acyclic")
            .join("state-v5");
    }
    env::temp_dir().join("acyclic-state-v5")
}

/// The running executable's own path. macOS reports the path it was started
/// through, which for the npm-installed command is a link outside the package.
pub(crate) fn current_executable() -> Result<PathBuf, String> {
    let executable = env::current_exe().map_err(display)?;
    #[cfg(unix)]
    let executable = executable.canonicalize().map_err(display)?;
    Ok(executable)
}

pub(crate) fn service_identity(data: &Path) -> Result<String, String> {
    service_identity_for(data, &current_executable()?)
}

/// The identity of an executable's artifact bytes. Hashing tens of megabytes
/// on every session start is avoidable: the identity is cached under the
/// file's fingerprint, which every rewrite or replacement of the file changes.
pub(crate) fn service_identity_for(data: &Path, executable: &Path) -> Result<String, String> {
    let cache = data.join("executable-identity");
    let file = fs::File::open(executable).map_err(display)?;
    let (fingerprint, changed) = executable_fingerprint(&file)?;
    if let Some(identity) = fs::read(&cache)
        .ok()
        .and_then(|cached| cached_identity(&cached, &fingerprint))
    {
        return Ok(identity);
    }
    let mut hasher = blake3::Hasher::new();
    read_executable(&file, |bytes| {
        hasher.update(bytes);
    })?;
    let identity = service_identity_from_digest(&hasher.finalize().to_hex());
    // Cache only a hash of bytes that did not change while they were read and
    // whose last change is older than any timestamp granularity: a later write
    // in the same clock tick could otherwise keep the fingerprint (the racy
    // timestamp problem Git's index solves the same way). A cache that cannot
    // be written, or is lost, only costs the next caller one hash.
    let settled = std::time::SystemTime::now()
        .duration_since(changed)
        .is_ok_and(|age| age > SETTLED_EXECUTABLE_AGE);
    if settled && executable_fingerprint(&file)?.0 == fingerprint {
        let staged = data.join(format!("executable-identity.{}", std::process::id()));
        let entry = format!("{}\n{identity}", hex::encode(fingerprint));
        if fs::write(&staged, entry)
            .and_then(|()| fs::rename(&staged, &cache))
            .is_err()
        {
            let _ = fs::remove_file(&staged);
        }
    }
    Ok(identity)
}

pub(crate) fn cached_identity(cached: &[u8], fingerprint: &[u8; 32]) -> Option<String> {
    let (cached_fingerprint, identity) = std::str::from_utf8(cached).ok()?.split_once('\n')?;
    (cached_fingerprint == hex::encode(fingerprint)
        && identity.len() == 64
        && identity
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f')))
    .then(|| identity.to_owned())
}

/// An executable changed longer ago than this has timestamps that any later
/// write must advance.
pub(crate) const SETTLED_EXECUTABLE_AGE: std::time::Duration = std::time::Duration::from_secs(1);

/// Digest of the file's identity, size, and modification and change times,
/// with the change time. The change time cannot be set by callers, so no write
/// can preserve it.
#[cfg(unix)]
pub(crate) fn executable_fingerprint(
    file: &fs::File,
) -> Result<([u8; 32], std::time::SystemTime), String> {
    use std::os::unix::fs::MetadataExt as _;
    let metadata = file.metadata().map_err(display)?;
    let mut hasher = blake3::Hasher::new();
    for field in [
        metadata.dev(),
        metadata.ino(),
        metadata.size(),
        metadata.mtime().cast_unsigned(),
        metadata.mtime_nsec().cast_unsigned(),
        metadata.ctime().cast_unsigned(),
        metadata.ctime_nsec().cast_unsigned(),
    ] {
        hasher.update(&field.to_le_bytes());
    }
    let changed = std::time::UNIX_EPOCH
        .checked_add(std::time::Duration::new(
            metadata.ctime().try_into().unwrap_or_default(),
            metadata.ctime_nsec().try_into().unwrap_or_default(),
        ))
        .unwrap_or(std::time::UNIX_EPOCH);
    Ok((*hasher.finalize().as_bytes(), changed))
}

/// Digest of the file's volume and identity, size, and write and change
/// times, with the change time. The change time cannot be set by callers, so
/// no write can preserve it.
#[cfg(windows)]
#[allow(
    unsafe_code,
    reason = "GetFileInformationByHandleEx fills fixed-size structures for a live handle"
)]
pub(crate) fn executable_fingerprint(
    file: &fs::File,
) -> Result<([u8; 32], std::time::SystemTime), String> {
    use std::os::windows::io::AsRawHandle as _;
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_BASIC_INFO, FILE_ID_INFO, FileBasicInfo, FileIdInfo, GetFileInformationByHandleEx,
    };
    fn query<T>(file: &fs::File, class: i32) -> Result<T, String> {
        let mut information = std::mem::MaybeUninit::<T>::zeroed();
        let size = u32::try_from(std::mem::size_of::<T>()).map_err(display)?;
        // SAFETY: the handle is live for the call and the buffer is exactly
        // `size` writable bytes of the structure this class returns.
        if unsafe {
            GetFileInformationByHandleEx(
                file.as_raw_handle(),
                class,
                information.as_mut_ptr().cast(),
                size,
            )
        } == 0
        {
            return Err(display(io::Error::last_os_error()));
        }
        // SAFETY: the call succeeded, so it initialized the structure.
        Ok(unsafe { information.assume_init() })
    }
    let identity = query::<FILE_ID_INFO>(file, FileIdInfo)?;
    let basic = query::<FILE_BASIC_INFO>(file, FileBasicInfo)?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(&identity.VolumeSerialNumber.to_le_bytes());
    hasher.update(&identity.FileId.Identifier);
    hasher.update(&file.metadata().map_err(display)?.len().to_le_bytes());
    for time in [basic.CreationTime, basic.LastWriteTime, basic.ChangeTime] {
        hasher.update(&time.to_le_bytes());
    }
    // FILETIME counts 100 ns intervals from 1601; Unix time starts 11,644,473,600 s later.
    let changed = u64::try_from(basic.ChangeTime)
        .ok()
        .and_then(|ticks| ticks.checked_sub(116_444_736_000_000_000))
        .and_then(|ticks| {
            std::time::UNIX_EPOCH
                .checked_add(std::time::Duration::from_nanos(ticks.saturating_mul(100)))
        })
        .unwrap_or(std::time::UNIX_EPOCH);
    Ok((*hasher.finalize().as_bytes(), changed))
}

pub(crate) fn service_identity_from_digest(digest: &str) -> String {
    blake3::hash(format!("{}:{digest}", env!("CARGO_PKG_VERSION")).as_bytes())
        .to_hex()
        .to_string()
}

pub(crate) fn read_executable(
    mut file: &fs::File,
    mut update: impl FnMut(&[u8]),
) -> Result<(), String> {
    file.rewind().map_err(display)?;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer).map_err(display)?;
        if read == 0 {
            break;
        }
        let chunk = buffer
            .get(..read)
            .ok_or_else(|| "binary hash read exceeded its buffer".to_owned())?;
        update(chunk);
    }
    Ok(())
}

pub(crate) struct ExecutableDigests {
    pub(crate) service_identity: String,
    pub(crate) sha256: String,
    pub(crate) blake3: String,
}

pub(crate) fn executable_digests() -> Result<ExecutableDigests, String> {
    executable_digests_for(&current_executable()?)
}

pub(crate) async fn executable_digests_async() -> Result<ExecutableDigests, String> {
    tokio::task::spawn_blocking(executable_digests)
        .await
        .map_err(display)?
}

pub(crate) fn executable_digests_for(executable: &Path) -> Result<ExecutableDigests, String> {
    // Launchers and plugin caches can expose the same signed artifact at different paths. The
    // service belongs to the artifact, not to one of those aliases; including the path caused
    // identical clients to continuously drain and replace each other's service.
    let mut sha256 = Sha256::new();
    let mut blake3 = blake3::Hasher::new();
    read_executable(&fs::File::open(executable).map_err(display)?, |chunk| {
        sha256.update(chunk);
        blake3.update(chunk);
    })?;
    let sha256 = hex::encode(sha256.finalize());
    let blake3 = blake3.finalize().to_hex().to_string();
    Ok(ExecutableDigests {
        service_identity: service_identity_from_digest(&blake3),
        sha256,
        blake3,
    })
}

pub(crate) fn blake3_file(path: &Path) -> Result<String, String> {
    let mut hasher = blake3::Hasher::new();
    read_executable(&fs::File::open(path).map_err(display)?, |bytes| {
        hasher.update(bytes);
    })?;
    Ok(hasher.finalize().to_hex().to_string())
}
