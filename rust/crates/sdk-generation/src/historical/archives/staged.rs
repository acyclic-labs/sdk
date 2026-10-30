use super::*;
use std::io::{Seek, SeekFrom};

/// Payloads live only in private temporary files until the complete immutable
/// snapshot, all member checks and the transport trailer have been accepted.
pub(crate) struct StagedArchive {
    storage: tempfile::TempDir,
    pub files: BTreeMap<String, String>,
    limits: ArchiveLimits,
}

impl StagedArchive {
    pub fn read(
        release: &RegistryVersion,
        archive: &Path,
        limits: ArchiveLimits,
    ) -> Result<Self, String> {
        validate_name(&release.package)?;
        validate_version(&release.num)?;
        let mut staged = Self {
            storage: tempfile::tempdir().map_err(|e| e.to_string())?,
            files: BTreeMap::new(),
            limits,
        };
        let mut source = fs::File::open(archive).map_err(|e| e.to_string())?;
        if source.metadata().map_err(|e| e.to_string())?.len() > limits.compressed_bytes {
            return Err("registry archive compressed-byte limit exceeded".into());
        }
        let mut snapshot = tempfile::tempfile().map_err(|e| e.to_string())?;
        let digest = transfer(
            &mut source,
            &mut snapshot,
            limits.compressed_bytes,
            "compressed-byte",
        )?;
        if digest != format!("sha256:{}", release.checksum) {
            return Err("released registry archive checksum differs".into());
        }
        snapshot
            .seek(SeekFrom::Start(0))
            .map_err(|e| e.to_string())?;
        visit_archive_members(
            &mut snapshot,
            &release.package,
            &release.num,
            limits,
            |name, entry| {
                let mut output = fs::OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(staged.path(name))
                    .map_err(|e| e.to_string())?;
                let digest = transfer(entry, &mut output, limits.member_bytes, "member-byte")?;
                staged.files.insert(name.to_owned(), digest);
                Ok(())
            },
        )?;
        staged.check_source(release, archive)?;
        Ok(staged)
    }

    fn path(&self, name: &str) -> std::path::PathBuf {
        // Archive names never become staging paths. A digest collision refuses
        // create_new rather than replacing a previously validated member.
        self.storage
            .path()
            .join(crate::sha256_bytes(name.as_bytes()).trim_start_matches("sha256:"))
    }

    pub fn check_source(&self, release: &RegistryVersion, archive: &Path) -> Result<(), String> {
        let mut source = fs::File::open(archive).map_err(|e| e.to_string())?;
        let digest = transfer(
            &mut source,
            &mut std::io::sink(),
            self.limits.compressed_bytes,
            "compressed-byte",
        )?;
        if digest != format!("sha256:{}", release.checksum) {
            return Err("registry archive changed during import".into());
        }
        Ok(())
    }

    pub fn vcs(&self) -> Result<Option<PublisherVcs>, String> {
        let bytes = self
            .files
            .contains_key(".cargo_vcs_info.json")
            .then(|| fs::read(self.path(".cargo_vcs_info.json")))
            .transpose()
            .map_err(|e| e.to_string())?;
        parse_archive_vcs(bytes.as_deref())
    }

    pub fn matches(&self, name: &str, other: &Path) -> Result<bool, String> {
        equal_files(&self.path(name), other)
    }

    pub fn publish(&self, root: &Path, directory: &str) -> Result<(), String> {
        fs::create_dir_all(root).map_err(|e| e.to_string())?;
        let root = root.canonicalize().map_err(|e| e.to_string())?;
        // Validate every destination before the first accepted file is written.
        for name in self.files.keys() {
            validate_import_path(&root, &root.join(directory).join(name))?;
        }
        for name in self.files.keys() {
            let path = root.join(directory).join(name);
            if path.exists() {
                if self.matches(name, &path)? {
                    continue;
                }
                return Err(format!(
                    "refusing to rewrite immutable artifact {}",
                    path.display()
                ));
            }
            let parent = path.parent().ok_or("archive destination has no parent")?;
            fs::create_dir_all(parent).map_err(|e| e.to_string())?;
            let mut output = tempfile::NamedTempFile::new_in(parent).map_err(|e| e.to_string())?;
            let mut input = fs::File::open(self.path(name)).map_err(|e| e.to_string())?;
            std::io::copy(&mut input, &mut output).map_err(|e| e.to_string())?;
            output.as_file().sync_all().map_err(|e| e.to_string())?;
            if let Err(error) = output.persist_noclobber(&path) {
                if !self.matches(name, &path)? {
                    return Err(error.error.to_string());
                }
            }
        }
        Ok(())
    }
}

fn transfer<R: Read + ?Sized, W: Write>(
    reader: &mut R,
    writer: &mut W,
    limit: u64,
    label: &str,
) -> Result<String, String> {
    let mut buffer = [0; 16 * 1024];
    let mut remaining = limit;
    let mut digest = ring::digest::Context::new(&ring::digest::SHA256);
    loop {
        let read = reader.read(&mut buffer).map_err(|e| e.to_string())?;
        if read == 0 {
            break;
        }
        if read as u64 > remaining {
            return Err(format!("registry archive {label} limit exceeded"));
        }
        remaining -= read as u64;
        digest.update(&buffer[..read]);
        writer
            .write_all(&buffer[..read])
            .map_err(|e| e.to_string())?;
    }
    let mut result = String::from("sha256:");
    for byte in digest.finish().as_ref() {
        result.push(char::from(b"0123456789abcdef"[(byte >> 4) as usize]));
        result.push(char::from(b"0123456789abcdef"[(byte & 15) as usize]));
    }
    Ok(result)
}

fn equal_files(left: &Path, right: &Path) -> Result<bool, String> {
    let mut left = fs::File::open(left).map_err(|e| e.to_string())?;
    let mut right = fs::File::open(right).map_err(|e| e.to_string())?;
    if left.metadata().map_err(|e| e.to_string())?.len()
        != right.metadata().map_err(|e| e.to_string())?.len()
    {
        return Ok(false);
    }
    let mut a = [0; 16 * 1024];
    let mut b = [0; 16 * 1024];
    loop {
        let count = left.read(&mut a).map_err(|e| e.to_string())?;
        right
            .read_exact(&mut b[..count])
            .map_err(|e| e.to_string())?;
        if a[..count] != b[..count] {
            return Ok(false);
        }
        if count == 0 {
            return Ok(true);
        }
    }
}
