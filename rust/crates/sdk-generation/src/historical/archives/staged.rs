use super::*;
use std::io::{Seek, SeekFrom};

/// Payloads live only in private temporary files until the complete immutable
/// snapshot, all member checks and the transport trailer have been accepted.
pub(crate) struct StagedArchive {
    storage: tempfile::TempDir,
    pub files: BTreeMap<String, StagedMember>,
    limits: ArchiveLimits,
}

pub(crate) struct StagedMember {
    pub sha256: String,
    offset: u64,
    length: u64,
}

#[cfg(test)]
mod tests;

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
        let (digest, _) = transfer(
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
        let mut payload = None;
        let mut offset = 0;
        visit_archive_members(
            &mut snapshot,
            &release.package,
            &release.num,
            limits,
            |name, entry| {
                let output = payload
                    .get_or_insert_with(|| fs::File::create(staged.storage.path().join("payload")))
                    .as_mut()
                    .map_err(|e| e.to_string())?;
                let (sha256, length) = transfer(entry, output, limits.member_bytes, "member-byte")?;
                staged.files.insert(
                    name.to_owned(),
                    StagedMember {
                        sha256,
                        offset,
                        length,
                    },
                );
                offset = offset
                    .checked_add(length)
                    .ok_or("archive payload offset overflow")?;
                Ok(())
            },
        )?;
        staged.check_source(release, archive)?;
        Ok(staged)
    }

    fn reader(&self, member: &StagedMember) -> Result<std::io::Take<fs::File>, String> {
        // Each reader owns its cursor; the range cannot consume another member.
        // Archive names never become staging paths.
        let mut file =
            fs::File::open(self.storage.path().join("payload")).map_err(|e| e.to_string())?;
        file.seek(SeekFrom::Start(member.offset))
            .map_err(|e| e.to_string())?;
        Ok(file.take(member.length))
    }

    pub fn check_source(&self, release: &RegistryVersion, archive: &Path) -> Result<(), String> {
        let mut source = fs::File::open(archive).map_err(|e| e.to_string())?;
        let (digest, _) = transfer(
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
        let Some(member) = self.files.get(".cargo_vcs_info.json") else {
            return Ok(None);
        };
        let mut reader = self.reader(member)?;
        let mut bytes =
            Vec::with_capacity(usize::try_from(reader.limit()).map_err(|e| e.to_string())?);
        reader.read_to_end(&mut bytes).map_err(|e| e.to_string())?;
        if reader.limit() != 0 {
            return Err("staged archive member is truncated".into());
        }
        parse_archive_vcs(Some(&bytes))
    }

    pub fn matches(&self, member: &StagedMember, other: &Path) -> Result<bool, String> {
        let mut right = fs::File::open(other).map_err(|e| e.to_string())?;
        if right.metadata().map_err(|e| e.to_string())?.len() != member.length {
            return Ok(false);
        }
        let mut left = self.reader(member)?;
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
                return Ok(left.limit() == 0);
            }
        }
    }

    /// Installs complete members into the Cargo working tree with no-clobber
    /// visibility. This reconstructible tree has no crash-durability promise;
    /// final documentation publication owns its separate durability boundary.
    pub fn publish(&self, root: &Path, directory: &str) -> Result<(), String> {
        fs::create_dir_all(root).map_err(|e| e.to_string())?;
        let root = root.canonicalize().map_err(|e| e.to_string())?;
        // Validate every destination before the first accepted file is written.
        for name in self.files.keys() {
            validate_import_path(&root, &root.join(directory).join(name))?;
        }
        for (name, member) in &self.files {
            let path = root.join(directory).join(name);
            if path.exists() {
                if self.matches(member, &path)? {
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
            let mut input = self.reader(member)?;
            std::io::copy(&mut input, &mut output).map_err(|e| e.to_string())?;
            if input.limit() != 0 {
                return Err("staged archive member is truncated".into());
            }
            if let Err(error) = output.persist_noclobber(&path) {
                if !self.matches(member, &path)? {
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
) -> Result<(String, u64), String> {
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
    Ok((result, limit - remaining))
}
