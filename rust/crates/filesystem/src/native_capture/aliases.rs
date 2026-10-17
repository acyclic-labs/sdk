use super::*;

#[derive(Clone, Copy)]
struct ObservedAlias {
    file_id: crate::FileId,
    host_identity: Option<[u8; 16]>,
    single_host_group: bool,
    observed_links: u64,
    expected_links: u64,
}

pub(super) struct CaptureAliasPlan(Vec<ObservedAlias>);

pub(super) fn detached_file_id(
    file_table: crate::foundation::Digest,
    old: crate::FileId,
    host: &HostStat,
    path: &NamespacePath,
    meter: &NativeMaterializationWork,
) -> Result<crate::FileId, CaptureError> {
    // A mounted checkout can retain one authored operation across captures. Its
    // ordinary path-derived ID is still held by the other aliases. Derive a new
    // identity from this exact pre-capture state, stable across retries of it.
    let domain = b"acyclic-fs-capture-detached-file-v1\0";
    let mut hashed = (domain.len() + 32 + 16 + 16) as u64;
    for component in path.components() {
        capture_charge(
            meter,
            crate::materializer::NativeResourceCredit::Items.delta(1),
        )?;
        hashed = hashed
            .checked_add(9)
            .and_then(|bytes| bytes.checked_add(component.as_bytes().len() as u64))
            .ok_or(WorkError::Overflow)?;
    }
    capture_charge(
        meter,
        WorkCounters {
            bytes_hashed: hashed,
            ..WorkCounters::UNCHARGED
        },
    )?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(domain);
    hasher.update(file_table.as_bytes());
    hasher.update(&old.into_bytes());
    hasher.update(&host.identity().to_bytes());
    for component in path.components() {
        hasher.update(&[component.encoding().tag()]);
        hasher.update(&(component.as_bytes().len() as u64).to_le_bytes());
        hasher.update(component.as_bytes());
    }
    let mut bytes = [0; 16];
    bytes.copy_from_slice(
        hasher
            .finalize()
            .as_bytes()
            .get(..16)
            .ok_or(WorkError::Overflow)?,
    );
    Ok(crate::FileId::from_bytes(bytes))
}

impl CaptureAliasPlan {
    // Host nlink includes names outside the capture root. Reuse a shared SDK record only
    // when every old binding is in scope and its surviving regular files still share one inode.
    pub(super) fn new(
        states: &[CapturePathState],
        meter: &NativeMaterializationWork,
    ) -> Result<Self, CaptureError> {
        let mut aliases: Vec<ObservedAlias> = Vec::new();
        for (observation, _, current) in states {
            capture_charge(
                meter,
                crate::materializer::NativeResourceCredit::Items.delta(1),
            )?;
            let Some(record) =
                current.filter(|record| record.kind == FileKind::Regular && record.link_count > 1)
            else {
                continue;
            };
            let host_identity = observation.as_ref().and_then(|host| {
                (host.file_type() == crate::demand::SourceNodeKind::RegularFile)
                    .then(|| host.identity().to_bytes())
            });
            capture_reserve(&mut aliases, 1, meter)?;
            aliases.push(ObservedAlias {
                file_id: record.file_id,
                host_identity,
                single_host_group: true,
                observed_links: 1,
                expected_links: record.link_count,
            });
        }
        capture_sort_by(
            &mut aliases,
            |left, right| left.file_id.cmp(&right.file_id),
            meter,
        )?;
        let mut groups = 0;
        for index in 0..aliases.len() {
            capture_charge(
                meter,
                crate::materializer::NativeResourceCredit::Items.delta(1),
            )?;
            let next = *aliases.get(index).ok_or(WorkError::Overflow)?;
            if groups > 0
                && aliases.get(groups - 1).ok_or(WorkError::Overflow)?.file_id == next.file_id
            {
                let prior = aliases.get_mut(groups - 1).ok_or(WorkError::Overflow)?;
                prior.observed_links = checked_increment(prior.observed_links)?;
                if let Some(identity) = next.host_identity {
                    if prior.host_identity.is_some_and(|prior| prior != identity) {
                        prior.single_host_group = false;
                    } else {
                        prior.host_identity = Some(identity);
                    }
                }
                prior.single_host_group &= prior.expected_links == next.expected_links;
            } else {
                capture_charge(
                    meter,
                    crate::materializer::NativeResourceCredit::Copy
                        .delta(std::mem::size_of::<ObservedAlias>() as u64),
                )?;
                *aliases.get_mut(groups).ok_or(WorkError::Overflow)? = next;
                groups += 1;
            }
        }
        aliases.truncate(groups);
        Ok(Self(aliases))
    }

    pub(super) fn requires_detach(
        &self,
        current: Option<FileRecord>,
        meter: &NativeMaterializationWork,
    ) -> Result<bool, CaptureError> {
        let Some(record) = current else {
            return Ok(false);
        };
        let (mut low, mut high) = (0, self.0.len());
        while low < high {
            capture_charge(
                meter,
                crate::materializer::NativeResourceCredit::Items.delta(1),
            )?;
            let middle = low + (high - low) / 2;
            let alias = self.0.get(middle).ok_or(WorkError::Overflow)?;
            match alias.file_id.cmp(&record.file_id) {
                std::cmp::Ordering::Less => low = middle + 1,
                std::cmp::Ordering::Greater => high = middle,
                std::cmp::Ordering::Equal => {
                    return Ok(!alias.single_host_group
                        || alias.host_identity.is_none()
                        || alias.observed_links != alias.expected_links);
                }
            }
        }
        Ok(false)
    }

    pub(super) fn release(self, meter: &NativeMaterializationWork) -> Result<(), CaptureError> {
        let bytes = vector_bytes(&self.0);
        drop(self);
        meter.clone().release(bytes).map_err(capture_demand_error)
    }
}
