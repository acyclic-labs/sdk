use super::*;

/// One accepted SDK mutation and its completed physical prefix.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NativeMaterializationActionPhase {
    Before,
    Completed,
}

pub(super) async fn generation_action<E, F, Fut>(
    control: &mut Option<&mut F>,
    phase: NativeMaterializationActionPhase,
) -> Result<(), E>
where
    F: FnMut(NativeMaterializationActionPhase) -> Fut,
    Fut: Future<Output = Result<(), E>>,
{
    if let Some(control) = control {
        control(phase).await?;
    }
    Ok(())
}

/// Native traversal and host work share the original ledger; standalone SDK reads contribute their measured prefixes.
pub(super) struct GenerationReceipt {
    pub(super) value: MaterializationReceipt,
    pub(super) custody: crate::materializer::NativeAllocation,
}

impl GenerationReceipt {
    pub(super) fn from_work(
        work: &crate::NativeMaterializationWork,
    ) -> Result<Self, MaterializeError> {
        let custody = work.claim(0, 0, crate::WorkCounters::UNCHARGED)?;
        Ok(Self {
            value: MaterializationReceipt::default(),
            custody,
        })
    }

    pub(super) fn runtime_error(&self, error: std::io::Error) -> MaterializeError {
        // Native runtime admission reports an I/O marker while retaining its
        // typed cause on this original ledger.
        self.custody.work().runtime_error(error).into()
    }

    pub(super) fn adopt(&mut self, bytes: usize) -> Result<(), MaterializeError> {
        let retained = self
            .custody
            .bytes()
            .checked_add(bytes as u64)
            .ok_or(MaterializeError::Work(WorkError::Overflow))?;
        let result = self.custody.reconcile_capacity(retained);
        result.map_err(MaterializeError::from)
    }

    pub(super) fn owned_arc<T>(
        &mut self,
        value: T,
        retained: usize,
    ) -> Result<Arc<(T, crate::NativeAllocation)>, MaterializeError> {
        let bytes = std::mem::size_of::<(T, crate::NativeAllocation)>();
        self.custody.grow(bytes as u64)?;
        let bytes = retained
            .checked_add(bytes)
            .ok_or(MaterializeError::Work(WorkError::Overflow))?;
        let custody = self.custody.split_off(bytes as u64)?;
        Ok(Arc::new((value, custody)))
    }

    pub(super) fn reserve<T>(
        &mut self,
        values: &mut Vec<T>,
        additional: usize,
    ) -> Result<(), MaterializeError> {
        let required = values
            .len()
            .checked_add(additional)
            .ok_or(MaterializeError::Work(WorkError::Overflow))?;
        let prior = values
            .capacity()
            .checked_mul(std::mem::size_of::<T>())
            .ok_or(MaterializeError::Work(WorkError::Overflow))? as u64;
        if self.custody.bytes() < prior {
            let mut prefix = self.custody.work().claim(
                prior - self.custody.bytes(),
                0,
                crate::WorkCounters::UNCHARGED,
            )?;
            self.custody.absorb(&mut prefix)?;
        }
        let capacity = if required > values.capacity() {
            required
                .max(
                    values
                        .capacity()
                        .checked_mul(2)
                        .ok_or(MaterializeError::Work(WorkError::Overflow))?,
                )
                .max(1)
        } else {
            values.capacity()
        };
        crate::materializer::reserve_buffer(values, capacity, &mut self.custody)
            .map_err(MaterializeError::from)
    }

    pub(super) fn push<T>(
        &mut self,
        values: &mut Vec<T>,
        value: T,
    ) -> Result<(), MaterializeError> {
        if values.len() == values.capacity() {
            let additional = values.capacity().max(1);
            self.reserve(values, additional)?;
        }
        values.push(value);
        Ok(())
    }

    pub(super) fn accept_source<T>(
        &mut self,
        value: T,
        custody: crate::NativeAllocation,
    ) -> Result<T, MaterializeError> {
        let mut owned = (value, custody);
        let result = self.custody.absorb(&mut owned.1);
        result?;
        Ok(owned.0)
    }

    pub(super) fn engine_failure(
        &self,
        failure: OperationFailure<crate::FsError>,
    ) -> MaterializeError {
        let error = MaterializeError::from(failure.error);
        match self.custody.work().clone().merge(*failure.work) {
            Ok(()) => error,
            Err(failure) => failure.into(),
        }
    }
}

pub(super) fn clone_namespace(
    path: &NamespacePath,
    extra: Option<&LogicalName>,
    limits: crate::model::VolumeLimits,
    receipt: &mut GenerationReceipt,
) -> Result<NamespacePath, MaterializeError> {
    let work = receipt.custody.work().clone();
    let cancellation = work.cancellation();
    let mut accounting = crate::kernel::KernelAccounting::native(&work, cancellation);
    let (path, allocation) = match extra {
        Some(name) => path.child_with_accounting(name, limits, &mut accounting, cancellation),
        None => path.clone_with_accounting(&mut accounting, cancellation),
    }?;
    let crate::kernel::KernelOwnedAllocation::NativeOriginal(mut custody) = allocation else {
        return Err(crate::NativeTreeMaterializationError::InvalidLayout.into());
    };
    receipt.custody.absorb(&mut custody)?;
    Ok(path)
}

#[cfg(test)]
mod allocation_properties;

#[cfg(unix)]
pub(super) type HostPathUnit = u8;
#[cfg(windows)]
pub(super) type HostPathUnit = u16;

pub(super) type HeldHostPath = Arc<(Vec<HostPathUnit>, crate::NativeAllocation)>;
pub(super) type HeldNamespacePath = Arc<(NamespacePath, crate::NativeAllocation)>;

pub(super) fn materialize_host_path(
    prefix: Option<&Vec<HostPathUnit>>,
    names: &[LogicalName],
    receipt: &mut GenerationReceipt,
) -> Result<HeldHostPath, MaterializeError> {
    let prefix = prefix.map_or(&[][..], |prefix| prefix.as_slice());
    let mut length = prefix
        .len()
        .checked_add(names.len().saturating_sub(1))
        .and_then(|length| length.checked_add(usize::from(!prefix.is_empty() && !names.is_empty())))
        .ok_or(MaterializeError::Work(WorkError::Overflow))?;
    for name in names {
        receipt.custody.work().charge(WorkCounters {
            items_examined: 1,
            ..WorkCounters::UNCHARGED
        })?;
        #[cfg(unix)]
        let count = {
            if !matches!(
                name.encoding(),
                NameEncoding::Utf8 | NameEncoding::PosixBytes
            ) {
                return Err(MaterializeError::UnrepresentableName);
            }
            name.as_bytes().len()
        };
        #[cfg(windows)]
        let count = match name.encoding() {
            NameEncoding::WindowsUtf16Le => name.as_bytes().len() / 2,
            NameEncoding::Utf8 | NameEncoding::PosixBytes => {
                let text = std::str::from_utf8(name.as_bytes())
                    .map_err(|_| MaterializeError::UnrepresentableName)?;
                let mut count = 0_usize;
                for _ in text.encode_utf16() {
                    receipt.custody.work().charge(WorkCounters {
                        items_examined: 1,
                        ..WorkCounters::UNCHARGED
                    })?;
                    count += 1;
                }
                count
            }
        };
        length = length
            .checked_add(count)
            .ok_or(MaterializeError::Work(WorkError::Overflow))?;
    }
    let mut units = Vec::new();
    receipt.reserve(&mut units, length)?;
    #[cfg(unix)]
    receipt.custody.work().charge(WorkCounters {
        bytes_copied: length as u64,
        ..WorkCounters::UNCHARGED
    })?;
    #[cfg(windows)]
    receipt.custody.work().charge(WorkCounters {
        bytes_copied: prefix.len() as u64 * 2,
        bytes_encoded: (length - prefix.len()) as u64 * 2,
        ..WorkCounters::UNCHARGED
    })?;
    units.extend_from_slice(prefix);
    for name in names {
        #[cfg(unix)]
        let separator = b'/';
        #[cfg(windows)]
        let separator = u16::from(b'\\');
        if !units.is_empty() {
            units.push(separator);
        }
        #[cfg(unix)]
        units.extend_from_slice(name.as_bytes());
        #[cfg(windows)]
        match name.encoding() {
            NameEncoding::WindowsUtf16Le => units.extend(
                crate::kernel::types::utf16le_units(name.as_bytes())
                    .ok_or_else(|| MaterializeError::UnrepresentableName)?,
            ),
            NameEncoding::Utf8 | NameEncoding::PosixBytes => units.extend(
                std::str::from_utf8(name.as_bytes())
                    .map_err(|_| MaterializeError::UnrepresentableName)?
                    .encode_utf16(),
            ),
        }
    }
    let retained = vec_bytes(&units);
    receipt.owned_arc(units, retained)
}

#[cfg(unix)]
pub(super) fn host_path(bytes: &[u8]) -> &Path {
    use std::os::unix::ffi::OsStrExt as _;
    Path::new(OsStr::from_bytes(bytes))
}

pub(super) static RESTORE_SEQUENCE: AtomicU64 = AtomicU64::new(0);
