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
    pub(super) fn new(
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<Self, (MaterializeError, WorkCounters)> {
        let original = crate::NativeMaterializationWork::new(budget, cancellation)
            .map_err(|(error, work)| (materialize_native_error(error), work))?;
        Self::from_work(&original).map_err(|error| (error, original.receipt()))
    }

    pub(super) fn from_work(
        work: &crate::NativeMaterializationWork,
    ) -> Result<Self, MaterializeError> {
        let custody = work
            .claim(0, 0, crate::WorkCounters::UNCHARGED)
            .map_err(materialize_native_error)?;
        Ok(Self {
            value: MaterializationReceipt::default(),
            custody,
        })
    }

    pub(super) fn run<T>(
        &self,
        operation: impl FnOnce(&mut NativeSourceWork<'_>) -> Result<T, DemandError>,
    ) -> Result<T, MaterializeError> {
        let result = operation(&mut NativeSourceWork::from_native_materialization_work(
            self.custody.work(),
        ));
        result.map_err(materialize_demand_error)
    }

    pub(super) fn adopt(&mut self, bytes: usize) -> Result<(), MaterializeError> {
        let retained = self
            .custody
            .bytes()
            .checked_add(bytes as u64)
            .ok_or(MaterializeError::Work(WorkError::Overflow))?;
        let result = self.custody.reconcile_capacity(retained);
        result.map_err(materialize_native_error)
    }

    pub(super) fn owned_arc<T>(
        &mut self,
        value: T,
        retained: usize,
    ) -> Result<Arc<(T, crate::NativeAllocation)>, MaterializeError> {
        let bytes = std::mem::size_of::<(T, crate::NativeAllocation)>();
        self.custody
            .grow(bytes as u64)
            .map_err(materialize_native_error)?;
        let bytes = retained
            .checked_add(bytes)
            .ok_or(MaterializeError::Work(WorkError::Overflow))?;
        let custody = self
            .custody
            .split_off(bytes as u64)
            .map_err(materialize_native_error)?;
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
        let work = self.custody.work().clone();
        crate::core_state::reserve(values, required, &work, &mut self.custody)
            .map_err(materialize_native_error)
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
        result.map_err(materialize_native_error)?;
        Ok(owned.0)
    }

    pub(super) fn engine_failure(
        &self,
        failure: OperationFailure<crate::FsError>,
    ) -> MaterializeError {
        let error = MaterializeError::from(failure.error);
        match self.run(|work| work.merge(*failure.work)) {
            Ok(()) => error,
            Err(failure) => failure,
        }
    }
}

pub(super) fn materialize_runtime_error(error: std::io::Error) -> MaterializeError {
    if let Some(work) = error
        .get_ref()
        .and_then(|error| error.downcast_ref::<WorkError>())
    {
        MaterializeError::Work(*work)
    } else if let Some(demand) = error
        .get_ref()
        .and_then(|error| error.downcast_ref::<DemandError>())
    {
        match demand {
            DemandError::Work(work) => MaterializeError::Work(*work),
            DemandError::Cancelled => MaterializeError::Cancelled(crate::CancellationError),
            _ => MaterializeError::Io(error),
        }
    } else {
        MaterializeError::Io(error)
    }
}

pub(super) fn materialize_native_error(
    error: crate::NativeTreeMaterializationError,
) -> MaterializeError {
    match error {
        crate::NativeTreeMaterializationError::Work(error) => MaterializeError::Work(error),
        crate::NativeTreeMaterializationError::Cancelled => {
            MaterializeError::Cancelled(crate::CancellationError)
        }
        crate::NativeTreeMaterializationError::Io(error) => MaterializeError::Io(error),
        error => MaterializeError::NativeWork(error),
    }
}

pub(super) fn materialize_demand_error(error: DemandError) -> MaterializeError {
    match error {
        DemandError::Work(error) => MaterializeError::Work(error),
        DemandError::Cancelled => MaterializeError::Cancelled(crate::CancellationError),
        DemandError::Io(error) => MaterializeError::Io(error),
        #[cfg(target_os = "linux")]
        DemandError::LinuxMetadata(error) => MaterializeError::UnsupportedMetadata(error.0),
        #[cfg(target_os = "macos")]
        DemandError::MacMetadata(error) => MaterializeError::UnsupportedMetadata(error.0),
        #[cfg(windows)]
        DemandError::WindowsMetadata(error) => MaterializeError::UnsupportedMetadata(error.0),
        error => MaterializeError::Demand(error),
    }
}

pub(super) fn clone_namespace_measured(
    path: &NamespacePath,
    extra: Option<&LogicalName>,
    limits: crate::model::VolumeLimits,
    receipt: &mut GenerationReceipt,
) -> Result<NamespacePath, MaterializeError> {
    let result = receipt
        .custody
        .work()
        .allocations(|ledger, work, budget, cancellation| {
            let result = match extra {
                Some(name) => {
                    path.child_with_ledger(name, limits, ledger, work, budget, cancellation)
                }
                None => path.clone_with_ledger(ledger, work, budget, cancellation),
            };
            result.map_err(|error| match error {
                crate::kernel::NamespacePathError::Work(error) => {
                    crate::NativeTreeMaterializationError::Work(error)
                }
                crate::kernel::NamespacePathError::Cancelled => {
                    crate::NativeTreeMaterializationError::Cancelled
                }
                _ => crate::NativeTreeMaterializationError::InvalidLayout,
            })
        });
    let (path, mut custody) = result.map_err(materialize_native_error)?;
    let result = receipt.custody.absorb(&mut custody);
    result.map_err(materialize_native_error)?;
    Ok(path)
}

#[cfg(unix)]
pub(super) type HostPathUnit = u8;
#[cfg(windows)]
pub(super) type HostPathUnit = u16;

pub(super) fn materialize_host_path(
    prefix: Option<&Vec<HostPathUnit>>,
    names: &[LogicalName],
    receipt: &mut GenerationReceipt,
) -> Result<Arc<(Vec<HostPathUnit>, crate::NativeAllocation)>, MaterializeError> {
    let prefix = prefix.map_or(&[][..], |prefix| prefix.as_slice());
    let mut length = prefix
        .len()
        .checked_add(names.len().saturating_sub(1))
        .and_then(|length| length.checked_add(usize::from(!prefix.is_empty() && !names.is_empty())))
        .ok_or(MaterializeError::Work(WorkError::Overflow))?;
    for name in names {
        receipt.run(|work| {
            work.charge(WorkCounters {
                items_examined: 1,
                ..WorkCounters::UNCHARGED
            })
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
                    receipt.run(|work| {
                        work.charge(WorkCounters {
                            items_examined: 1,
                            ..WorkCounters::UNCHARGED
                        })
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
    receipt.run(|work| {
        work.charge(WorkCounters {
            bytes_copied: length as u64,
            ..WorkCounters::UNCHARGED
        })
    })?;
    #[cfg(windows)]
    receipt.run(|work| {
        work.charge(WorkCounters {
            bytes_copied: prefix.len() as u64 * 2,
            bytes_encoded: (length - prefix.len()) as u64 * 2,
            ..WorkCounters::UNCHARGED
        })
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
