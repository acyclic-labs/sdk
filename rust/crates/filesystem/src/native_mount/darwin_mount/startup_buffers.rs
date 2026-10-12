use super::*;

pub(super) struct StartupBuffers {
    pub(super) destination: DarwinOwned<CString>,
    pub(super) pointers: [usize; 7],
}

impl StartupBuffers {
    pub(super) fn new(
        destination: &Path,
        writable: bool,
        named_attributes: bool,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Self, NativeMountError> {
        let destination =
            native_name(destination.as_os_str().as_bytes(), work).map_err(|error| match error {
                NativeMountError::Io(error) if error.raw_os_error() == Some(libc::EIO) => {
                    driver_errno(libc::EINVAL)
                }
                error => error,
            })?;
        let options = [
            Some(c"nobrowse"),
            (!writable).then_some(c"ro"),
            named_attributes.then_some(c"namedattr"),
        ];
        let count = 3 + 2 * (usize::from(!writable) + usize::from(named_attributes));
        let pointers = copying(
            work,
            crate::WorkCounters {
                bytes_copied: (count * std::mem::size_of::<usize>()) as u64,
                items_examined: count as u64,
                ..crate::WorkCounters::UNCHARGED
            },
            || {
                let mut pointers = [0; 7];
                pointers[0] = c"acyclic-fs".as_ptr() as usize;
                for (index, option) in options.into_iter().flatten().enumerate() {
                    pointers[2 * index + 1] = c"-o".as_ptr() as usize;
                    pointers[2 * index + 2] = option.as_ptr() as usize;
                }
                pointers
            },
        )?;
        Ok(Self {
            destination,
            pointers,
        })
    }

    pub(super) fn argument_count(&self) -> c_int {
        self.pointers
            .iter()
            .position(|pointer| *pointer == 0)
            .unwrap_or(7) as c_int
    }
}
