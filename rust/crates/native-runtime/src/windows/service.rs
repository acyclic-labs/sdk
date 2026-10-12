//! Detached service startup with one inherited readiness pipe.
use std::{fs::File, io, mem::zeroed, path::Path};
use std::os::windows::{ffi::OsStrExt as _, io::{AsRawHandle, FromRawHandle}};
use windows_sys::Win32::{
    Foundation::{CloseHandle, HANDLE, HANDLE_FLAG_INHERIT, INVALID_HANDLE_VALUE, SetHandleInformation},
    Security::SECURITY_ATTRIBUTES,
    System::{
        Console::{GetStdHandle, STD_OUTPUT_HANDLE, SetStdHandle},
        Pipes::CreatePipe,
        Threading::{
            CREATE_NEW_PROCESS_GROUP, CreateProcessW, DETACHED_PROCESS,
            DeleteProcThreadAttributeList, EXTENDED_STARTUPINFO_PRESENT, InitializeProcThreadAttributeList,
            LPPROC_THREAD_ATTRIBUTE_LIST, PROC_THREAD_ATTRIBUTE_HANDLE_LIST, PROCESS_INFORMATION,
            STARTF_USESTDHANDLES, STARTUPINFOEXW, UpdateProcThreadAttribute,
        },
    },
};

pub(crate) fn spawn_service_process(executable: &Path) -> io::Result<File> {
    let executable_argument = executable.as_os_str().encode_wide().collect::<Vec<_>>();
    if executable_argument.contains(&0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "service executable path contains NUL",
        ));
    }
    if executable_argument.contains(&u16::from(b'"')) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "service executable path contains a quote",
        ));
    }
    let mut executable_wide = executable_argument.clone();
    executable_wide.push(0);
    let mut command_line = Vec::with_capacity(executable_argument.len() + 32);
    command_line.push(u16::from(b'"'));
    command_line.extend(executable_argument);
    command_line.extend(format!("\" __service {}\0", crate::SERVICE_READY_ARGUMENT).encode_utf16());

    let (reader, writer) = readiness_pipe()?;
    let writer_handle: HANDLE = writer.as_raw_handle();
    let mut attributes = HandleListAttribute::new(&writer_handle)?;
    // SAFETY: zero is the documented initial state for these Win32 outputs.
    let mut startup: STARTUPINFOEXW = unsafe { zeroed() };
    startup.StartupInfo.cb = u32::try_from(std::mem::size_of::<STARTUPINFOEXW>())
        .map_err(|_| io::Error::other("STARTUPINFOEXW size does not fit u32"))?;
    startup.StartupInfo.dwFlags = STARTF_USESTDHANDLES;
    startup.StartupInfo.hStdOutput = writer_handle;
    startup.lpAttributeList = attributes.list();
    // SAFETY: CreateProcessW initializes every returned handle on success.
    let mut process: PROCESS_INFORMATION = unsafe { zeroed() };
    // SAFETY: both buffers are live and NUL-terminated. Inheritance is limited to the attribute list's single handle, which outlives the call.
    let created = unsafe {
        CreateProcessW(
            executable_wide.as_ptr(),
            command_line.as_mut_ptr(),
            std::ptr::null(),
            std::ptr::null(),
            1,
            CREATE_NEW_PROCESS_GROUP | DETACHED_PROCESS | EXTENDED_STARTUPINFO_PRESENT,
            std::ptr::null(),
            std::ptr::null(),
            &raw const startup.StartupInfo,
            &mut process,
        )
    };
    if created == 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: successful creation transfers both owned handles to this process.
    unsafe {
        CloseHandle(process.hThread);
        CloseHandle(process.hProcess);
    }
    drop(attributes);
    drop(writer);
    Ok(reader)
}

fn readiness_pipe() -> io::Result<(File, File)> {
    let attributes = SECURITY_ATTRIBUTES {
        nLength: u32::try_from(std::mem::size_of::<SECURITY_ATTRIBUTES>())
            .map_err(|_| io::Error::other("SECURITY_ATTRIBUTES size does not fit u32"))?,
        lpSecurityDescriptor: std::ptr::null_mut(),
        bInheritHandle: 1,
    };
    let mut reader: HANDLE = std::ptr::null_mut();
    let mut writer: HANDLE = std::ptr::null_mut();
    // SAFETY: both outputs are writable and the attributes are live.
    if unsafe { CreatePipe(&mut reader, &mut writer, &raw const attributes, 0) } == 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: CreatePipe returned two owned handles.
    let (reader, writer) =
        unsafe { (File::from_raw_handle(reader), File::from_raw_handle(writer)) };
    // SAFETY: the reader handle is live.
    if unsafe { SetHandleInformation(reader.as_raw_handle(), HANDLE_FLAG_INHERIT, 0) } == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok((reader, writer))
}

struct HandleListAttribute {
    buffer: Vec<u8>,
}

impl HandleListAttribute {
    fn new(handle: &HANDLE) -> io::Result<Self> {
        let mut size = 0;
        // SAFETY: a null list with a size output queries the required size; this documented probe fails with ERROR_INSUFFICIENT_BUFFER.
        unsafe { InitializeProcThreadAttributeList(std::ptr::null_mut(), 1, 0, &mut size) };
        let mut buffer = vec![0; size];
        // SAFETY: the buffer holds exactly the size the probe requested.
        if unsafe { InitializeProcThreadAttributeList(buffer.as_mut_ptr().cast(), 1, 0, &mut size) }
            == 0
        {
            return Err(io::Error::last_os_error());
        }
        let mut attribute = Self { buffer };
        // SAFETY: the list is initialized, and `handle` outlives it because the caller keeps both alive across process creation.
        if unsafe {
            UpdateProcThreadAttribute(
                attribute.list(),
                0,
                PROC_THREAD_ATTRIBUTE_HANDLE_LIST as usize,
                std::ptr::from_ref(handle).cast(),
                std::mem::size_of::<HANDLE>(),
                std::ptr::null_mut(),
                std::ptr::null(),
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(attribute)
    }

    fn list(&mut self) -> LPPROC_THREAD_ATTRIBUTE_LIST {
        self.buffer.as_mut_ptr().cast()
    }
}

impl Drop for HandleListAttribute {
    fn drop(&mut self) {
        // SAFETY: a constructed attribute always holds an initialized list.
        unsafe { DeleteProcThreadAttributeList(self.list()) };
    }
}

pub(crate) fn take_standard_output() -> io::Result<Option<File>> {
    // SAFETY: querying a standard handle has no preconditions.
    let handle = unsafe { GetStdHandle(STD_OUTPUT_HANDLE) };
    if handle.is_null() || handle == INVALID_HANDLE_VALUE {
        return Ok(None);
    }
    // SAFETY: clearing a standard handle has no preconditions; this process now solely owns the handle it held.
    if unsafe { SetStdHandle(STD_OUTPUT_HANDLE, std::ptr::null_mut()) } == 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the handle is live and no longer reachable as standard output.
    Ok(Some(unsafe { File::from_raw_handle(handle) }))
}
