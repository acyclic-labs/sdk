//! Windows change-journal observations through held directory handles.

#![allow(
    unsafe_code,
    reason = "reads the USN journal through Win32 volume control calls"
)]

use crate::windows_usn_codec::WindowsUsnError;
use std::mem::size_of;
use std::os::windows::io::{AsRawHandle, BorrowedHandle};
use windows::Win32::Foundation::{ERROR_JOURNAL_NOT_ACTIVE, HANDLE};
use windows::Win32::System::IO::DeviceIoControl;
use windows::Win32::System::Ioctl::{FSCTL_QUERY_USN_JOURNAL, USN_JOURNAL_DATA_V0};

/// Journal position observed on one Windows volume.
pub struct JournalSnapshot {
    /// Identity of the current volume change journal.
    pub journal_id: u64,
    /// First USN that the current journal can still represent.
    pub lowest_valid_usn: u64,
    /// First USN outside the observed journal boundary.
    pub next_usn: u64,
}

/// Reads the journal for the volume that owns the supplied held directory.
///
/// # Errors
///
/// Returns a native I/O error or refuses invalid journal metadata.
pub fn query_journal(root: BorrowedHandle<'_>) -> Result<JournalSnapshot, WindowsUsnError> {
    let mut data = USN_JOURNAL_DATA_V0::default();
    let mut returned = 0_u32;
    let output_size = u32::try_from(size_of::<USN_JOURNAL_DATA_V0>())
        .map_err(|_| WindowsUsnError::InvalidJournal)?;
    let handle = HANDLE(root.as_raw_handle());
    // SAFETY: `root` holds a valid directory handle for the duration of the call;
    // the output pointer references a correctly sized initialized structure;
    // no overlapped operation outlives these stack values.
    unsafe {
        DeviceIoControl(
            handle,
            FSCTL_QUERY_USN_JOURNAL,
            None,
            0,
            Some((&raw mut data).cast()),
            output_size,
            Some(&raw mut returned),
            None,
        )
    }
    .map_err(|error| {
        if error.code() == ERROR_JOURNAL_NOT_ACTIVE.to_hresult() {
            WindowsUsnError::JournalInactive
        } else {
            WindowsUsnError::Native(error.to_string())
        }
    })?;
    if returned < output_size || data.NextUsn < 0 || data.LowestValidUsn < 0 {
        return Err(WindowsUsnError::InvalidJournal);
    }
    Ok(JournalSnapshot {
        journal_id: data.UsnJournalID,
        lowest_valid_usn: u64::try_from(data.LowestValidUsn)
            .map_err(|_| WindowsUsnError::InvalidJournal)?,
        next_usn: u64::try_from(data.NextUsn).map_err(|_| WindowsUsnError::InvalidJournal)?,
    })
}
