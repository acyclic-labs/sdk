use super::*;

pub(super) const unsafe fn end_directory(
    _callback_data: *const PRJ_CALLBACK_DATA,
    _enumeration_id: *const GUID,
) -> HRESULT {
    HR_OK
}

pub(super) const unsafe fn get_directory(
    _callback_data: *const PRJ_CALLBACK_DATA,
    _enumeration_id: *const GUID,
    _search_expression: PCWSTR,
    _buffer: PRJ_DIR_ENTRY_BUFFER_HANDLE,
) -> HRESULT {
    HR_OK
}

pub(super) const unsafe fn placeholder(_callback_data: *const PRJ_CALLBACK_DATA) -> HRESULT {
    HR_FILE_NOT_FOUND
}

pub(super) const unsafe fn file_data(
    _callback_data: *const PRJ_CALLBACK_DATA,
    _byte_offset: u64,
    _length: u32,
) -> HRESULT {
    HR_UNEXPECTED
}
