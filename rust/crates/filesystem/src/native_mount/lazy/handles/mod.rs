use super::*;

mod source_file;

mod view_file;

pub(in crate::native_mount::lazy) use source_file::LazyOpenFile;

pub(in crate::native_mount::lazy) use view_file::ViewBoundOpenFile;

pub(in crate::native_mount::lazy) fn untrack_open_handle(handles: &OpenIdentityHandles, file_id: FileId, ptr: *const ()) {
    let Ok(mut open) = handles.lock() else {
        return;
    };
    if let Some(entries) = open.get_mut(&file_id) {
        entries.retain(|entry| entry.as_ptr() as *const () != ptr);
        if entries.is_empty() {
            open.remove(&file_id);
        }
    }
}
