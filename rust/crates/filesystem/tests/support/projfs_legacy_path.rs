use std::os::windows::ffi::{OsStrExt, OsStringExt};

pub(crate) fn components(wide: &[u16]) -> Option<Vec<Vec<u8>>> {
    let host_path = std::path::PathBuf::from(std::ffi::OsString::from_wide(wide));
    let mut path = Vec::<Vec<u8>>::new();
    for component in host_path.components() {
        let std::path::Component::Normal(component) = component else {
            return None;
        };
        let bytes = component
            .encode_wide()
            .flat_map(u16::to_le_bytes)
            .collect::<Vec<_>>();
        // Previous MountPath::child cloned every accumulated component.
        let mut child = path.clone();
        child.push(bytes);
        path = child;
    }
    Some(path)
}
