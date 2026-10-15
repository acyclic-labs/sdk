use super::{FileKind, FilePayload, FileRecord, MountNode, MountNodeKind};

pub(super) fn mount_node(record: FileRecord) -> MountNode {
    let kind = match record.kind {
        FileKind::Regular => MountNodeKind::Regular,
        FileKind::Directory => MountNodeKind::Directory,
        FileKind::SymbolicLink => MountNodeKind::SymbolicLink,
        FileKind::Fifo => MountNodeKind::Fifo,
        FileKind::Socket => MountNodeKind::Socket,
        FileKind::CharacterDevice => MountNodeKind::CharacterDevice,
        FileKind::BlockDevice => MountNodeKind::BlockDevice,
        FileKind::ReparsePoint | FileKind::MountBoundary => MountNodeKind::Unsupported,
    };
    let logical_bytes = match &record.payload {
        FilePayload::InlineRegular(data) => {
            u64::try_from(data.as_bytes().len()).unwrap_or(u64::MAX)
        }
        FilePayload::Regular { logical_bytes, .. } => *logical_bytes,
        FilePayload::SymbolicLink { target_bytes, .. } => *target_bytes,
        FilePayload::Directory { .. }
        | FilePayload::Empty
        | FilePayload::Device { .. }
        | FilePayload::ReparsePoint { .. } => 0,
    };
    let device = match &record.payload {
        FilePayload::Device { major, minor } => Some((*major, *minor)),
        _ => None,
    };
    MountNode {
        file_id: record.file_id,
        kind,
        logical_bytes,
        link_count: record.link_count,
        device,
    }
}
