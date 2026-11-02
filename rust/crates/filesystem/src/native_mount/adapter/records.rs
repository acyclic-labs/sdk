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
    let device = match &record.payload {
        FilePayload::Device { major, minor } => Some((*major, *minor)),
        _ => None,
    };
    MountNode {
        file_id: record.file_id,
        kind,
        logical_bytes: record.logical_file_bytes(),
        link_count: record.link_count,
        device,
    }
}
