use acyclic_fs::{GuardedAppend, Head, ProposedCommit, PublicationPermit};
use acyclic_fs_format::identity::{AuthorityId, Digest, OperationId};
use acyclic_fs_work::performance::{WorkBudget, WorkCounters};

pub(super) const BUDGET: WorkBudget = WorkBudget {
    authority_records_read: 32,
    authority_records_appended: 32,
    authority_bytes_read: 131_072,
    authority_bytes_written: 131_072,
    backend_read_operations: 32,
    backend_write_operations: 32,
    bytes_hashed: 131_072,
    bytes_copied: 131_072,
    items_examined: 128,
    allocation_operations: 32,
    peak_allocation_bytes: 131_072,
    ..WorkCounters::UNCHARGED
};

pub(super) fn request(id: AuthorityId, head: Head) -> GuardedAppend {
    GuardedAppend {
        authority_id: id,
        epoch: head.epoch,
        expected: head,
        commit: ProposedCommit {
            publication: None,
            operation_id: OperationId::from_bytes([0x91; 16]),
            fingerprint: Digest::from_bytes([0x92; 32]),
            payload: bytes::Bytes::from_static(b"canonical configuration bytes"),
        },
        permit: PublicationPermit::Unrestricted,
    }
}
