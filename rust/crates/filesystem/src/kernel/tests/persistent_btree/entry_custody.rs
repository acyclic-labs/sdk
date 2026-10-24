use super::*;
use crate::foundation::FileId;
use crate::kernel::file_table_mutation::FileTableFormat;
use crate::kernel::persistent_btree::{Page, shared::SharedPage};
use crate::kernel::{FileKind, FilePayload, FileRecord};
use crate::storage::{ObjectId, ObjectKind};
use crate::{CancellationToken, Digest, NativeMaterializationWork, WorkBudget};

fn check<const DETACH: bool>() {
    loom::model(|| {
        let cancellation = CancellationToken::new();
        let original =
            NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).unwrap();
        let resource = original.native_volume_work(WorkBudget::UNBOUNDED).unwrap();
        let baseline = original.live_allocation_bytes();
        let mut accounting = KernelAccounting::native(&resource, &cancellation);
        let mut values = Owned::allocate(1, &mut accounting).unwrap();
        values.value.push(FileRecord {
            file_id: FileId::from_bytes([1; 16]),
            kind: FileKind::Fifo,
            link_count: 1,
            metadata: ObjectId {
                kind: ObjectKind::Metadata,
                digest: Digest::ZERO,
            },
            payload: FilePayload::Empty,
        });
        let Owned {
            value,
            allocation,
            bytes,
        } = values;
        let mut page = SharedPage::<FileTableFormat>::from_owned(
            Owned::new(Page::Leaf(value), allocation, bytes),
            &mut accounting,
        )
        .unwrap();
        let SharedPage::Leaf { entries, .. } = &mut page.value else {
            unreachable!()
        };
        let entry = entries.first_mut().unwrap();
        if DETACH {
            // Deliberately restore page-scoped funding: an entry surviving the
            // page no longer retains the original allocation that paid for it.
            let entry = Arc::get_mut(entry).unwrap();
            let old = std::mem::replace(
                &mut entry._allocation,
                accounting.claim_owned(0, 0).unwrap(),
            );
            drop(old);
        }
        let surviving_entry = entry.clone();
        let retained = surviving_entry.bytes;
        page.retire(&mut accounting).unwrap();
        let reader_work = original.clone();
        let reader = loom::thread::spawn(move || {
            // Only the production allocation-resource mutex is observed here;
            // no page or cache lock is held while borrowing the surviving entry.
            assert_eq!(surviving_entry.file_id, FileId::from_bytes([1; 16]));
            let funded = reader_work.live_allocation_bytes() == baseline + retained;
            drop(surviving_entry);
            funded
        });
        cancellation.cancel();
        assert!(reader.join().unwrap(), "entry lost its original funding");
        assert_eq!(original.live_allocation_bytes(), baseline);
    });
}

#[test]
fn entry_custody_survives_its_original_page_and_cancellation() {
    check::<false>();
}

#[test]
#[should_panic(expected = "entry lost its original funding")]
fn page_scoped_entry_funding_is_detected() {
    check::<true>();
}
