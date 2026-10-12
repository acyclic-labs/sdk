use super::*;

use crate::model::{ModelContent, ModelMessage, ModelRole};

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn partial_field_copy_releases_original_custody_and_cancelled_copy_is_empty() {
    use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(
        WorkBudget {
            allocation_operations: 3,
            peak_allocation_bytes: NativeMaterializationWork::accounting_state_bytes() + 4096,
            bytes_copied: 4096,
            items_examined: 128,
            ..WorkBudget::default()
        },
        &cancellation,
    )
    .expect("original ledger");
    let messages = vec![
        ModelMessage {
            role: ModelRole::User,
            content: ModelContent::Text("first".into()),
        },
        ModelMessage {
            role: ModelRole::User,
            content: ModelContent::Text("second".into()),
        },
    ];
    let mut observer = crate::store::HistoryWork::new(
        crate::filesystem::PreparationIo::from_native_work(&work).expect("original observer"),
    );
    assert!(clone_messages(&messages, None, &[], &mut Some(&mut observer)).is_err());
    let custody = observer.into_custody().expect("native custody");
    assert_eq!(
        custody.bytes(),
        0,
        "failed partial payload drops before its quota releases"
    );
    drop(custody);
    let mut observer = crate::store::HistoryWork::new(
        crate::filesystem::PreparationIo::from_native_work(&work).expect("original observer"),
    );
    cancellation.cancel();
    assert!(clone_messages(&messages, None, &[], &mut Some(&mut observer)).is_err());
}
