use super::*;
use proptest::prelude::*;

#[tokio::test]
async fn constant_callbacks_preserve_status_admission_and_all_source_counters()
-> Result<(), Box<dyn std::error::Error>> {
    let source = windows_checkout_source().await?;
    let root = tempfile::tempdir()?;
    let work =
        crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .map_err(|(error, _)| error)?;
    let runtime = names::callback_runtime(source, root.path(), &work)?;
    let data = PRJ_CALLBACK_DATA {
        InstanceContext: std::ptr::from_ref(&runtime).cast_mut().cast(),
        ..Default::default()
    };
    let callbacks = callbacks();
    let invoke = |data, offset, length| {
        // SAFETY: the instance data lives through these calls. These callbacks
        // deliberately ignore the unused enumeration/search/content arguments.
        unsafe {
            [
                callbacks
                    .EndDirectoryEnumerationCallback
                    .expect("end callback")(data, std::ptr::null()),
                callbacks
                    .GetDirectoryEnumerationCallback
                    .expect("get callback")(
                    data,
                    std::ptr::null(),
                    PCWSTR::null(),
                    PRJ_DIR_ENTRY_BUFFER_HANDLE::default(),
                ),
                callbacks
                    .GetPlaceholderInfoCallback
                    .expect("placeholder callback")(data),
                callbacks.GetFileDataCallback.expect("data callback")(data, offset, length),
            ]
        }
    };
    let prefix = work.receipt();
    for closed in [false, true] {
        if closed {
            runtime.callbacks.close_admission();
        }
        let expected = if closed {
            [HR_FILE_INVALID; 4]
        } else {
            [HR_OK, HR_OK, HR_FILE_NOT_FOUND, HR_UNEXPECTED]
        };
        let mut runner = proptest::test_runner::TestRunner::new(ProptestConfig {
            cases: 32,
            source_file: Some(file!()),
            ..ProptestConfig::default()
        });
        runner.run(&(any::<u64>(), any::<u32>()), |(offset, length)| {
            prop_assert_eq!(invoke(&raw const data, offset, length), expected);
            prop_assert_eq!(invoke(std::ptr::null(), offset, length), [HR_UNEXPECTED; 4]);
            prop_assert_eq!(lock_recover(&runtime.callbacks.active).0, 0);
            prop_assert_eq!(work.receipt(), prefix);
            Ok(())
        })?;
    }
    Ok(())
}
