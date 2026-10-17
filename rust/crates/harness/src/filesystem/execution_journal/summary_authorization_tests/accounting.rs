use super::*;
use crate::conversation::BoundedContentFuture;
use acyclic_fs::{MemoryAuthorityBackend, MemoryObjectBackend};
use proptest::prelude::*;

type Journal =
    FilesystemExecutionJournal<MemoryStream, MemoryAuthorityBackend, MemoryObjectBackend>;

// Forward all exercised operations to the real journal; omit only the buffer
// release in projection. Authentication and the executor list remain untouched.
struct OmitBufferRelease<'a>(&'a Journal);

macro_rules! forward {
    ($(fn $method:ident<$lt:lifetime>($($name:ident: $arg:ty),*) -> $result:ty;)+) => {
        $(fn $method<$lt>(&$lt self, $($name: $arg),*) -> Result<BoundedContentFuture<$lt, $result>> {
            self.0.$method($($name),*)
        })+
    };
}

impl ExecutionJournal for OmitBufferRelease<'_> {
    forward! {
        fn replay<'a>(operation: OperationId, after: u64, maximum: u32,
            io: Option<&'a PreparationIo>) -> Vec<ExecutionRecord>;
        fn append<'a>(operation: OperationId, key: &'a str, event: &'a ExecutionEvent,
            io: Option<&'a PreparationIo>) -> ();
        fn stage<'a>(operation: OperationId, key: &'a str, bytes: &'a [u8], media: &'static str,
            io: Option<&'a PreparationIo>) -> FileRef;
        fn load<'a>(file: &'a FileRef, io: Option<&'a PreparationIo>) -> acyclic_stream::OwnedBytes;
        fn open_interaction<'a>(id: InteractionId, interaction: &'a Interaction,
            io: Option<&'a PreparationIo>) -> ();
        fn interaction_outcome<'a>(id: InteractionId,
            io: Option<&'a PreparationIo>) -> Option<InteractionOutcome>;
        fn authenticate_summary<'a>(file: &'a FileRef, io: Option<&'a PreparationIo>) -> bool;
    }

    fn verify_model_content<'a>(
        &'a self,
        content: &'a ModelContent,
        io: Option<&'a PreparationIo>,
        context: Option<&'a crate::runtime::TaskContext>,
        summaries: &'a [&'a FileRef],
    ) -> Result<BoundedContentFuture<'a, ()>> {
        BoundedContentFuture::new(io, || {
            self.0
                .verify_model_content_with_io::<false>(content, io, context, summaries)
        })
    }
}

// Every iteration must return precisely to the caller's existing custody, not
// merely stay below a roomy budget. Both broken variants use this same detector.
async fn releases_all(
    journal: &dyn ExecutionJournal,
    content: &ModelContent,
    scope: &crate::runtime::RuntimeScope,
    repetitions: usize,
    release_list: bool,
) -> Result<bool> {
    let cancellation = acyclic_fs::CancellationToken::new();
    let io = PreparationIo::from_budget(
        acyclic_fs::performance::work_budget(65_536, 64 * 1_024 * 1_024),
        &cancellation,
    )?;
    io.retain_owned(23)?;
    let before = io.owned_bytes()?;
    for _ in 0..repetitions {
        if release_list {
            crate::executor::verify_model_contents_scoped(
                journal,
                std::iter::once(content),
                Limits::default(),
                Some(scope),
                Some(&io),
                None,
            )
            .await?;
        } else {
            crate::executor::verify_model_contents_with_summary_authorization::<true, false>(
                journal,
                std::iter::once(content),
                Limits::default(),
                Some(scope),
                Some(&io),
                None,
            )
            .await?;
        }
        if io.owned_bytes()? != before {
            return Ok(false);
        }
    }
    Ok(true)
}

#[test]
fn scoped_summary_preflight_releases_buffers_and_the_authenticated_list() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let fixture = runtime.block_on(fixture())?;
    let owner = runtime.block_on(fixture.running(84))?;
    let turn = OperationId::new();
    let operation =
        super::super::super::task_runtime::execution_operation(owner.task_id(), turn, None)?;
    let journal = fixture
        .execution(owner, operation)?
        .with_turn(operation, turn);
    let file = runtime.block_on(output(&journal, operation, "summary:0:output"))?;
    runtime.block_on(publish(&journal, &file, &proof(operation, file.clone())))?;
    let scope = crate::runtime::RuntimeScope::new(
        crate::Capabilities::new([] as [&str; 0]),
        Limits::default(),
    )?;
    let part = crate::model::ModelContentPart::File {
        file,
        policy: crate::model::FileProjectionPolicy::BoundedFull,
    };
    proptest::test_runner::TestRunner::default()
        .run(&(1usize..4, 1usize..4), |(repetitions, references)| {
            let content = ModelContent::Parts(vec![part.clone(); references]);
            prop_assert!(
                runtime.block_on(releases_all(&journal, &content, &scope, repetitions, true,))?,
                "scoped preflight retained freed custody"
            );
            prop_assert!(
                !runtime.block_on(releases_all(
                    &OmitBufferRelease(&journal),
                    &content,
                    &scope,
                    repetitions,
                    true,
                ))?,
                "detector missed omitted projection-buffer release"
            );
            prop_assert!(
                !runtime.block_on(releases_all(&journal, &content, &scope, repetitions, false,))?,
                "detector missed omitted executor-list release"
            );
            Ok(())
        })
        .unwrap();
    Ok(())
}
