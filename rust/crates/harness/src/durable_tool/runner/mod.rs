use super::*;

/// Dispatches each durable tool call once and reconciles retained dispatches.
pub struct DurableToolRunner {
    tools: ToolRegistry,
    journal: Arc<dyn ExecutionJournal>,
    limits: Limits,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    native_custody: Option<acyclic_fs::NativeAllocation>,
}

impl DurableToolRunner {
    /// Pins the registered definitions and the owner-bound ref-only journal.
    pub fn new(
        tools: &ToolRegistry,
        journal: Arc<dyn ExecutionJournal>,
        _work: crate::NativeWork<'_>,
    ) -> Result<Self> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = _work {
            if !journal.supports_native_execution(work)? {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unsupported,
                    reason: "tool journal has no native execution producer",
                });
            }
            let metadata =
                (std::mem::size_of::<Self>() - std::mem::size_of::<ToolRegistry>()) as u64;
            let mut custody = work.claim(metadata, 0, acyclic_fs::WorkCounters::UNCHARGED)?;
            let mut tools_custody;
            let (tools, hold) = tools.clone_with_native_work(work)?;
            tools_custody = hold;
            custody.absorb(&mut tools_custody)?;
            return Ok(Self {
                tools,
                journal,
                limits: Limits::default(),
                native_custody: Some(custody),
            });
        }
        Ok(Self {
            tools: tools.clone(),
            journal,
            limits: Limits::default(),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_custody: None,
        })
    }

    /// Narrows the maximum artifact and projection bytes before publication.
    pub fn with_limits(mut self, limits: Limits) -> Result<Self> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let io = self
            .native_custody
            .as_ref()
            .map(|custody| crate::filesystem::PreparationIo::from_native_work(custody.work()))
            .transpose()?;
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let io = None;
        crate::executor::with_history_io(io.as_ref(), |work| limits.validate(work))?;
        self.limits = limits;
        Ok(self)
    }

    /// Runs an admitted invocation; reconciliation never dispatches missing intent.
    pub fn run<'a>(
        &'a self,
        task_id: TaskId,
        definition: &'a ToolDefinition,
        invocation: &'a ToolInvocation,
        context: &'a ToolContext,
        work: crate::NativeWork<'a>,
        reconcile_only: bool,
    ) -> Result<
        crate::store::history::HistoryContentFuture<'a, (Outcome<Value>, crate::NativeCustody)>,
    > {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let io = work
            .map(crate::executor::PreparationIo::from_native_work)
            .transpose()?;
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let io: Option<crate::executor::PreparationIo> = {
            if work.is_some() {
                return Err(Error::Invalid("native tool work is unavailable".into()));
            }
            None
        };
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = work {
            work.remaining()?;
            if !context.native_metadata_matches(work)
                || !self
                    .native_custody
                    .as_ref()
                    .is_some_and(|guard| guard.work().shares_ledger(work))
            {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "native tool context or runner metadata has no original custody",
                });
            }
        }
        let tool = self
            .tools
            .get_version(&definition.name, &definition.revision, io.as_ref())?
            .ok_or_else(|| {
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                if work.is_some() {
                    return Error::NativeBoundary {
                        code: crate::wire::ErrorCode::NotFound,
                        reason: "admitted tool revision is not registered",
                    };
                }
                Error::NotFound(format!("tool {}@{}", definition.name, definition.revision))
            })?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = work {
            tool.executor.supports_native_execution(work)?;
            tool.projection.supports_native_projection(Some(work))?;
            if !self.journal.supports_native_execution(work)? {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unsupported,
                    reason: "tool journal has no native execution producer",
                });
            }
        }
        let future = || async move {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            let io = work
                .map(crate::executor::PreparationIo::from_native_work)
                .transpose()?;
            #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
            let io: Option<crate::executor::PreparationIo> = None;
            let copied = 'copy: {
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                if let Some(work) = work {
                    let mut custody = work.claim(
                        (std::mem::size_of::<ToolInvocation>() - std::mem::size_of::<Value>())
                            as u64,
                        0,
                        acyclic_fs::WorkCounters::UNCHARGED,
                    )?;
                    // Payload tuples drop before the header guard on every error path.
                    let mut call = work.display(&invocation.call_id)?;
                    custody.absorb(&mut call.1)?;
                    let mut name = work.display(&invocation.name)?;
                    custody.absorb(&mut name.1)?;
                    let mut arguments =
                        crate::tool::clone_value(&invocation.arguments, None, work)?;
                    custody.absorb(&mut arguments.1)?;
                    break 'copy (
                        ToolInvocation {
                            operation_id: invocation.operation_id,
                            call_id: call.0,
                            name: name.0,
                            arguments: arguments.0,
                        },
                        Some(custody),
                    );
                }
                (invocation.clone(), None)
            };
            let result = self
                .run_engine(
                    task_id,
                    invocation.operation_id,
                    definition,
                    copied,
                    context,
                    tool,
                    io.as_ref(),
                    reconcile_only,
                )
                .await?;
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if work.is_some() && result.1.is_none() {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Storage,
                    reason: "native tool outcome has no custody",
                });
            }
            Ok(result)
        };
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = work {
            return crate::NativeFutureBundle::new(work, future)
                .map(crate::store::history::HistoryContentFuture::Native);
        }
        Ok(crate::store::history::HistoryContentFuture::Ordinary {
            future: Box::pin(future()),
            frame_bytes: 0,
        })
    }
}

mod engine;
mod journal;
mod value;
pub(super) use value::tool_error;
use value::*;
