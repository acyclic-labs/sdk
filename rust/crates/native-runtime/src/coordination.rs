//! Fixed native coordination shards; actors select one by their canonical key.
use std::{
    collections::hash_map::RandomState,
    fmt,
    future::Future,
    hash::{BuildHasher, Hash},
    io,
    pin::Pin,
    sync::{Arc, LazyLock},
    task::{Context, Poll},
};
mod health;
mod lifetime;
use health::ShardHealth;
pub use lifetime::with_actor_lifetime;

/// A delivery ended without its ordinary application result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ActorFailure {
    /// The selected shard stopped; its original producer no longer exists.
    Unavailable,
    /// The delivery's owner requested cancellation.
    Cancelled,
    /// The delivery unwound.
    Panicked,
}

impl fmt::Display for ActorFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Unavailable => "actor coordination shard unavailable",
            Self::Cancelled => "actor delivery cancelled",
            Self::Panicked => "actor delivery panicked",
        })
    }
}

impl std::error::Error for ActorFailure {}

fn failure(reason: ActorFailure) -> io::Error {
    io::Error::new(
        match reason {
            ActorFailure::Unavailable => io::ErrorKind::NotConnected,
            ActorFailure::Cancelled => io::ErrorKind::Interrupted,
            ActorFailure::Panicked => io::ErrorKind::Other,
        },
        reason,
    )
}

struct Health(Arc<ShardHealth>);

impl Drop for Health {
    fn drop(&mut self) {
        self.0.retire();
    }
}

struct Shard {
    handle: tokio::runtime::Handle,
    alive: Arc<ShardHealth>,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
}

struct RuntimeOwner(Option<tokio::runtime::Runtime>);

impl Drop for RuntimeOwner {
    fn drop(&mut self) {
        // Runtime shutdown may run from an asynchronous admission caller if
        // OS thread creation fails. Never block that caller on destruction.
        if let Some(runtime) = self.0.take() {
            runtime.shutdown_background();
        }
    }
}

impl Drop for Shard {
    fn drop(&mut self) {
        // Refuse new deliveries before asking the original runtime owner to
        // cancel its tasks. Dropping an owner never joins a busy actor.
        self.alive.retire();
        drop(self.stop.take());
    }
}

/// One current-thread runtime and OS thread per available core.
///
/// The immutable hash seed and shard array retain placement for the process
/// lifetime. No actor registry, per-actor thread, task scan or work stealing
/// is introduced. Stream follows remain the producers of actor input wakes.
pub struct Coordination {
    first: Shard,
    others: Box<[Shard]>,
    width: std::num::NonZeroUsize,
    hash: RandomState,
}

impl Coordination {
    fn start(width: std::num::NonZeroUsize) -> io::Result<Self> {
        let mut shards = Vec::with_capacity(width.get());
        for index in 0..width.get() {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            let handle = runtime.handle().clone();
            let owner = RuntimeOwner(Some(runtime));
            let alive = Arc::new(ShardHealth::new());
            let health = Health(Arc::clone(&alive));
            let (stop, stopped) = tokio::sync::oneshot::channel();
            std::thread::Builder::new()
                .name(format!("acyclic-coordination-{index}"))
                .spawn(move || {
                    // Health is retired before runtime destruction cancels
                    // tasks and wakes their result observers, also on unwind.
                    let retirement = health;
                    if let Some(runtime) = &owner.0 {
                        runtime.block_on(async {
                            let _ = stopped.await;
                        });
                    }
                    drop(retirement);
                    drop(owner);
                })?;
            shards.push(Shard {
                handle,
                alive,
                stop: Some(stop),
            });
        }
        let mut shards = shards.into_iter();
        let first = shards
            .next()
            .ok_or_else(|| failure(ActorFailure::Unavailable))?;
        Ok(Self {
            first,
            others: shards.collect(),
            width,
            hash: RandomState::new(),
        })
    }

    /// Runs the application admission/control future on the calling thread.
    /// Actor deliveries use [`Self::actor`] so every poll stays on their shard.
    /// This creates no additional runtime or thread.
    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.first.handle.block_on(future)
    }

    /// Selects one immutable executor from an actor's canonical reference.
    /// Hashing costs O(key bytes); selection and cloning one handle are O(1)
    /// in actors, history and cores. Identity validation belongs to admission.
    pub fn actor(&self, key: &impl Hash) -> io::Result<ActorExecutor> {
        let width = u64::try_from(self.width.get()).map_err(io::Error::other)?;
        let index = usize::try_from(self.hash.hash_one(key) % width).map_err(io::Error::other)?;
        let shard = if index == 0 {
            &self.first
        } else {
            self.others
                .get(index - 1)
                .ok_or_else(|| failure(ActorFailure::Unavailable))?
        };
        Ok(ActorExecutor {
            handle: shard.handle.clone(),
            alive: Arc::clone(&shard.alive),
        })
    }
}

/// Fixed placement for already-admitted work. It grants no application authority.
#[derive(Clone)]
pub struct ActorExecutor {
    handle: tokio::runtime::Handle,
    alive: Arc<ShardHealth>,
}

impl ActorExecutor {
    /// Transfers one owned delivery to its selected current-thread runtime.
    /// Dropping the returned observer detaches; explicit abort cancels it.
    /// Retains the original [`with_actor_lifetime`] owner when one is scoped.
    /// No application mailbox or second cross-actor wake channel is created.
    pub fn spawn<F>(&self, future: F) -> ActorTask<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let task = if self.alive.is_available() {
            Some(crate::task::spawn_on(
                &self.handle,
                lifetime::inherit(future),
            ))
        } else {
            drop(future);
            None
        };
        ActorTask {
            task,
            retired: self.alive.retired(),
            alive: Arc::clone(&self.alive),
        }
    }
}

pin_project_lite::pin_project! {
    /// Optional observation and cancellation authority for one original delivery.
    pub struct ActorTask<T> {
        task: Option<crate::TaskHandle<T>>,
        alive: Arc<ShardHealth>,
        #[pin]
        retired: tokio_util::sync::WaitForCancellationFutureOwned,
    }
}

impl<T> ActorTask<T> {
    /// Retains cancellation authority independently of the result observer.
    pub fn abort_handle(&self) -> Option<crate::AbortHandle> {
        self.task.as_ref().map(crate::TaskHandle::abort_handle)
    }

    /// Requests cancellation of the original delivery, retaining its observer.
    pub fn abort(&self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

impl<T> Future for ActorTask<T> {
    type Output = io::Result<T>;
    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let Some(task) = this.task.as_mut() else {
            return Poll::Ready(Err(failure(ActorFailure::Unavailable)));
        };
        if let Poll::Ready(result) = Pin::new(task).poll(context) {
            return Poll::Ready(result.map_err(|error| {
                failure(if !this.alive.is_available() {
                    ActorFailure::Unavailable
                } else if error.kind() == io::ErrorKind::Interrupted {
                    ActorFailure::Cancelled
                } else {
                    ActorFailure::Panicked
                })
            }));
        }
        if this.retired.poll(context).is_ready() {
            // Physical work may still be blocked. Detach its original owner;
            // reporting retirement cannot release its accepted input custody.
            drop(this.task.take());
            Poll::Ready(Err(failure(ActorFailure::Unavailable)))
        } else {
            Poll::Pending
        }
    }
}

static COORDINATION: LazyLock<io::Result<Coordination>> =
    LazyLock::new(|| Coordination::start(std::thread::available_parallelism()?));

/// Starts or borrows the one process-wide native coordination executor.
pub fn coordination() -> io::Result<&'static Coordination> {
    COORDINATION
        .as_ref()
        .map_err(|error| io::Error::new(error.kind(), error.to_string()))
}

#[cfg(test)]
mod tests;
