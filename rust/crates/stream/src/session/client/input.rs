//! Input credit and terminal ownership on the original admitted call.
use super::*;
use tokio::sync::Semaphore;

pub(super) struct Window {
    // Includes reservations until their frame is queued. Only the pool grants
    // and debits; cancellation returns reservations without inventing credit.
    balance: Mutex<wire::SessionWindow>,
    frames: Semaphore,
    bytes: Semaphore,
}

impl Window {
    pub(super) fn new() -> Self {
        Self {
            balance: Mutex::new(Default::default()),
            frames: Semaphore::new(0),
            bytes: Semaphore::new(0),
        }
    }

    pub(super) fn grant(&self, grant: &wire::SessionWindow) -> Result<(), StreamError> {
        let mut balance = self.balance.lock().map_err(|_| StreamError::Unavailable)?;
        let mut next = *balance;
        wire_codec::session::grant_credit(&mut next, grant)?;
        if !usize::try_from(next.bytes).is_ok_and(|bytes| bytes <= Semaphore::MAX_PERMITS)
            || next.frames as usize > Semaphore::MAX_PERMITS
        {
            return Err(StreamError::Capacity);
        }
        *balance = next;
        drop(balance);
        self.bytes.add_permits(grant.bytes as usize);
        self.frames.add_permits(grant.frames as usize);
        Ok(())
    }

    async fn reserve(window: &Arc<Self>, length: u32) -> Result<Reservation, StreamError> {
        let frames = window
            .frames
            .acquire()
            .await
            .map_err(|_| StreamError::Unavailable)?;
        let bytes = window
            .bytes
            .acquire_many(length)
            .await
            .map_err(|_| StreamError::Unavailable)?;
        frames.forget();
        bytes.forget();
        Ok(Reservation {
            window: window.clone(),
            length,
            debited: false,
            accepted: false,
        })
    }

    pub(super) fn close(&self) {
        self.frames.close();
        self.bytes.close();
    }
}

pub(super) struct Reservation {
    window: Arc<Window>,
    length: u32,
    debited: bool,
    accepted: bool,
}

impl Reservation {
    // Both debits and the queue admission run in the same pool turn, with no
    // await or peer grant between them. Refusal retires that link before refund.
    fn debit(&mut self) -> Result<(), StreamError> {
        let mut balance = self
            .window
            .balance
            .lock()
            .map_err(|_| StreamError::Unavailable)?;
        let bytes = balance
            .bytes
            .checked_sub(u64::from(self.length))
            .ok_or(StreamError::Capacity)?;
        let frames = balance.frames.checked_sub(1).ok_or(StreamError::Capacity)?;
        *balance = wire::SessionWindow { bytes, frames };
        self.debited = true;
        Ok(())
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if self.accepted {
            return;
        }
        if self.debited {
            if let Ok(mut balance) = self.window.balance.lock() {
                balance.bytes += u64::from(self.length);
                balance.frames += 1;
            } else {
                self.window.close();
                return;
            }
        }
        self.window.bytes.add_permits(self.length as usize);
        self.window.frames.add_permits(1);
    }
}

pub(super) struct Credit {
    channel: Reservation,
    aggregate: Reservation,
}
impl Credit {
    pub(super) fn debit(&mut self) -> Result<(), StreamError> {
        self.channel.debit()?;
        self.aggregate.debit()
    }
    pub(super) fn accepted(mut self) {
        self.channel.accepted = true;
        self.aggregate.accepted = true;
    }
}

pub(super) struct State {
    pub window: Arc<Window>,
    connection: Mutex<Option<Arc<Window>>>,
    terminal: Mutex<Terminal>,
    changed: gate::Gate,
    pool: PoolHandle,
    id: u64,
    progress: Arc<Progress>,
}
struct Terminal {
    cause: Option<StreamError>,
    finish: Option<oneshot::Sender<Result<(), StreamError>>>,
    // Exactly the reply cursor and, after successful open, its input sender.
    // Route ownership observes their terminal without keeping either alive.
    clients: u8,
}
impl State {
    pub(super) fn new(pool: PoolHandle, id: u64, progress: Arc<Progress>) -> Self {
        Self {
            window: Arc::new(Window::new()),
            connection: Mutex::new(None),
            terminal: Mutex::new(Terminal {
                cause: None,
                finish: None,
                clients: 1,
            }),
            changed: gate::Gate::new(),
            pool,
            id,
            progress,
        }
    }
    pub(super) fn bind(&self, connection: Arc<Window>) -> Result<(), StreamError> {
        *self
            .connection
            .lock()
            .map_err(|_| StreamError::Unavailable)? = Some(connection);
        Ok(())
    }
    pub(super) fn close(&self, cause: StreamError) {
        let finish = if let Ok(mut terminal) = self.terminal.lock() {
            if terminal.cause.is_none() {
                terminal.cause = Some(cause);
            }
            terminal.finish.take()
        } else {
            None
        };
        self.window.close();
        let _ = self.changed.set(true);
        // Publish the original cause before abandoning the queue waiter.
        drop(finish);
    }
    pub(super) fn finishing(
        &self,
        finish: oneshot::Sender<Result<(), StreamError>>,
    ) -> Result<(), StreamError> {
        let mut terminal = self.terminal.lock().map_err(|_| StreamError::Unavailable)?;
        if let Some(cause) = &terminal.cause {
            let cause = cause.clone();
            drop(terminal);
            let _ = finish.send(Err(cause));
            return Ok(());
        }
        if terminal.finish.is_some() {
            return Err(StreamError::InvalidResponse);
        }
        terminal.finish = Some(finish);
        Ok(())
    }
    pub(super) fn finished(&self) -> Result<(), StreamError> {
        let finish = self
            .terminal
            .lock()
            .map_err(|_| StreamError::Unavailable)?
            .finish
            .take()
            .ok_or(StreamError::InvalidResponse)?;
        let _ = finish.send(Ok(()));
        Ok(())
    }
    fn cause(&self) -> StreamError {
        self.terminal
            .lock()
            .ok()
            .and_then(|terminal| terminal.cause.clone())
            .unwrap_or(StreamError::Unavailable)
    }
    async fn until_closed<T>(
        &self,
        operation: impl std::future::Future<Output = Result<T, StreamError>>,
    ) -> Result<T, StreamError> {
        let changed = self.changed.changed(false);
        let operation = core::pin::pin!(operation);
        let changed = core::pin::pin!(changed);
        match future::select(changed, operation).await {
            future::Either::Left(_) => Err(self.cause()),
            future::Either::Right((result, _)) => result,
        }
    }
    async fn reserve(&self, length: u32) -> Result<Credit, StreamError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| StreamError::Unavailable)?
            .clone()
            .ok_or(StreamError::Unavailable)?;
        let channel = Window::reserve(&self.window, length).await?;
        let aggregate = Window::reserve(&connection, length).await?;
        Ok(Credit { channel, aggregate })
    }
    pub(super) fn sender(state: &Arc<Self>) -> Result<CallInputSender, StreamError> {
        let mut terminal = state
            .terminal
            .lock()
            .map_err(|_| StreamError::Unavailable)?;
        terminal.clients = terminal
            .clients
            .checked_add(1)
            .filter(|clients| *clients == 2)
            .ok_or(StreamError::Unavailable)?;
        Ok(CallInputSender {
            state: state.clone(),
            next: 0,
            finished: false,
        })
    }
    pub(super) fn cancel(&self) {
        self.close(StreamError::Unavailable);
        // Withdraw the upload without erasing an earlier typed reply failure
        // already queued by the same route. The reply cursor owns its fence.
        let _ = self.pool.removed.send(self.id);
    }
    pub(super) fn release(&self) {
        let last = {
            // Cleanup must still retire original interest after lock poisoning.
            let mut terminal = self
                .terminal
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            terminal.clients -= 1;
            terminal.clients == 0
        };
        if last {
            self.pool.retire_interest(self.id, &self.progress);
        }
    }
}

pub(super) struct Owner(pub Arc<State>);
impl Drop for Owner {
    fn drop(&mut self) {
        self.0.close(StreamError::Unavailable);
    }
}

/// Upload half of one admitted canonical call. It uses the same authority,
/// connection and cancellation as its reply. It cannot clone, reconnect or
/// replay an upload. Dropping it before `finish` withdraws the original call.
pub struct CallInputSender {
    pub(super) state: Arc<State>,
    pub(super) next: u64,
    pub(super) finished: bool,
}
impl CallInputSender {
    /// Queue one original immutable fragment under channel AND connection
    /// credit. No input is collected or copied. A cancelled send future must
    /// not be retried: drop this sender to withdraw the original operation.
    pub async fn send(&mut self, bytes: Bytes) -> Result<(), StreamError> {
        if bytes.is_empty() || bytes.len() > crate::MAX_RECORD_BYTES {
            return Err(StreamError::InvalidArgument);
        }
        let next = self.next.checked_add(1).ok_or(StreamError::OutOfRange)?;
        let value = frame(
            self.state.id,
            1,
            Body::Data(wire::SessionData {
                after: Some(cursor(next)),
                accepted: 0,
                durable: 0,
                item: Some(Item::ActorCall(bytes)),
            }),
        );
        let length = u32::try_from(value.encoded_len()).map_err(|_| StreamError::LimitExceeded)?;
        let mut attempt = Attempt {
            state: self.state.clone(),
            accepted: false,
        };
        // Sequence is claimed before awaiting. Cancellation cannot reuse the
        // old position for a frame whose queue verdict was not observed.
        self.next = next;
        let result = self
            .state
            .until_closed(async {
                let credit = self.state.reserve(length).await?;
                self.queue(value, Some(credit)).await
            })
            .await;
        attempt.accepted = result.is_ok();
        result
    }
    /// Send the original ordered EOF/trailers without requiring data credit.
    /// Retains the call until its server accepts that terminal, so completion
    /// cannot discard queued input when the reply has already retired.
    pub async fn finish(mut self, trailers: Vec<wire::SessionHeader>) -> Result<(), StreamError> {
        let value = frame(
            self.state.id,
            1,
            Body::InputEnd(wire::SessionInputEnd { trailers }),
        );
        if value.encoded_len() > crate::MAX_COMMAND_BYTES {
            return Err(StreamError::LimitExceeded);
        }
        self.state.until_closed(self.queue(value, None)).await?;
        self.finished = true;
        Ok(())
    }
    async fn queue(
        &self,
        frame: wire::SessionFrame,
        credit: Option<Credit>,
    ) -> Result<(), StreamError> {
        let (sent, result) = oneshot::channel();
        self.state
            .pool
            .commands
            .send(Command::Input {
                id: self.state.id,
                frame,
                credit,
                sent,
            })
            .await
            .map_err(|_| StreamError::Unavailable)?;
        result.await.map_err(|_| StreamError::Unavailable)?
    }
}
impl Drop for CallInputSender {
    fn drop(&mut self) {
        if !self.finished {
            self.state.cancel();
        }
        self.state.release();
    }
}

struct Attempt {
    state: Arc<State>,
    accepted: bool,
}
impl Drop for Attempt {
    fn drop(&mut self) {
        if !self.accepted {
            self.state.cancel();
        }
    }
}
