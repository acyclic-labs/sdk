use super::*;

pub(crate) struct Cache<T = Client> {
    // Lock order: never nest this cache lock with a session lock or an await.
    // Construction only publishes an empty pool; its task owns all dialing.
    client: Mutex<Option<T>>,
}

#[cfg(acyclic_stream_loom)]
/// Models the real cache lock and detects split publication.
pub fn verify_cache(broken: bool) {
    loom::model(move || {
        // Only publication is modeled here. The payload is a Copy identity;
        // client lifecycle/credit/fencing atomics have their own pool models.
        let state = Arc::new((Cache::<u8>::new(), AtomicU64::new(0)));
        let run = |identity| {
            let state = Arc::clone(&state);
            loom::thread::spawn(move || {
                let create = || {
                    state.1.fetch_add(1, Ordering::SeqCst);
                    identity
                };
                if broken {
                    let observed = *state.0.client.lock().unwrap();
                    observed.unwrap_or_else(|| {
                        let value = create();
                        *state.0.client.lock().unwrap() = Some(value);
                        value
                    })
                } else {
                    state.0.get_or_try_init(create).unwrap()
                }
            })
        };
        let first = run(1);
        let second = run(2);
        let first = first.join().unwrap();
        let second = second.join().unwrap();
        assert_eq!(
            state.1.load(Ordering::SeqCst),
            1,
            "one shared pool must be published"
        );
        assert_eq!(first, second);
        assert_eq!(*state.0.client.lock().unwrap(), Some(first));
    });
}

impl<T: Clone> Cache<T> {
    pub(crate) fn new() -> Self {
        Self {
            client: Mutex::new(None),
        }
    }
    pub(crate) fn get_or_try_init(&self, create: impl FnOnce() -> T) -> Result<T, StreamError> {
        let mut client = self.client.lock().map_err(|_| StreamError::Unavailable)?;
        if let Some(client) = client.as_ref() {
            return Ok(client.clone());
        }
        let created = create();
        *client = Some(created.clone());
        Ok(created)
    }
}
impl Cache {
    pub(crate) fn abandon_pending(&self) -> Result<(), StreamError> {
        let client = self
            .client
            .lock()
            .map_err(|_| StreamError::Unavailable)?
            .clone();
        // Release the cache lock before touching the pool's wakeup gate.
        client.map_or(Ok(()), |client| client.abandon_pending())
    }
}
