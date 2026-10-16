use super::*;

pub(crate) struct Cache {
    // Lock order: never nest this cache lock with a session lock or an await.
    // Construction only publishes an empty pool; its task owns all dialing.
    client: Mutex<Option<Client>>,
}

#[cfg(acyclic_stream_loom)]
/// Models the real cache lock and detects split publication.
pub fn verify_cache(broken: bool) {
    loom::model(move || {
        let cache = Arc::new(Cache::new());
        let creates = Arc::new(AtomicU64::new(0));
        let (commands, _) = mpsc::channel(2);
        let template = Client {
            commands,
            removed: Arc::new(signal::Signal::new()),
            next_id: Arc::new(AtomicU64::new(1)),
            active: Arc::new(AtomicU64::new(0)),
        };
        let run = || {
            let cache = Arc::clone(&cache);
            let creates = Arc::clone(&creates);
            let template = template.clone();
            loom::thread::spawn(move || {
                let create = || {
                    creates.fetch_add(1, Ordering::SeqCst);
                    template.clone()
                };
                if broken {
                    let observed = cache.client.lock().unwrap().clone();
                    observed.unwrap_or_else(|| {
                        let client = create();
                        *cache.client.lock().unwrap() = Some(client.clone());
                        client
                    })
                } else {
                    cache.get_or_try_init(create).unwrap()
                }
            })
        };
        let first = run();
        let second = run();
        first.join().unwrap();
        second.join().unwrap();
        assert_eq!(
            creates.load(Ordering::SeqCst),
            1,
            "one shared pool must be published"
        );
    });
}
impl Cache {
    pub(crate) fn new() -> Self {
        Self {
            client: Mutex::new(None),
        }
    }
    pub(crate) fn get_or_try_init(
        &self,
        create: impl FnOnce() -> Client,
    ) -> Result<Client, StreamError> {
        let mut client = self.client.lock().map_err(|_| StreamError::Unavailable)?;
        if let Some(client) = client.as_ref() {
            return Ok(client.clone());
        }
        let created = create();
        *client = Some(created.clone());
        Ok(created)
    }
}
