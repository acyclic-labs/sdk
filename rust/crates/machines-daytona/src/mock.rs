//! In-process HTTP/1.1 stand-in for the Daytona API, for unit tests of provider behaviour
//! that depends on what Daytona answers. It records every request and answers each with the
//! test's handler; nothing leaves the loopback interface.

use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};

use tokio::{
    io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _, BufReader},
    net::{TcpListener, TcpStream},
};

/// One request the mock received.
#[derive(Clone, Debug)]
pub struct Request {
    /// HTTP method.
    pub method: String,
    /// Path without the query string.
    pub path: String,
    /// Decoded query pairs.
    pub query: Vec<(String, String)>,
    /// `Authorization` header, if any.
    pub authorization: Option<String>,
    /// Request body as text.
    pub body: String,
}

impl Request {
    /// Whether the request is `method path`.
    pub fn is(&self, method: &str, path: &str) -> bool {
        self.method == method && self.path == path
    }
}

/// Status code and JSON body.
pub type Reply = (u16, String);
type Handler = Arc<dyn Fn(Request) -> Pin<Box<dyn Future<Output = Reply> + Send>> + Send + Sync>;

/// A running mock; dropped with the test runtime.
pub struct Mock {
    /// Base URL to use as `DaytonaConfig::api_url`.
    pub url: String,
    requests: Arc<Mutex<Vec<Request>>>,
}

impl Mock {
    /// Starts a mock answering every request with `handler`.
    pub async fn start<F, Fut>(handler: F) -> Self
    where
        F: Fn(Request) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Reply> + Send + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let handler: Handler = Arc::new(move |request| Box::pin(handler(request)));
        let log = Arc::clone(&requests);
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let handler = Arc::clone(&handler);
                let log = Arc::clone(&log);
                tokio::spawn(async move {
                    let _ = serve(stream, handler, log).await;
                });
            }
        });
        Self { url, requests }
    }

    /// Every request received so far.
    pub fn requests(&self) -> Vec<Request> {
        self.requests
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Waits (up to five seconds) until a `method path` request has arrived.
    pub async fn wait_for(&self, method: &str, path: &str) {
        for _ in 0..1_000 {
            if self
                .requests()
                .iter()
                .any(|request| request.is(method, path))
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("mock never received {method} {path}");
    }
}

async fn serve(
    stream: TcpStream,
    handler: Handler,
    log: Arc<Mutex<Vec<Request>>>,
) -> std::io::Result<()> {
    let (read, mut write) = stream.into_split();
    let mut reader = BufReader::new(read);
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).await? == 0 {
            return Ok(());
        }
        let mut parts = line.split_whitespace();
        let method = parts.next().unwrap_or_default().to_owned();
        let target = parts.next().unwrap_or_default().to_owned();
        let (mut length, mut authorization) = (0_usize, None);
        loop {
            let mut header = String::new();
            reader.read_line(&mut header).await?;
            let header = header.trim_end();
            if header.is_empty() {
                break;
            }
            if let Some((name, value)) = header.split_once(':') {
                if name.eq_ignore_ascii_case("content-length") {
                    length = value.trim().parse().unwrap_or(0);
                } else if name.eq_ignore_ascii_case("authorization") {
                    authorization = Some(value.trim().to_owned());
                }
            }
        }
        let mut body = vec![0; length];
        reader.read_exact(&mut body).await?;
        let url = reqwest::Url::parse(&format!("http://mock{target}")).unwrap();
        let request = Request {
            method,
            path: url.path().to_owned(),
            query: url.query_pairs().into_owned().collect(),
            authorization,
            body: String::from_utf8_lossy(&body).into_owned(),
        };
        log.lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(request.clone());
        let (status, body) = handler(request).await;
        let response = format!(
            "HTTP/1.1 {status} MOCK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
            body.len()
        );
        write.write_all(response.as_bytes()).await?;
    }
}
