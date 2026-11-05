//! Exercise the public boundary on the process main thread's default stack.

pub(super) fn main() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let credentials = directory.path().join("identity.json");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    for command in ["whoami", "identity-mcp"] {
        let arguments = [
            command.to_owned(),
            "--endpoint".to_owned(),
            "http://127.0.0.1:1".to_owned(),
            "--credentials".to_owned(),
            credentials.display().to_string(),
            "--invalid-fixture-option".to_owned(),
        ];
        let pending = acyclic_plugin_identity::run(&arguments, None);
        let boundary_size = std::mem::size_of_val(&pending);
        if boundary_size != 2 * std::mem::size_of::<usize>() {
            return Err(format!(
                "identity state machine escaped its crate boundary: {boundary_size} bytes"
            )
            .into());
        }
        let Err(error) = runtime.block_on(pending) else {
            return Err("invalid command-line options must fail".into());
        };
        if !error.to_string().contains("--invalid-fixture-option") {
            return Err(format!("unexpected command error: {error}").into());
        }
    }
    let arguments = [
        "secrets".to_owned(),
        "list".to_owned(),
        "--endpoint".to_owned(),
        "http://127.0.0.1:1".to_owned(),
        "--scope".to_owned(),
        "fixture".to_owned(),
        "--bearer-file".to_owned(),
        directory.path().join("absent-bearer").display().to_string(),
    ];
    let Err(error) = runtime.block_on(acyclic_plugin_identity::run(&arguments, None)) else {
        return Err("Secrets admitted an invocation without its original context".into());
    };
    if !error.to_string().starts_with("missing_operation_context:") {
        return Err(format!("Secrets touched input before invocation admission: {error}").into());
    }
    runtime.block_on(secrets_invocation(directory.path()))?;
    println!("CLI and MCP entry boundaries passed on the default main-thread stack");
    Ok(())
}

async fn secrets_invocation(directory: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    use axum::{Json, Router, routing::post};
    use std::io::Write as _;
    let bearer = directory.join("bearer");
    acyclic_native_runtime::private_file::create(&bearer)?.write_all(b"fixture-bearer")?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let arguments = [
        "secrets".into(),
        "list".into(),
        "--endpoint".into(),
        format!("http://{}", listener.local_addr()?),
        "--scope".into(),
        "fixture".into(),
        "--bearer-file".into(),
        bearer.display().to_string(),
    ];
    let router = Router::new().route(
        "/v1/identity/secrets/list",
        post(|| async {
            Json(acyclic_auth::contract::SecretListReply {
                generation: 7,
                variables: vec![],
                next_cursor: None,
            })
        }),
    );
    let server = tokio::spawn(async move { axum::serve(listener, router).await });
    let result = acyclic_plugin_identity::run(
        &arguments,
        Some(acyclic_plugin_identity::interrupt_context()),
    )
    .await;
    server.abort();
    result?;

    // The same original CLI owner must stop a physical HTTP wait before EOF.
    // The OS interrupt is delivered only after the request reaches the server.
    #[cfg(unix)]
    {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let mut arguments = arguments;
        arguments[3] = format!("http://{}", listener.local_addr()?);
        let (entered, accepted) = tokio::sync::oneshot::channel();
        let (release, released) = tokio::sync::oneshot::channel::<()>();
        let events = std::sync::Arc::new(std::sync::Mutex::new(Some((entered, released))));
        let router = Router::new().route(
            "/v1/identity/secrets/list",
            post(move || {
                let events = events.clone();
                async move {
                    let events = events.lock().expect("request event").take();
                    if let Some((entered, released)) = events {
                        let _ = entered.send(());
                        let _ = released.await;
                    }
                    Json(acyclic_auth::contract::SecretListReply {
                        generation: 7,
                        variables: vec![],
                        next_cursor: None,
                    })
                }
            }),
        );
        let server = tokio::spawn(async move { axum::serve(listener, router).await });
        let interrupt = async {
            accepted.await?;
            let status = acyclic_native_runtime::run_blocking_io(
                || {
                    std::process::Command::new("kill")
                        .args(["-INT", &std::process::id().to_string()])
                        .status()
                },
                None,
            )?
            .await??;
            if !status.success() {
                return Err("original OS interrupt was not delivered".into());
            }
            Ok::<_, Box<dyn std::error::Error>>(())
        };
        let request = acyclic_plugin_identity::run(
            &arguments,
            Some(acyclic_plugin_identity::interrupt_context()),
        );
        let result = async {
            let request = core::pin::pin!(request);
            let interrupt = core::pin::pin!(interrupt);
            match futures::future::select(request, interrupt).await {
                futures::future::Either::Left((result, _)) => Ok(result),
                futures::future::Either::Right((delivered, request)) => {
                    delivered?;
                    Ok::<_, Box<dyn std::error::Error>>(request.await)
                }
            }
        }
        .await;
        drop(release);
        server.abort();
        let result = result?;
        let Err(error) = result else {
            return Err("interrupted HTTP request succeeded".into());
        };
        if !error.to_string().starts_with("operation_cancelled:") {
            return Err(format!("original CLI interrupt lost: {error}").into());
        }
    }
    Ok(())
}
