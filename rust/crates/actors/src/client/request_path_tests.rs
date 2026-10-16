use super::*;
use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};
use tonic::{
    body::Body,
    codegen::{Service, http},
};

#[derive(Clone)]
struct Echo(Arc<AtomicUsize>);
impl tonic::server::NamedService for Echo {
    const NAME: &'static str = "acyclic.actors.v1.ActorsService";
}
impl tonic::server::UnaryService<wire::InvokeActorRequest> for Echo {
    type Response = wire::InvokeActorResponse;
    type Future = std::future::Ready<Result<tonic::Response<Self::Response>, tonic::Status>>;
    fn call(&mut self, request: tonic::Request<wire::InvokeActorRequest>) -> Self::Future {
        self.0.fetch_add(1, Ordering::SeqCst);
        let expected = if request.get_ref().body.first() == Some(&8) {
            "Bearer renewed"
        } else {
            "Bearer fixture"
        };
        assert_eq!(
            request
                .metadata()
                .get("authorization")
                .and_then(|value| value.to_str().ok()),
            Some(expected)
        );
        let request = request.into_inner();
        std::future::ready(Ok(tonic::Response::new(wire::InvokeActorResponse {
            status: 200,
            body: request.body,
            headers: request.headers,
        })))
    }
}
impl Service<http::Request<Body>> for Echo {
    type Response = http::Response<Body>;
    type Error = std::convert::Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        assert_eq!(
            request.uri().path(),
            "/acyclic.actors.v1.ActorsService/InvokeActor"
        );
        let service = self.clone();
        Box::pin(async move {
            Ok(tonic::server::Grpc::new(tonic_prost::ProstCodec::default())
                .unary(service, request)
                .await)
        })
    }
}

#[tokio::test]
async fn request_path_binary_calls_keep_admission_credentials_and_response_semantics()
-> Result<(), Box<dyn std::error::Error>> {
    let incoming = tonic::transport::server::TcpIncoming::bind(([127, 0, 0, 1], 0).into())?;
    let endpoint = format!("http://{}", incoming.local_addr()?);
    let calls = Arc::new(AtomicUsize::new(0));
    let (shutdown, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(Echo(Arc::clone(&calls)))
            .serve_with_incoming_shutdown(incoming, async {
                let _ = stopped.await;
            }),
    );
    let result = async {
        let channel = tonic::transport::Endpoint::from_shared(endpoint)?
            .connect()
            .await?;
        let client = Client {
            credential: None,
            inner: wire::actors_service_client::ActorsServiceClient::with_interceptor(
                crate::grpc::TracedChannel::new(channel, crate::grpc::transport_span),
                BearerAuth::new("fixture")?,
            ),
        };
        let request = wire::InvokeActorRequest {
            actor_id: "actor-a".into(),
            method: "POST".into(),
            url: "/work".into(),
            body: vec![7; 65_536],
            headers: vec![],
        };
        for _ in 0..2 {
            let response = wire::InvokeActorResponse::decode(
                client
                    .call("InvokeActor", &request.encode_to_vec())
                    .await?
                    .as_slice(),
            )?;
            assert_eq!(response.status, 200);
            assert_eq!(response.body, request.body);
        }
        let renewed = wire::InvokeActorRequest {
            body: vec![8],
            ..request.clone()
        };
        let response = wire::InvokeActorResponse::decode(
            client
                .call_with_credential("InvokeActor", &renewed.encode_to_vec(), "renewed")
                .await?
                .as_slice(),
        )?;
        assert_eq!(response.body, vec![8]);
        client.call("InvokeActor", &request.encode_to_vec()).await?;
        let invalid = wire::InvokeActorRequest {
            actor_id: String::new(),
            ..request
        };
        assert!(matches!(
            client.call("InvokeActor", &invalid.encode_to_vec()).await,
            Err(Error::Configuration(_))
        ));
        assert!(matches!(
            client.call("InvokeActor", &[0xff]).await,
            Err(Error::Configuration(_))
        ));
        assert!(matches!(
            client.call("Unknown", &[]).await,
            Err(Error::Configuration(_))
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 4);
        let renewals = Arc::new(AtomicUsize::new(0));
        let resolved = renewals.clone();
        let client = client.with_credential_provider(CredentialProvider::new(move || {
            resolved.fetch_add(1, Ordering::Relaxed);
            std::future::ready(Ok("renewed".into()))
        }));
        let typed = domain::InvokeActorRequest::try_from(renewed.clone())?;
        assert_eq!(client.invoke_actor(&typed).await?.body(), &[8]);
        let output = client.call("InvokeActor", &renewed.encode_to_vec()).await?;
        assert_eq!(
            wire::InvokeActorResponse::decode(output.as_slice())?.body,
            vec![8]
        );
        assert_eq!(renewals.load(Ordering::Relaxed), 2);
        assert_eq!(calls.load(Ordering::SeqCst), 6);
        Ok::<_, Box<dyn std::error::Error>>(())
    }
    .await;
    let _ = shutdown.send(());
    server.await??;
    result
}

#[test]
#[ignore = "opt-in debug-profile request-path measurement: npm run bench:clients"]
fn request_path_measure_admitted_request_conversion() -> Result<(), Error> {
    for consume in [false, true] {
        let requests: Result<Vec<_>, _> = (0..256)
            .map(|_| {
                domain::InvokeActorRequest::try_from(wire::InvokeActorRequest {
                    actor_id: "actor-a".into(),
                    method: "POST".into(),
                    url: "/work".into(),
                    body: vec![7; 65_536],
                    headers: vec![],
                })
            })
            .collect();
        let requests = requests?;
        let started = std::time::Instant::now();
        for request in requests {
            let wire =
                wire::InvokeActorRequest::from(if consume { request } else { request.clone() });
            std::hint::black_box(wire);
        }
        eprintln!(
            "actors admitted request conversion consume={consume} iterations=256 bytes=65536 elapsed_us={}",
            started.elapsed().as_micros()
        );
    }
    Ok(())
}
