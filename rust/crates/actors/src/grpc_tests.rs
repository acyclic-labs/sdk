#![allow(
    clippy::unwrap_used,
    clippy::panic,
    reason = "bounded generated-client counterexamples"
)]
use crate::{grpc::*, wire};
use std::{
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use tonic::Status;
use tonic::{
    body::Body,
    codegen::{Bytes, Service, http},
};
use tracing_subscriber::{
    layer::{Context as LayerContext, SubscriberExt},
    registry::LookupSpan,
};

struct Data(std::collections::VecDeque<http_body::Frame<Bytes>>);
impl http_body::Body for Data {
    type Data = Bytes;
    type Error = Status;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, Status>>> {
        Poll::Ready(self.0.pop_front().map(Ok))
    }
    fn is_end_stream(&self) -> bool {
        self.0.is_empty()
    }
}
struct Transport(Option<Body>);
impl Service<http::Request<Body>> for Transport {
    type Response = http::Response<Body>;
    type Error = Status;
    type Future = std::future::Ready<Result<Self::Response, Status>>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Status>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, _: http::Request<Body>) -> Self::Future {
        std::future::ready(Ok(http::Response::builder()
            .header("content-type", "application/grpc")
            .header("grpc-status", "0")
            .body(self.0.take().unwrap())
            .unwrap()))
    }
}
type Log = Arc<Mutex<Vec<(String, String, String)>>>;
struct Capture(Log);
struct Fields<'a>(&'a str, Log);
impl tracing::field::Visit for Fields<'_> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.1.lock().unwrap().push((
            self.0.into(),
            field.name().into(),
            format!("{value:?}").trim_matches('"').into(),
        ));
    }
}
impl<S: tracing::Subscriber + for<'a> LookupSpan<'a>> tracing_subscriber::Layer<S> for Capture {
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        _: &tracing::span::Id,
        _: LayerContext<'_, S>,
    ) {
        attrs.record(&mut Fields(attrs.metadata().name(), self.0.clone()));
    }
    fn on_record(
        &self,
        id: &tracing::span::Id,
        record: &tracing::span::Record<'_>,
        ctx: LayerContext<'_, S>,
    ) {
        record.record(&mut Fields(ctx.span(id).unwrap().name(), self.0.clone()));
    }
    fn on_close(&self, id: tracing::span::Id, ctx: LayerContext<'_, S>) {
        self.0.lock().unwrap().push((
            ctx.span(&id).unwrap().name().into(),
            "closed".into(),
            "1".into(),
        ));
    }
}
#[tokio::test]
async fn typed_result_matches_decoder_while_transport_reports_wire_status() {
    for case in 0..5 {
        let log = Log::default();
        let _guard = tracing::subscriber::set_default(
            tracing_subscriber::registry().with(Capture(log.clone())),
        );
        // Valid gRPC frame envelope, invalid protobuf payload; the other
        // case has no unary response message despite transport status OK.
        let body = if case == 0 {
            Body::empty()
        } else {
            let bytes = if case == 1 {
                &[0, 0, 0, 0, 1, 0xff][..]
            } else {
                &[0, 0, 0, 0, 0][..]
            };
            let mut frames = std::collections::VecDeque::from([http_body::Frame::data(
                Bytes::copy_from_slice(bytes),
            )]);
            if case >= 2 {
                let mut trailers = http::HeaderMap::new();
                if case == 3 {
                    trailers.insert("untouched", "value".parse().unwrap());
                }
                if case == 4 {
                    trailers.insert("grpc-status", "14".parse().unwrap());
                }
                frames.push_back(http_body::Frame::trailers(trailers));
            }
            Body::new(Data(frames))
        };
        let transport =
            acyclic_grpc_observability::TracedChannel::<_, CallSpan>::new(Transport(Some(body)));
        let mut client = wire::actors_service_client::ActorsServiceClient::new(transport);
        let result = client
            .inspect_actor(wire::InspectActorRequest::default())
            .await;
        if case < 2 {
            let error = result.unwrap_err();
            assert_eq!(error.code(), tonic::Code::Internal);
            if case == 0 {
                assert_eq!(error.message(), "Missing response message.");
            }
        } else {
            assert!(result.is_ok());
        }
        let seen = log.lock().unwrap();
        for name in ["acyclic.actors.grpc.call", "acyclic.actors.grpc.transport"] {
            assert_eq!(
                seen.iter()
                    .filter(|(n, k, _)| n == name && k == "closed")
                    .count(),
                1
            );
        }
        for (name, field, value) in [
            (
                "acyclic.actors.grpc.call",
                "outcome",
                if case < 2 { "err" } else { "ok" },
            ),
            (
                "acyclic.actors.grpc.call",
                "rpc.code",
                if case < 2 { "13" } else { "0" },
            ),
            ("acyclic.actors.grpc.call", "outcome.scope", "typed_rpc"),
            (
                "acyclic.actors.grpc.transport",
                "outcome",
                if case == 4 { "err" } else { "ok" },
            ),
            (
                "acyclic.actors.grpc.transport",
                "rpc.code",
                if case == 4 { "14" } else { "0" },
            ),
            (
                "acyclic.actors.grpc.transport",
                "outcome.scope",
                "grpc_transport",
            ),
        ] {
            assert!(
                seen.iter()
                    .any(|(n, k, v)| (n.as_str(), k.as_str(), v.as_str()) == (name, field, value)),
                "{name} {field} {value}: {seen:?}"
            );
        }
        assert!(!seen.iter().any(|(n, k, v)| n == "acyclic.actors.grpc.call"
            && k == "outcome"
            && v == if case < 2 { "ok" } else { "err" }));
    }
}

#[derive(Clone)]
struct TlsService;
impl tonic::server::NamedService for TlsService {
    const NAME: &'static str = "acyclic.actors.v1.ActorsService";
}
impl Service<http::Request<Body>> for TlsService {
    type Response = http::Response<Body>;
    type Error = std::convert::Infallible;
    type Future = std::future::Ready<Result<Self::Response, Self::Error>>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        assert_eq!(
            request.uri().path(),
            "/acyclic.actors.v1.ActorsService/InspectActor"
        );
        assert_eq!(request.headers()["authorization"], "Bearer private-token");
        let mut trailers = http::HeaderMap::new();
        trailers.insert("grpc-status", "0".parse().unwrap());
        let frames = std::collections::VecDeque::from([
            http_body::Frame::data(Bytes::from_static(&[0, 0, 0, 0, 0])),
            http_body::Frame::trailers(trailers),
        ]);
        std::future::ready(Ok(http::Response::builder()
            .header("content-type", "application/grpc")
            .body(Body::new(Data(frames)))
            .unwrap()))
    }
}
#[tokio::test]
async fn authenticated_tls_generated_call_closes_typed_and_wire_spans()
-> Result<(), Box<dyn std::error::Error>> {
    let log = Log::default();
    let _guard =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(Capture(log.clone())));
    let certificate = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()])?;
    let pem = certificate.cert.pem();
    let identity =
        tonic::transport::Identity::from_pem(&pem, certificate.signing_key.serialize_pem());
    let incoming = tonic::transport::server::TcpIncoming::bind(([127, 0, 0, 1], 0).into())?;
    let address = incoming.local_addr()?;
    let (shutdown, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .tls_config(tonic::transport::ServerTlsConfig::new().identity(identity))?
            .add_service(TlsService)
            .serve_with_incoming_shutdown(incoming, async {
                let _ = stopped.await;
            }),
    );
    let result = async {
        let mut client = connect_with_ca_certificate(
            &format!("https://{address}"),
            "private-token",
            Some(pem.as_bytes()),
        )
        .await?;
        client
            .inspect_actor(wire::InspectActorRequest::default())
            .await?;
        Ok::<_, Box<dyn std::error::Error>>(())
    }
    .await;
    let _ = shutdown.send(());
    server.await??;
    result?;
    let seen = log.lock().unwrap();
    for name in ["acyclic.actors.grpc.call", "acyclic.actors.grpc.transport"] {
        assert!(
            seen.iter()
                .any(|(n, k, v)| n == name && k == "rpc" && v == "InspectActor")
        );
        assert_eq!(
            seen.iter()
                .filter(|(n, k, _)| n == name && k == "closed")
                .count(),
            1
        );
        assert!(
            seen.iter()
                .any(|(n, k, v)| n == name && k == "outcome" && v == "ok")
        );
        assert!(
            seen.iter()
                .any(|(n, k, v)| n == name && k == "rpc.code" && v == "0")
        );
    }
    assert!(!format!("{seen:?}").contains("private-token"));
    Ok(())
}
