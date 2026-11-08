use super::*;

#[derive(Clone, Copy)]
enum Phase {
    Prepare,
    Open,
    Output,
    Input,
}

struct Rejected {
    phase: Phase,
    error: StreamError,
}

impl SocketProvider for Rejected {
    fn prepare(self: Arc<Self>, _: Bytes) -> Result<Box<dyn PreparedSocketOpen>, StreamError> {
        if matches!(self.phase, Phase::Prepare) {
            return Err(self.error.clone());
        }
        Ok(Box::new(self))
    }
}

impl PreparedSocketOpen for Arc<Rejected> {
    fn resource(&self) -> &str {
        "actors:/refusal"
    }

    fn open(
        self: Box<Self>,
        authorization: CallAuthorization,
    ) -> Result<SocketChannel, StreamError> {
        authorization.authorize(
            self.resource(),
            &[],
            &acyclic_auth::contract::RecordAttributes::try_new(vec![])
                .map_err(|_| StreamError::InvalidArgument)?,
            acyclic_auth::authority::PolicyDecision::Allow,
        )?;
        if matches!(self.phase, Phase::Open) {
            return Err(self.error.clone());
        }
        let terminal = if matches!(self.phase, Phase::Output) {
            stream::once(future::ready(Err(self.error.clone()))).boxed()
        } else {
            stream::pending().boxed()
        };
        Ok(SocketChannel {
            outbound: stream::once(future::ready(Ok(Bytes::from_static(b"accepted"))))
                .chain(terminal)
                .boxed(),
            inbound: Box::new((*self).clone()),
        })
    }
}

impl SocketInbound for Arc<Rejected> {
    fn receive(&self, _: Bytes) -> Result<(), StreamError> {
        Err(self.error.clone())
    }
}

#[tokio::test]
async fn local_socket_refusals_retain_the_original_error_after_prior_data() -> TestResult {
    // Capture a real OS refusal at runtime, then retain the shared status
    // producer's original error across every local socket failure boundary.
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    drop(listener);
    let refused = tokio::net::TcpStream::connect(address).await.unwrap_err();
    let status = tonic::Status::from_error(Box::new(refused));
    let original = crate::grpc::status(&status);
    assert!(matches!(original, StreamError::Refused(_)));
    for phase in [Phase::Prepare, Phase::Open, Phase::Output, Phase::Input] {
        let client = Client::in_process_with_sockets(
            Arc::new(MemoryStream::default()),
            testing::authority(),
            None,
            Arc::new(Rejected {
                phase,
                error: original.clone(),
            }),
            Arc::new(crate::runtime::PlatformExecutor),
            None,
        );
        client.register_credential(
            0,
            testing::credential_with_grants(vec![acyclic_auth::contract::ResourceGrant {
                resource: acyclic_auth::contract::ResourcePattern {
                    name: "actors:/refusal".into(),
                    prefix: false,
                },
                actions: vec!["actors/invoke".into()],
            }]),
        )?;
        let actual = match client.actor_socket(Bytes::from_static(b"open"), 0).await {
            Err(error) => error,
            Ok(mut socket) => {
                assert_eq!(socket.next().await?, Some(Bytes::from_static(b"accepted")));
                if matches!(phase, Phase::Input) {
                    socket.send(Bytes::from_static(b"input")).await?;
                }
                socket.next().await.unwrap_err()
            }
        };
        assert_eq!(actual, original, "local socket lost its original refusal");
    }
    Ok(())
}
