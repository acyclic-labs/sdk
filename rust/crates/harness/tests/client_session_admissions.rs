//! Admission publication failures retain the exact durable retry identity.
use super::*;
use prost::Message as _;
use std::io::Write;

#[derive(Default)]
struct RejectAdmission(Vec<u8>);

impl Write for RejectAdmission {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let frame = wire::ServerFrame::decode(bytes).unwrap();
        if matches!(frame.frame, Some(wire::server_frame::Frame::Admission(_))) {
            return Err(std::io::Error::other("stdout rejected admission"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn assert_retry(state: &ClientSession<MemoryClientState>) {
    assert_eq!(
        state.pending().cloned().collect::<Vec<_>>(),
        [command()],
        "failed publication must retain retry intent"
    );
}

async fn stdout_retry(reopen: bool, padding: usize) {
    let host = Arc::new(Host::default());
    host.requires_admission.store(true, Ordering::Relaxed);
    let mut state = session();
    state.queue(command()).unwrap();
    for index in 0..padding {
        let mut cursor = state.resume().cursors.remove(0);
        cursor.authority.as_mut().unwrap().id = format!("padding-{index}");
        state.track(cursor).unwrap();
    }
    let mut client = HarnessClient::connect(host.clone(), state).await.unwrap();
    let mut second = command();
    second.operation = Some(wire::OperationIdentity {
        operation_id: "00000000-0000-0000-0000-000000000002".into(),
        idempotency_key: "submit-2".into(),
    });
    assert!(
        client
            .flush_outbox(|response| {
                RejectAdmission::default()
                    .write_all(
                        wire::ServerFrame {
                            frame: Some(wire::server_frame::Frame::Admission(response)),
                        }
                        .encode_to_vec()
                        .as_slice(),
                    )
                    .map_err(|error| Error::Storage(error.to_string()))
            })
            .await
            .is_err()
    );
    assert_retry(client.session());
    let state = client.into_session();
    let state = if reopen {
        ClientSession::open(state.into_store()).unwrap()
    } else {
        state
    };
    assert_retry(&state);
    let mut client = HarnessClient::connect(host.clone(), state).await.unwrap();
    let mut admissions = Vec::new();
    client
        .submit(second.clone(), |response| {
            admissions.push(response);
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(
        admissions,
        [
            admission(&command(), wire::AdmissionState::Accepted),
            admission(&second, wire::AdmissionState::Accepted)
        ]
    );
    assert_eq!(client.session().pending().count(), 0);
    assert_eq!(
        host.commands.lock().unwrap().as_slice(),
        &[command(), command(), second]
    );
}

#[tokio::test]
async fn publication_failure_retains_predecessor_for_retry_and_reopen() {
    for reopen in [false, true] {
        for padding in [0, 61] {
            stdout_retry(reopen, padding).await;
        }
    }
}

#[test]
#[should_panic(expected = "failed publication must retain retry intent")]
fn negative_control_commit_before_stdout_loses_retry_intent() {
    // Deliberately reconstruct the old ordering with the same production journal
    // admission path and the same failing binary sink. The retry oracle rejects it.
    let mut state = session();
    state.queue(command()).unwrap();
    let response = wire::ServerFrame {
        frame: Some(wire::server_frame::Frame::Admission(admission(
            &command(),
            wire::AdmissionState::Accepted,
        ))),
    };
    state.receive(response.clone()).unwrap();
    assert!(
        RejectAdmission::default()
            .write_all(response.encode_to_vec().as_slice())
            .is_err()
    );
    assert_retry(&ClientSession::open(state.into_store()).unwrap());
}

proptest::proptest! {
    // Property: no valid admission can remove intent when its sink rejects it,
    // for all admission decisions and every position in the checkpoint interval.
    #[test]
    fn rejected_admission_publication_preserves_exact_pending_intent(
        state in 0usize..3,
        padding in 0usize..62,
    ) {
        futures::executor::block_on(async {
            let host = Arc::new(Host::default());
            *host.admission_state.lock().unwrap() = Some([wire::AdmissionState::Accepted, wire::AdmissionState::Rejected, wire::AdmissionState::Indeterminate][state]);
            let mut state = session();
            state.queue(command()).unwrap();
            for index in 0..padding {
                let mut cursor = state.resume().cursors.remove(0);
                cursor.authority.as_mut().unwrap().id = format!("padding-{index}");
                state.track(cursor).unwrap();
            }
            let mut client = HarnessClient::connect(host, state).await.unwrap();
            assert!(client.flush_outbox(|_| Err(Error::Storage("publication failed".into()))).await.is_err());
            assert_retry(client.session());
            assert_retry(&ClientSession::open(client.into_session().into_store()).unwrap());
        });
    }
}
