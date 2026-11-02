//! Deterministic lost-reply schedules through the actual account signer and
//! canonical verifier. The single-record model is the serving owner's atomic
//! store cut; it deliberately exposes omission of that cut as a negative control.
use super::*;
use crate::{
    contract::*,
    operation::{Clock, StopFuture, Stopped},
    trust::{AccountRoot, Issuer, Snapshot},
};
use ed25519_dalek::{Signer, SigningKey};
use futures::{channel::oneshot, executor::block_on, future::pending};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};

struct OriginalClock(AtomicU64);
impl Clock for OriginalClock {
    fn now_unix_millis(&self) -> Result<u64, Stopped> {
        Ok(self.0.load(Ordering::SeqCst))
    }
    fn expires_at(&self, _: u64) -> Result<StopFuture, Stopped> {
        panic!("issuance must retain its original stop, never register a timer")
    }
}

fn context(clock: Arc<OriginalClock>) -> Context {
    Context::from_clock(2_000_000, pending(), clock)
}

fn request(operation: u8, epoch: u64, change: impl FnOnce(&mut CertifiedKey)) -> Request {
    let root = crate::testing::root(|_| {});
    let verified = crate::verify::verify(&root, &crate::testing::Snapshot::default(), 100).unwrap();
    let mut claims = verified.blocks()[0].certificates.as_ref().unwrap().keys[0]
        .key
        .clone();
    claims.key_epoch = epoch;
    claims.parent_revocation_ids = vec![vec![7; 32]];
    change(&mut claims);
    Request::new(
        [operation; 32],
        SigningKey::from_bytes(&[2; 32]).verifying_key().to_bytes(),
        "test".into(),
        claims.into(),
    )
    .unwrap()
}

struct Stored {
    key: BlockId,
    digest: BlockId,
    certificate: CertificateChainProto,
}

struct IssuerModel {
    root: AccountRoot,
    birth: BirthCertificate,
    record: Option<Stored>,
    signatures: usize,
    issues: usize,
    lookups: usize,
    lose_reply: bool,
    retain: bool,
}

impl IssuerModel {
    fn new(retain: bool) -> (Self, Snapshot) {
        let environment = Issuer::new("test", SigningKey::from_bytes(&[1; 32]));
        let root_key = SigningKey::from_bytes(&[2; 32]);
        let birth = environment
            .birth("account", &root_key.verifying_key(), 1)
            .unwrap();
        let (root, _) = environment.genesis("account", root_key, 1).unwrap();
        let trust = Snapshot {
            environment: "test".into(),
            environment_keys: environment.anchor(),
            account: "account".into(),
            issuer_keys: vec![SigningKey::from_bytes(&[2; 32]).verifying_key().to_bytes()],
            minimum_key_epoch: root.minimum_key_epoch(),
            revoked_key_ids: Default::default(),
            revoked_block_ids: Default::default(),
        };
        (
            Self {
                root,
                birth,
                record: None,
                signatures: 0,
                issues: 0,
                lookups: 0,
                lose_reply: true,
                retain,
            },
            trust,
        )
    }

    fn lookup(&self, request: &Request) -> Reply {
        let decision = match &self.record {
            Some(record) if record.key == *request.operation_key() => {
                if record.digest == *request.digest() {
                    Decision::Accepted(record.certificate.clone())
                } else {
                    Decision::Refused(Refusal::Conflict)
                }
            }
            _ => Decision::Unknown,
        };
        Reply {
            request_digest: *request.digest(),
            decision,
        }
    }

    fn issue(&mut self, request: &Request) -> Result<Reply, Unknown> {
        self.issues += 1;
        if self.record.is_some() {
            return Ok(self.lookup(request));
        }
        let certificates = self.root.certify_request(&self.birth, request).unwrap();
        self.signatures += 1;
        if self.retain {
            self.record = Some(Stored {
                key: *request.operation_key(),
                digest: *request.digest(),
                certificate: certificates.into(),
            });
        }
        if std::mem::take(&mut self.lose_reply) {
            Err(Unknown::Unavailable)
        } else {
            Ok(self.lookup(request))
        }
    }
}

impl Provider for Mutex<IssuerModel> {
    fn issue<'a>(&'a self, _: &'a Context, request: &'a Request) -> Pending<'a> {
        Box::pin(async move { self.lock().unwrap().issue(request) })
    }
    fn recover<'a>(&'a self, _: &'a Context, request: &'a Request) -> Pending<'a> {
        Box::pin(async move {
            let mut owner = self.lock().unwrap();
            owner.lookups += 1;
            Ok(owner.lookup(request))
        })
    }
}

#[test]
fn same_key_recovery_preserves_original_issuance_and_current_authority() {
    block_on(async {
        let (model, mut trust) = IssuerModel::new(true);
        let epoch = model.root.minimum_key_epoch();
        let clock = Arc::new(OriginalClock(AtomicU64::new(100_000)));
        let mut client = Client::new(
            Mutex::new(model),
            context(clock.clone()),
            request(8, epoch, |_| {}),
        );
        assert!(matches!(
            client.issue(&trust).await.unwrap(),
            Outcome::Unknown(Unknown::Unavailable)
        ));
        let before = crate::crypto::signature_verifications();
        let accepted = match client.recover(&trust).await.unwrap() {
            Outcome::Accepted(certificates) => certificates,
            _ => panic!("lost acknowledgement must recover the retained certificate"),
        };
        assert_eq!(crate::crypto::signature_verifications() - before, 2);
        // Repeated issue with the exact key is a replay, not another signing.
        let before = crate::crypto::signature_verifications();
        assert!(matches!(
            client.issue(&trust).await.unwrap(),
            Outcome::Accepted(_)
        ));
        assert_eq!(crate::crypto::signature_verifications() - before, 0);
        {
            let owner = client.provider.lock().unwrap();
            assert_eq!((owner.signatures, owner.issues, owner.lookups), (1, 2, 1));
            assert_eq!(
                owner.record.as_ref().unwrap().certificate,
                CertificateChainProto::from(accepted.clone())
            );
        }
        // Same key with changed authority cannot replace the retained decision.
        let changed = request(8, epoch, |claims| claims.principal.id = "foreign".into());
        assert!(matches!(
            client
                .provider
                .issue(&client.context, &changed)
                .await
                .unwrap()
                .decision,
            Decision::Refused(Refusal::Conflict)
        ));
        // An absent lookup is not a refusal and cannot trigger fresh issuance.
        let missing = request(9, epoch, |_| {});
        assert!(matches!(
            client
                .provider
                .recover(&client.context, &missing)
                .await
                .unwrap()
                .decision,
            Decision::Unknown
        ));
        assert_eq!(client.provider.lock().unwrap().signatures, 1);

        trust
            .revoked_key_ids
            .insert(accepted.keys[0].key.key_id.clone());
        assert!(matches!(
            client.recover(&trust).await.unwrap(),
            Outcome::Unknown(Unknown::Authority(VerifyError::Revoked))
        ));
        trust.revoked_key_ids.clear();
        assert!(matches!(
            client.recover(&trust).await.unwrap(),
            Outcome::Accepted(_)
        ));
        trust.revoked_block_ids.insert([7; 32]);
        assert!(matches!(
            client.recover(&trust).await.unwrap(),
            Outcome::Unknown(Unknown::Authority(VerifyError::Revoked))
        ));
        trust.revoked_block_ids.clear();

        // A valid signature by ANOTHER currently trusted account issuer must
        // not settle an operation pinned to the original root.
        let foreign = SigningKey::from_bytes(&[6; 32]);
        let mut other = accepted.clone();
        let mut transcript = Vec::new();
        crate::crypto::claims_message(
            &mut transcript,
            crate::KEY_DOMAIN,
            &CertifiedKeyProto::from(other.keys[0].key.clone()),
        )
        .unwrap();
        other.keys[0].signature = foreign.sign(&transcript).to_bytes().to_vec();
        trust.issuer_keys.push(foreign.verifying_key().to_bytes());
        assert_eq!(
            CertificateVerifier::default()
                .verify_certificates(other.clone().into(), client.request.issuer(), &trust, 100)
                .err(),
            Some(VerifyError::Trust)
        );
        // Negative control: the SAME signed certificate is acceptable for an
        // explicit request to that other trusted issuer.
        assert!(
            CertificateVerifier::default()
                .verify_certificates(
                    other.into(),
                    &foreign.verifying_key().to_bytes(),
                    &trust,
                    100
                )
                .is_ok()
        );

        // Even a genuine certificate cannot substitute changed request claims.
        {
            let mut owner = client.provider.lock().unwrap();
            let changed = request(8, epoch, |claims| claims.grants.grants.clear());
            let certificate = owner.root.certify_request(&owner.birth, &changed).unwrap();
            owner.record.as_mut().unwrap().certificate = certificate.into();
        }
        assert!(matches!(
            client.recover(&trust).await.unwrap(),
            Outcome::Unknown(Unknown::InvalidReply)
        ));
        client
            .provider
            .lock()
            .unwrap()
            .record
            .as_mut()
            .unwrap()
            .certificate = accepted.clone().into();
        trust.issuer_keys.clear();
        assert!(matches!(
            client.recover(&trust).await.unwrap(),
            Outcome::Unknown(Unknown::Authority(_))
        ));
        trust.issuer_keys.push(*client.request.issuer());
        // Time is read after the remote wait from the original clock, not a
        // caller timestamp captured before dispatch.
        clock.0.store(
            accepted.keys[0].key.validity.expires_at_unix_seconds * 1000,
            Ordering::SeqCst,
        );
        assert!(matches!(
            client.recover(&trust).await.unwrap(),
            Outcome::Unknown(Unknown::Authority(VerifyError::Time))
        ));

        // Negative control: deleting the original commit cut loses recovery
        // and a repeat signs twice even though the caller preserved the key.
        let (mut broken, _) = IssuerModel::new(false);
        let original = request(8, epoch, |_| {});
        assert!(broken.issue(&original).is_err());
        assert!(matches!(
            broken.lookup(&original).decision,
            Decision::Unknown
        ));
        let _ = broken.issue(&original);
        assert_eq!(broken.signatures, 2);
        broken.root.raise_minimum_epoch(epoch + 1, 100).unwrap();
        assert_eq!(
            broken.root.certify_request(&broken.birth, &original).err(),
            Some(VerifyError::Trust)
        );
    });
}

struct Source {
    reply: Mutex<Option<oneshot::Receiver<Reply>>>,
    entered: Mutex<Option<oneshot::Sender<()>>>,
}
impl Provider for Source {
    fn issue<'a>(&'a self, _: &'a Context, _: &'a Request) -> Pending<'a> {
        Box::pin(async move {
            let _ = self.entered.lock().unwrap().take().unwrap().send(());
            let receive = self.reply.lock().unwrap().take().unwrap();
            receive.await.map_err(|_| Unknown::Unavailable)
        })
    }
    fn recover<'a>(&'a self, context: &'a Context, request: &'a Request) -> Pending<'a> {
        self.issue(context, request)
    }
}

#[test]
fn original_stop_and_source_loss_settle_issuance_without_a_reply() {
    block_on(async {
        for stopped in [false, true] {
            let (_reply, receive) = oneshot::channel();
            let (entered, _ready) = oneshot::channel();
            let context = if stopped {
                Context::from_clock(
                    1_000_000,
                    futures::future::ready(Stopped::Cancelled),
                    Arc::new(OriginalClock(AtomicU64::new(100_000))),
                )
            } else {
                Context::from_stopped(1_000_000, pending())
            };
            let mut client = Client::new(
                Source {
                    reply: Mutex::new(Some(receive)),
                    entered: Mutex::new(Some(entered)),
                },
                context,
                request(8, 0, |_| {}),
            );
            let error = client
                .issue(&crate::testing::Snapshot::default())
                .await
                .err();
            assert_eq!(
                error,
                Some(if stopped {
                    BudgetError::Stopped(Stopped::Cancelled)
                } else {
                    BudgetError::ClockUnavailable
                })
            );
            assert!(client.provider.entered.lock().unwrap().is_some());
        }
        for stop in [false, true] {
            let (send, receive) = oneshot::channel();
            let (entered, ready) = oneshot::channel();
            let (cancel, cancelled) = oneshot::channel();
            let context = Context::from_clock(
                1_000_000,
                async move {
                    let _ = cancelled.await;
                    Stopped::Cancelled
                },
                Arc::new(OriginalClock(AtomicU64::new(100_000))),
            );
            let mut client = Client::new(
                Source {
                    reply: Mutex::new(Some(receive)),
                    entered: Mutex::new(Some(entered)),
                },
                context,
                request(8, 0, |_| {}),
            );
            let trust = crate::testing::Snapshot::default();
            let operation = client.issue(&trust);
            let terminal = async move {
                ready.await.unwrap();
                if stop {
                    drop(cancel);
                    // Keep the remote response open: only the original caller
                    // producer loss can end this wait.
                    let _held_reply = send;
                    pending::<()>().await;
                } else {
                    drop(send);
                    let _held_caller = cancel;
                    pending::<()>().await;
                }
            };
            futures::pin_mut!(operation, terminal);
            match futures::future::select(operation, terminal).await {
                futures::future::Either::Left((Ok(outcome), _)) => {
                    if stop {
                        assert!(matches!(
                            outcome,
                            Outcome::Unknown(Unknown::Stopped(BudgetError::Stopped(
                                Stopped::Cancelled
                            )))
                        ));
                    } else {
                        assert!(matches!(outcome, Outcome::Unknown(Unknown::Unavailable)));
                    }
                }
                _ => panic!("original primitive terminal must settle the operation"),
            }
        }
    });
}
