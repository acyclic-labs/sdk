//! Real shared client + signed authority, against a disposable contract provider.
#[path = "common/signed.rs"]
mod signed;
use acyclic_auth::{
    authority::{PolicyDecision, Scope, Target as AuthorityTarget},
    contract::*,
    credential::CredentialProvider,
    operation::ProtectedBytes,
    secrets::{self, Client, Error, Response, Transport, TransportFuture},
    verify::{VerifiedCapability, verify},
};
use futures::executor::block_on;
use prost::Message;
use std::{
    collections::BTreeMap,
    ops::Bound,
    sync::{Arc, Mutex},
};
use zeroize::Zeroizing;

type TestResult = Result<(), Box<dyn std::error::Error>>;
const PLANTED: &str = "PLANTED_SECRET_DO_NOT_LOG_6547";

fn scope_grant(actions: &[&str]) -> Grant {
    Grant {
        kind: GrantKind::Resource(ResourceGrant {
            resource: ResourcePattern {
                name: secrets::scope_resource("fixture"),
                prefix: true,
            },
            actions: actions.iter().map(|action| (*action).into()).collect(),
        }),
    }
}
fn grants() -> GrantSet {
    GrantSet {
        grants: vec![scope_grant(&[secrets::PUT, secrets::LIST, secrets::DELETE])],
    }
}
// The denied holder keeps ordinary product grants on the same names.
fn product_grants() -> GrantSet {
    GrantSet {
        grants: vec![Grant {
            kind: GrantKind::Resource(ResourceGrant {
                resource: ResourcePattern {
                    name: "fs:/fixture".into(),
                    prefix: true,
                },
                actions: vec!["fs/read".into(), "fs/write".into(), "fs/list".into()],
            }),
        }],
    }
}
fn capability(allowed: bool) -> Result<VerifiedCapability, Box<dyn std::error::Error>> {
    let bytes = signed::root(|block| {
        if let Some(certs) = &mut block.certificates {
            for cert in &mut certs.keys {
                cert.key.grants = if allowed { grants() } else { product_grants() };
            }
        }
    });
    Ok(verify(&bytes, &signed::Snapshot::default(), signed::NOW)?)
}
fn authorizes(
    authority: &VerifiedCapability,
    action: &str,
    resource: &str,
    policy: PolicyDecision,
) -> bool {
    let attributes = RecordAttributes::default();
    let usage = signed::limits(None);
    let mut request = signed::request(resource, &attributes, &usage);
    request.target = AuthorityTarget::Resource { action, resource };
    authority
        .authorize(&signed::Snapshot::default(), &request, policy, Scope::Exact)
        .is_ok()
}

#[derive(Default)]
struct State {
    generation: u64,
    rows: BTreeMap<String, (SecretPutRequest, u64)>,
}
#[derive(Clone, Copy, Default)]
struct Mutants {
    ignore_cas: bool,
    leak: bool,
    retain_deleted: bool,
    /// Continues a listing across a mutation instead of failing typed.
    unfenced: bool,
}
struct Provider {
    state: Arc<Mutex<State>>,
    authority: VerifiedCapability,
    mutants: Mutants,
}
fn response(status: u16, value: impl serde::Serialize) -> Result<Response, Error> {
    Ok(Response {
        status,
        body: Zeroizing::new(serde_json::to_vec(&value).map_err(|_| Error::InvalidResponse)?),
    })
}
enum Call {
    Put(SecretPutRequest),
    List(SecretListRequest),
    Delete(SecretDeleteRequest),
}
impl Provider {
    fn call(&self, path: &str, body: &[u8]) -> Result<Response, Error> {
        let decode = |_| Error::InvalidRequest;
        let call = match path {
            "/v1/identity/secrets/put" => Call::Put(serde_json::from_slice(body).map_err(decode)?),
            "/v1/identity/secrets/list" => {
                Call::List(serde_json::from_slice(body).map_err(decode)?)
            }
            "/v1/identity/secrets/delete" => {
                Call::Delete(serde_json::from_slice(body).map_err(decode)?)
            }
            _ => return Err(Error::InvalidRequest),
        };
        // The one authorization path: the typed resource grant intersection.
        let (action, resource) = match &call {
            Call::Put(request) => secrets::put_target(request),
            Call::List(request) => secrets::list_target(request),
            Call::Delete(request) => secrets::delete_target(request),
        };
        if !authorizes(&self.authority, action, &resource, PolicyDecision::Allow) {
            return response(403, PLANTED);
        }
        let mut state = self.state.lock().map_err(|_| Error::Transport)?;
        let (name, expected, put) = match call {
            Call::List(request) => return self.list(&state, &request),
            Call::Put(request) => (
                request.name.clone(),
                request.expected_generation,
                Some(request),
            ),
            Call::Delete(request) => (request.name, request.expected_generation, None),
        };
        if expected != state.generation && !self.mutants.ignore_cas {
            return response(409, PLANTED);
        }
        state.generation += 1;
        let version = state.generation;
        let kind = if let Some(row) = put {
            let kind = row.kind;
            state.rows.insert(name.clone(), (row, version));
            kind
        } else {
            if !self.mutants.retain_deleted {
                state.rows.remove(&name);
            }
            SecretKind::Deleted
        };
        response(
            200,
            SecretMutationReply {
                name,
                generation: version,
                version,
                kind,
            },
        )
    }
    // O(page): an ordered range read from the cursor, never a scan.
    fn list(&self, state: &State, request: &SecretListRequest) -> Result<Response, Error> {
        if self.mutants.leak {
            return response(
                200,
                serde_json::json!({"generation":state.generation,"variables":[
                    {"kind":"protected","name":"protected","version":1,"value":PLANTED}]}),
            );
        }
        let after = match &request.cursor {
            Some(cursor) if cursor.generation != state.generation && !self.mutants.unfenced => {
                return response(409, PLANTED);
            }
            Some(cursor) => Bound::Excluded(cursor.after.clone()),
            None => Bound::Unbounded,
        };
        let chunk = match request.page_size {
            0 => 2,
            size => size as usize,
        };
        let mut rows = state.rows.range((after, Bound::Unbounded));
        let variables: Vec<_> = rows
            .by_ref()
            .take(chunk)
            .map(|(name, (row, version))| SecretVariable {
                kind: match row.kind {
                    SecretKind::Plain => SecretVariableKind::Plain(PlainSecretVariable {
                        name: name.clone(),
                        version: *version,
                        value: row.value.clone(),
                    }),
                    _ => SecretVariableKind::Protected(ProtectedSecretVariable {
                        name: name.clone(),
                        version: *version,
                    }),
                },
            })
            .collect();
        let next_cursor = rows
            .next()
            .and(variables.last())
            .map(|last| SecretListCursor {
                generation: state.generation,
                after: last.identity().0.into(),
            });
        // An unfenced provider answers at the cursor's generation, mixing two.
        let generation = match (&request.cursor, self.mutants.unfenced) {
            (Some(cursor), true) => cursor.generation,
            _ => state.generation,
        };
        response(
            200,
            SecretListReply {
                generation,
                variables,
                next_cursor,
            },
        )
    }
}
impl Transport for Provider {
    fn post<'a>(
        &'a self,
        path: &'static str,
        _bearer: &'a str,
        body: ProtectedBytes,
    ) -> TransportFuture<'a> {
        Box::pin(async move { self.call(path, body.as_ref()) })
    }
}
fn client(
    state: &Arc<Mutex<State>>,
    allowed: bool,
    mutants: Mutants,
) -> Result<Client<Provider>, Box<dyn std::error::Error>> {
    Ok(Client::new(
        Provider {
            state: state.clone(),
            authority: capability(allowed)?,
            mutants,
        },
        CredentialProvider::new(|| async { Ok("fixture-bearer".into()) }),
    ))
}
fn list_request(cursor: Option<SecretListCursor>, page_size: u32) -> SecretListRequest {
    SecretListRequest {
        scope: "fixture".into(),
        cursor,
        page_size,
    }
}

#[test]
fn secrets_provider_conformance_and_cas_authority_visibility_fence_negative_controls() -> TestResult
{
    block_on(async {
        let none = Mutants::default();
        for (mutants, expected) in [
            (none, Ok(())),
            (
                Mutants {
                    ignore_cas: true,
                    ..none
                },
                Err("secrets stale generation did not conflict"),
            ),
            (
                Mutants { leak: true, ..none },
                Err("secrets/list initial failed"),
            ),
            (
                Mutants {
                    retain_deleted: true,
                    ..none
                },
                Err("secrets/delete receipts did not match the stored scope"),
            ),
            (
                Mutants {
                    unfenced: true,
                    ..none
                },
                Err("secrets/list continued a listing across a mutation"),
            ),
        ] {
            let state = Arc::new(Mutex::new(State::default()));
            let allowed = client(&state, true, mutants)?;
            let denied = client(&state, false, none)?;
            let result = secrets::conformance::verify(&allowed, &denied, "fixture").await;
            assert_eq!(result, expected, "provider mutant escaped conformance");
            if let Err(error) = result {
                assert!(!error.contains(PLANTED));
            }
        }
        // Negative control for the grant oracle: a "denied" holder that does
        // carry secrets grants must be detected as widening.
        let state = Arc::new(Mutex::new(State::default()));
        let allowed = client(&state, true, none)?;
        let widened = client(&state, true, none)?;
        assert_eq!(
            secrets::conformance::verify(&allowed, &widened, "fixture").await,
            Err("secrets existing product grants implied secrets authority")
        );
        Ok(())
    })
}

#[test]
fn secrets_listing_pages_every_row_once_and_a_mid_listing_mutation_fails_typed() -> TestResult {
    block_on(async {
        for unfenced in [false, true] {
            let state = Arc::new(Mutex::new(State::default()));
            let mutants = Mutants {
                unfenced,
                ..Mutants::default()
            };
            let allowed = client(&state, true, mutants)?;
            let mut generation = 0;
            for name in ["a", "b", "c", "d", "e"] {
                generation = allowed
                    .put(&SecretPutRequest {
                        scope: "fixture".into(),
                        name: name.into(),
                        kind: SecretKind::Protected,
                        value: PLANTED.into(),
                        expected_generation: generation,
                    })
                    .await?
                    .generation;
            }
            // A complete listing reaches every row once, in order, one
            // chunk-bounded page per call.
            let mut names = Vec::new();
            let mut cursor = None;
            loop {
                let page = allowed.list(&list_request(cursor, 2)).await?;
                assert!(page.variables.len() <= 2, "page exceeded its chunk");
                assert_eq!(page.generation, generation);
                names.extend(page.variables.iter().map(|row| row.identity().0.to_owned()));
                cursor = page.next_cursor;
                if cursor.is_none() {
                    break;
                }
            }
            assert_eq!(names, ["a", "b", "c", "d", "e"]);
            // A mutation between pages ends the listing; nothing is mixed.
            let first = allowed.list(&list_request(None, 2)).await?;
            allowed
                .delete(&SecretDeleteRequest {
                    scope: "fixture".into(),
                    name: "a".into(),
                    expected_generation: generation,
                })
                .await?;
            let continued = allowed.list(&list_request(first.next_cursor, 2)).await;
            if unfenced {
                // Negative control: the mutant provider continues across the
                // mutation and stamps a newer cursor on an older page; the
                // client refuses the mixed page rather than returning it.
                assert_eq!(continued, Err(Error::InvalidResponse));
            } else {
                assert_eq!(continued, Err(Error::ListingChanged));
            }
            let restarted = allowed.list(&list_request(None, 0)).await?;
            assert_eq!(restarted.generation, generation + 1);
            assert_eq!(restarted.variables[0].identity().0, "b");
        }
        Ok(())
    })
}

#[test]
fn secrets_conformance_rejects_successful_deletion_receipts_that_retain_rows() -> TestResult {
    block_on(async {
        let state = Arc::new(Mutex::new(State::default()));
        let allowed = client(
            &state,
            true,
            Mutants {
                retain_deleted: true,
                ..Mutants::default()
            },
        )?;
        let denied = client(&state, false, Mutants::default())?;
        assert_eq!(
            secrets::conformance::verify(&allowed, &denied, "fixture").await,
            Err("secrets/delete receipts did not match the stored scope")
        );
        let stored = state.lock().map_err(|_| "provider state poisoned")?;
        assert_eq!(stored.generation, 4, "both successful deletes advanced CAS");
        assert_eq!(stored.rows.len(), 2, "mutant retained both deleted rows");
        Ok(())
    })
}

#[test]
fn secrets_typed_grants_intersect_action_scope_name_owner_and_attenuation() -> TestResult {
    let authority = capability(true)?;
    let denied = capability(false)?;
    let key = secrets::variable_resource("fixture", "key");
    let scope = secrets::scope_resource("fixture");
    for (action, resource) in [
        (secrets::PUT, &key),
        (secrets::LIST, &scope),
        (secrets::DELETE, &key),
    ] {
        assert!(authorizes(
            &authority,
            action,
            resource,
            PolicyDecision::Allow
        ));
        assert!(!authorizes(
            &authority,
            action,
            resource,
            PolicyDecision::Deny
        ));
        assert!(!authorizes(
            &denied,
            action,
            resource,
            PolicyDecision::Allow
        ));
    }
    // There is no read action: nothing in the catalog grants one.
    assert!(!authorizes(
        &authority,
        "secrets/read",
        &key,
        PolicyDecision::Allow
    ));
    // Neighbouring scopes, including byte-prefix and encoded-slash lookalikes,
    // are outside the grant.
    for resource in [
        secrets::variable_resource("fixture-other", "key"),
        secrets::scope_resource("fixture-other"),
        secrets::variable_resource("fixture/key", "x"),
    ] {
        assert!(!authorizes(
            &authority,
            secrets::PUT,
            &resource,
            PolicyDecision::Allow
        ));
    }
    // A child narrowed to one name keeps put on it and loses everything else.
    let only = secrets::variable_resource("fixture", "only");
    let narrowed = authority.attenuate(
        vec![Caveat {
            kind: CaveatKind::Grants(GrantSet {
                grants: vec![Grant {
                    kind: GrantKind::Resource(ResourceGrant {
                        resource: ResourcePattern {
                            name: only.clone(),
                            prefix: false,
                        },
                        actions: vec![secrets::PUT.into()],
                    }),
                }],
            }),
        }],
        &[99; 32],
    )?;
    assert!(authorizes(
        &narrowed,
        secrets::PUT,
        &only,
        PolicyDecision::Allow
    ));
    for (action, resource) in [
        (secrets::PUT, &key),
        (secrets::DELETE, &only),
        (secrets::LIST, &scope),
    ] {
        assert!(!authorizes(
            &narrowed,
            action,
            resource,
            PolicyDecision::Allow
        ));
    }
    Ok(())
}

#[test]
fn secret_refs_name_one_canonical_resource_and_reject_unwritten_versions() -> TestResult {
    let reference = secrets::SecretRef::new("prod/eu".into(), "API_KEY".into(), 7)?;
    assert_eq!(reference.resource(), "secrets:/prod%2Feu/API_KEY");
    assert!(acyclic_auth::valid_resource(&reference.resource()));
    for (scope, name, version) in [("", "a", 1), ("s", "", 1), ("s", "a\n", 1), ("s", "a", 0)] {
        assert_eq!(
            secrets::SecretRef::new(scope.into(), name.into(), version),
            Err(Error::InvalidRequest)
        );
    }
    let value = secrets::SecretValue::new(PLANTED.as_bytes().to_vec());
    assert_eq!(value.expose(), PLANTED.as_bytes(), "positive control");
    assert!(!format!("{value:?} {reference:?}").contains(PLANTED));
    Ok(())
}

struct Fault {
    status: u16,
    body: Vec<u8>,
}
impl Transport for Fault {
    fn post<'a>(
        &'a self,
        _path: &'static str,
        _bearer: &'a str,
        _body: ProtectedBytes,
    ) -> TransportFuture<'a> {
        Box::pin(async move {
            Ok(Response {
                status: self.status,
                body: Zeroizing::new(self.body.clone()),
            })
        })
    }
}
#[test]
fn secrets_redacts_requests_protobuf_and_every_provider_error() -> TestResult {
    let request = SecretPutRequest {
        scope: PLANTED.into(),
        name: PLANTED.into(),
        kind: SecretKind::Protected,
        value: PLANTED.into(),
        expected_generation: 0,
    };
    let wire = SecretPutRequestProto::from(request.clone());
    assert!(
        !format!("{request:?} {wire:?}").contains(PLANTED),
        "semantic/protobuf debug exposed a value"
    );
    assert!(
        String::from_utf8_lossy(&wire.encode_to_vec()).contains(PLANTED),
        "negative control must plant an actual value"
    );
    for status in [200, 400, 401, 403, 409, 500, 302] {
        let client = Client::new(
            Fault {
                status,
                body: PLANTED.as_bytes().to_vec(),
            },
            CredentialProvider::new(|| async { Ok("fixture".into()) }),
        );
        let Err(error) = block_on(client.put(&request)) else {
            return Err("malformed provider reply admitted".into());
        };
        assert!(!format!("{error:?} {error}").contains(PLANTED));
        assert!(
            std::error::Error::source(&error).is_none(),
            "provider source chain must be erased"
        );
    }
    let client = Client::new(
        Fault {
            status: 200,
            body: Vec::new(),
        },
        CredentialProvider::new(|| async {
            Err(acyclic_auth::credential::CredentialError::Provider(
                std::io::Error::other(PLANTED).into(),
            ))
        }),
    );
    let result = block_on(client.put(&request));
    assert_eq!(result, Err(Error::Credential));
    assert!(!format!("{result:?}").contains(PLANTED));
    Ok(())
}

#[test]
fn secrets_contract_rejects_account_selectors_and_protected_value_fields() -> TestResult {
    for request in [
        serde_json::json!({"scope":"fixture","account":PLANTED}),
        serde_json::json!({"scope":"fixture","reveal":true}),
    ] {
        assert!(serde_json::from_value::<SecretListRequest>(request).is_err());
    }
    let leaked = serde_json::json!({"generation":1,"variables":[{"kind":"protected","name":"key","version":1,"value":PLANTED}]});
    assert!(serde_json::from_value::<SecretListReply>(leaked.clone()).is_err());
    assert!(
        leaked.to_string().contains(PLANTED),
        "loose JSON negative control must expose the planted value"
    );
    let valid = serde_json::json!({"generation":1,"variables":[{"kind":"protected","name":"key","version":1},
        {"kind":"plain","name":"visible","version":1,"value":"public"}],
        "nextCursor":{"generation":1,"after":"visible"}});
    let reply: SecretListReply = serde_json::from_value(valid.clone())?;
    assert_eq!(serde_json::to_value(reply.clone())?, valid);
    let wire = SecretListReplyProto::from(reply.clone()).encode_to_vec();
    assert_eq!(
        SecretListReply::try_from(SecretListReplyProto::decode(wire.as_slice())?)?,
        reply
    );
    Ok(())
}

#[test]
fn secrets_canonical_request_rejects_discarded_fields_before_dispatch() -> TestResult {
    let request = SecretListRequestProto {
        scope: "fixture".into(),
        ..Default::default()
    };
    let mut bytes = request.encode_to_vec();
    let decoded: SecretListRequest = secrets::decode_request::<SecretListRequestProto, _>(&bytes)?;
    assert_eq!(decoded.scope, "fixture");
    // Unknown wire field 15 is a planted account selector. Ordinary prost
    // decoding discards it; the shared canonical admission must refuse it.
    bytes.extend_from_slice(&[0x7a, 3, b'b', b'a', b'd']);
    assert_eq!(SecretListRequestProto::decode(bytes.as_slice())?, request);
    assert_eq!(
        secrets::decode_request::<SecretListRequestProto, SecretListRequest>(&bytes),
        Err(Error::InvalidRequest)
    );
    Ok(())
}

#[cfg(feature = "client")]
mod http {
    use super::*;
    use axum::{
        Router,
        body::Bytes,
        extract::State as HttpState,
        http::{HeaderMap, StatusCode, Uri},
        routing::post,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct LogWriter(Arc<Mutex<Vec<u8>>>);
    impl std::io::Write for LogWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0
                .lock()
                .map_err(|_| std::io::Error::other("log capture poisoned"))?
                .extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    struct Server {
        allowed: Provider,
        denied: Provider,
        fault: AtomicUsize,
        calls: AtomicUsize,
    }
    async fn serve(
        HttpState(server): HttpState<Arc<Server>>,
        uri: Uri,
        headers: HeaderMap,
        body: Bytes,
    ) -> (StatusCode, HeaderMap, Vec<u8>) {
        server.calls.fetch_add(1, Ordering::SeqCst);
        let mut extra = HeaderMap::new();
        let response = if server.fault.load(Ordering::SeqCst) == 1 {
            extra.insert(
                "location",
                axum::http::HeaderValue::from_static("/redirect-target"),
            );
            response(302, PLANTED)
        } else if server.fault.load(Ordering::SeqCst) == 2 {
            response(500, PLANTED)
        } else if headers
            .get("content-type")
            .is_none_or(|value| value != "application/json")
        {
            response(400, PLANTED)
        } else {
            let provider = if headers
                .get("authorization")
                .is_some_and(|value| value == "Bearer allowed-fixture")
            {
                &server.allowed
            } else {
                &server.denied
            };
            provider.call(uri.path(), &body)
        };
        match response {
            Ok(response) => (
                StatusCode::from_u16(response.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                extra,
                response.body.to_vec(),
            ),
            Err(_) => (StatusCode::BAD_REQUEST, extra, Vec::new()),
        }
    }

    #[tokio::test]
    async fn secrets_native_http_runs_conformance_and_redacts_errors_without_following_redirects()
    -> TestResult {
        use futures::FutureExt;

        // Exercise the native response constructor used by HttpTransport::post,
        // including reqwest body collection. The complete in-memory native body
        // must finish on its first poll; an unexpected wait fails immediately.
        // Sharing backing requires a copy that leaves the other owner intact.
        for length in [1024, 1024 * 1024] {
            let original = Bytes::from(vec![7; length]);
            let pointer = original.as_ptr();
            let response = reqwest::Response::from(axum::http::Response::new(original));
            let receive = Response::from_http(response);
            let mut protected = None;
            let transfer = acyclic_test_support::measure(|| {
                protected = receive.now_or_never();
            });
            let protected = protected.ok_or("complete native response did not finish")??;
            assert_eq!(protected.status, 200);
            assert_eq!(protected.body.as_ptr(), pointer);
            assert_eq!(protected.body.len(), length);
            assert_eq!((transfer.count_total, transfer.bytes_total), (0, 0));
            drop(protected);

            // The former response handoff, on the same native response type,
            // must fail the zero-allocation oracle even with unique backing.
            let response =
                reqwest::Response::from(axum::http::Response::new(Bytes::from(vec![7; length])));
            let mut copied = None;
            let legacy = acyclic_test_support::measure(|| {
                copied = response
                    .bytes()
                    .now_or_never()
                    .map(|result| result.map(|bytes| Zeroizing::new(bytes.to_vec())));
            });
            assert_eq!(
                copied
                    .ok_or("complete native copy control did not finish")??
                    .len(),
                length
            );
            assert!(legacy.bytes_total >= length as u64);

            let original = Bytes::from(vec![7; length]);
            let shared = original.clone();
            let response = reqwest::Response::from(axum::http::Response::new(shared));
            let receive = Response::from_http(response);
            let mut protected = None;
            let copy = acyclic_test_support::measure(|| {
                protected = receive.now_or_never();
            });
            let protected = protected.ok_or("shared native response did not finish")??;
            assert_ne!(protected.body.as_ptr(), original.as_ptr());
            assert_eq!(protected.body.as_slice(), original.as_ref());
            assert!(copy.bytes_total >= length as u64);
            drop(protected);
            assert!(original.iter().all(|byte| *byte == 7));
        }

        // The original bearer allocation survives the actual HTTP and gRPC
        // metadata constructors and clones. The old Vec route is a full-copy
        // negative control, measured independently of encoding the credential.
        for length in [1024, acyclic_auth::credential::MAX_CREDENTIAL_BYTES] {
            let token = "a".repeat(length);
            let encoded = acyclic_auth::credential::bearer_bytes(&token);
            let pointer = encoded.as_ptr();
            let mut header = None;
            let transfer = acyclic_test_support::measure(|| {
                header = Some(reqwest::header::HeaderValue::from_maybe_shared(
                    encoded.clone(),
                ));
            });
            let mut header = header.ok_or("header transfer missing")??;
            header.set_sensitive(true);
            assert_eq!(header.as_bytes().as_ptr(), pointer);
            let retained = header.clone();
            drop(header);
            assert!(retained.is_sensitive());
            assert_eq!(retained.as_bytes().as_ptr(), pointer);
            assert_eq!(transfer.count_total, 0, "HTTP bearer handoff allocated");
            let copy = acyclic_test_support::measure(|| {
                let _ = std::hint::black_box(reqwest::header::HeaderValue::from_bytes(
                    retained.as_bytes(),
                ));
            });
            assert!(copy.bytes_total >= encoded.len() as u64);
            #[cfg(feature = "grpc")]
            {
                let mut metadata = None;
                let transfer = acyclic_test_support::measure(|| {
                    metadata = Some(
                        tonic::metadata::MetadataValue::<tonic::metadata::Ascii>::try_from(
                            encoded.clone(),
                        ),
                    );
                });
                let mut metadata = metadata.ok_or("metadata transfer missing")??;
                metadata.set_sensitive(true);
                let retained = metadata.clone();
                drop(metadata);
                assert!(retained.is_sensitive());
                assert_eq!(retained.as_encoded_bytes().as_ptr(), pointer);
                assert_eq!(transfer.count_total, 0, "gRPC bearer handoff allocated");
                let copied = retained.as_encoded_bytes().to_vec();
                let copy = acyclic_test_support::measure(|| {
                    let _ = std::hint::black_box(tonic::metadata::MetadataValue::<
                        tonic::metadata::Ascii,
                    >::try_from(copied));
                });
                assert!(copy.bytes_total >= encoded.len() as u64);
            }
            drop(encoded);
            assert_eq!(retained.as_bytes().as_ptr(), pointer);
            assert_eq!(retained.as_bytes().len(), length + b"Bearer ".len());
        }
        // The shared native body must retain the encoded allocation through
        // transport clones and slices. Its control cost is independent of the
        // payload; an ordinary full copy is the allocation negative control.
        let mut controls = None;
        for length in [1024, 1024 * 1024] {
            let encoded = Zeroizing::new(vec![7; length]);
            let pointer = encoded.as_ptr();
            let mut owned = None;
            let cost = acyclic_test_support::measure(|| {
                owned = Some(ProtectedBytes::new(encoded).into_bytes());
            });
            let original = owned.ok_or("native body missing")?;
            assert_eq!(original.as_ptr(), pointer);
            let retained = original.slice(1..);
            drop(original);
            let request = reqwest::Body::from(retained);
            assert_eq!(
                request
                    .as_bytes()
                    .ok_or("native body is not owned")?
                    .as_ptr(),
                pointer.wrapping_add(1),
            );
            assert_eq!(request.as_bytes().map(<[u8]>::len), Some(length - 1));
            let current = (cost.count_total, cost.bytes_total);
            if let Some(controls) = controls {
                assert_eq!(current, controls, "body transfer copied payload bytes");
            }
            controls = Some(current);
            let copied = acyclic_test_support::measure(|| {
                std::hint::black_box(request.as_bytes().unwrap().to_vec());
            });
            assert!(copied.bytes_total >= (length - 1) as u64);
            assert!(cost.bytes_total < copied.bytes_total);
            eprintln!(
                "native body payload={length} transfer_allocations={} transfer_bytes={} copy_allocations={} copy_bytes={}",
                cost.count_total, cost.bytes_total, copied.count_total, copied.bytes_total,
            );
        }
        let logs = Arc::new(Mutex::new(Vec::new()));
        let capture = logs.clone();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_max_level(tracing::Level::TRACE)
            .with_writer(move || LogWriter(capture.clone()))
            .finish();
        let _subscriber = tracing::subscriber::set_default(subscriber);
        tracing::error!(value = PLANTED, "unredacted logging negative control");
        {
            let mut captured = logs.lock().map_err(|_| "log capture poisoned")?;
            assert!(
                String::from_utf8_lossy(&captured).contains(PLANTED),
                "logging control did not emit its value"
            );
            captured.clear();
        }
        let state = Arc::new(Mutex::new(State::default()));
        let server = Arc::new(Server {
            allowed: Provider {
                state: state.clone(),
                authority: capability(true)?,
                mutants: Mutants::default(),
            },
            denied: Provider {
                state,
                authority: capability(false)?,
                mutants: Mutants::default(),
            },
            fault: AtomicUsize::new(0),
            calls: AtomicUsize::new(0),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let origin = format!("http://{}", listener.local_addr()?);
        let router = Router::new()
            .route("/v1/identity/secrets/{operation}", post(serve))
            .with_state(server.clone());
        let task = tokio::spawn(async move { axum::serve(listener, router).await });
        let resolutions = Arc::new(AtomicUsize::new(0));
        let counter = resolutions.clone();
        let allowed = Client::new(
            secrets::HttpTransport::new(&origin)?,
            CredentialProvider::new(move || {
                counter.fetch_add(1, Ordering::SeqCst);
                async { Ok("allowed-fixture".into()) }
            }),
        );
        let denied = Client::new(
            secrets::HttpTransport::new(&origin)?,
            CredentialProvider::new(|| async { Ok("denied-fixture".into()) }),
        );
        let result = secrets::conformance::verify(&allowed, &denied, "fixture").await;
        assert_eq!(result, Ok(()));
        assert_eq!(
            resolutions.load(Ordering::SeqCst) + 3,
            server.calls.load(Ordering::SeqCst),
            "every physical request needs fresh credentials"
        );
        for (fault, expected) in [(1, Error::Http(302)), (2, Error::Http(500))] {
            server.fault.store(fault, Ordering::SeqCst);
            let calls = server.calls.load(Ordering::SeqCst);
            let result = allowed.list(&list_request(None, 0)).await;
            assert_eq!(result, Err(expected));
            assert!(!format!("{result:?}").contains(PLANTED));
            assert_eq!(
                server.calls.load(Ordering::SeqCst),
                calls + 1,
                "redirect or retry dispatched a second request"
            );
        }
        task.abort();
        let captured = logs.lock().map_err(|_| "log capture poisoned")?;
        let captured = String::from_utf8_lossy(&captured);
        for value in [
            PLANTED,
            "conformance-protected-value-never-return",
            "allowed-fixture",
            "denied-fixture",
        ] {
            assert!(
                !captured.contains(value),
                "HTTP diagnostics emitted protected input or credentials"
            );
        }
        Ok(())
    }
}

#[test]
fn secrets_published_vectors_match_shared_request_and_reply_admission() -> TestResult {
    let suite: serde_json::Value = serde_json::from_slice(secrets::conformance::SUITE)?;
    let vectors = suite
        .get("vectors")
        .and_then(serde_json::Value::as_array)
        .ok_or("vectors missing")?;
    for vector in vectors {
        let request = vector.get("request").ok_or("request missing")?.clone();
        if vector.get("reject").and_then(serde_json::Value::as_str) == Some("invalid_request") {
            assert!(serde_json::from_value::<SecretListRequest>(request).is_err());
            continue;
        }
        let client = Client::new(
            Fault {
                status: 200,
                body: serde_json::to_vec(vector.get("response").ok_or("response missing")?)?,
            },
            CredentialProvider::new(|| async { Ok("fixture".into()) }),
        );
        let result = match vector.get("operation").and_then(serde_json::Value::as_str) {
            Some("put") => block_on(client.put(&serde_json::from_value(request)?)).map(drop),
            Some("delete") => block_on(client.delete(&serde_json::from_value(request)?)).map(drop),
            Some("list") => block_on(client.list(&serde_json::from_value(request)?)).map(drop),
            _ => return Err("unknown vector operation".into()),
        };
        match vector.get("reject").and_then(serde_json::Value::as_str) {
            Some(code) => assert_eq!(result.map_err(Error::code), Err(code)),
            None => result?,
        }
    }
    Ok(())
}

#[test]
fn secrets_and_identity_share_endpoint_admission_before_credentials() {
    for (origin, allowed) in [
        ("https://example.com", true),
        ("http://127.0.0.1:1234", true),
        ("http://[::1]:1234", true),
        ("http://localhost:1234", true),
        ("http://example.com", false),
        ("https://user:password@example.com", false),
        ("https://example.com/path", false),
        ("https://example.com?bearer=planted", false),
        ("https://example.com#planted", false),
    ] {
        assert!(
            url::Url::parse(origin).is_ok(),
            "negative control must admit the raw URL"
        );
        assert_eq!(acyclic_auth::credential::endpoint(origin).is_ok(), allowed);
    }
}
