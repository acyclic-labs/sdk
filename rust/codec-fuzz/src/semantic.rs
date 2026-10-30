//! Service admission uses the production conversions after either wire syntax.
use prost::Message;
use std::fmt::Debug;

pub(super) fn project<W, S, E>(
    bytes: &[u8],
    decode: impl Fn(W) -> Result<S, E>,
    encode: impl Fn(S) -> W,
) where
    W: Message + Default,
    S: Clone + Debug + PartialEq,
    E: Debug,
{
    if let Ok(wire) = W::decode(bytes)
        && let Ok(value) = decode(wire)
    {
        let encoded = encode(value.clone()).encode_to_vec();
        assert_eq!(
            decode(W::decode(encoded.as_slice()).expect("canonical wire decodes"))
                .expect("canonical semantic value is admitted"),
            value,
        );
    }
}

fn admit<W: Message + Default, T>(bytes: &[u8], decode: impl FnOnce(W) -> T) {
    if let Ok(wire) = W::decode(bytes) {
        let _ = decode(wire);
    }
}

fn inference<W: Message + Default>(bytes: &[u8], kind: &str, identity: impl FnOnce(&W) -> &[u8]) {
    use acyclic_inference::validate_customer_wire;
    let Ok(wire) = W::decode(bytes) else {
        return;
    };
    let expected = identity(&wire);
    if validate_customer_wire(kind, bytes, expected, &[]).is_ok() {
        assert!(validate_customer_wire(kind, &wire.encode_to_vec(), expected, &[]).is_ok());
        // A view is never transferable to a different caller-selected identity.
        if !expected.is_empty() {
            let mut foreign = expected.to_vec();
            foreign[0] ^= 1;
            assert!(validate_customer_wire(kind, bytes, &foreign, &[]).is_err());
        }
    }
}

pub(super) fn check(name: &str, bytes: &[u8]) {
    use acyclic_stream::{wire as w, wire_codec as c};
    macro_rules! projection {
        ($wire:ident, $decode:expr, $encode:expr) => {
            project::<w::$wire, _, _>(bytes, $decode, $encode)
        };
    }
    macro_rules! admission {
        ($wire:ty, $decode:expr) => {
            admit::<$wire, _>(bytes, $decode)
        };
    }
    match name {
        "acyclic.stream.v1.AppendRequest" => admission!(w::AppendRequest, c::append_from_wire),
        "acyclic.stream.v1.ForkRequest" => admission!(w::ForkRequest, c::fork_from_wire),
        "acyclic.stream.v1.FollowRequest" => admission!(w::FollowRequest, c::follow_from_wire),
        "acyclic.stream.v1.HeadsRequest" => {
            projection!(HeadsRequest, c::heads_from_wire, c::heads_request_to_wire)
        }
        "acyclic.stream.v1.ChildrenPageRequest" => projection!(
            ChildrenPageRequest,
            c::children_page_from_wire,
            c::children_page_request_to_wire
        ),
        "acyclic.stream.v1.ChildrenPageResponse" => projection!(
            ChildrenPageResponse,
            c::children_page_from_response,
            c::children_page_to_wire
        ),
        "acyclic.stream.v1.CommitRequest" => {
            projection!(CommitRequest, c::commit_from_wire, |value| {
                c::commit_to_wire(&value)
            })
        }
        "acyclic.stream.v1.CommitReceipt" => {
            projection!(CommitReceipt, c::receipt_from_wire, c::receipt_to_wire)
        }
        "acyclic.stream.v1.ForkReceipt" => {
            projection!(ForkReceipt, c::fork_receipt_from_wire, |value| {
                c::fork_receipt_to_wire(&value)
            })
        }
        "acyclic.stream.v1.PathHead" => {
            projection!(PathHead, c::head_from_wire, c::head_to_wire)
        }
        "acyclic.stream.v1.ForkOrigin" => {
            projection!(ForkOrigin, c::fork_origin_from_wire, c::fork_origin_to_wire)
        }
        "acyclic.stream.v1.Revision" => {
            projection!(Revision, c::revision_from_wire, |value| {
                c::revision_to_wire(&value)
            })
        }
        "acyclic.stream.v1.AppendRecord" => {
            projection!(AppendRecord, c::append_record, c::append_record_wire)
        }
        "acyclic.stream.v1.Record" => {
            // Origin elision is meaningful only with the original followed path.
            // Exercise explicit origin and both matching/foreign contexts using
            // the production codec; never reconstruct an origin in this harness.
            for followed in [
                None,
                Some(acyclic_stream::StreamPath::new("codec/events").unwrap()),
                Some(acyclic_stream::StreamPath::new("codec/foreign").unwrap()),
            ] {
                projection!(Record, |wire| c::record(wire, followed.as_ref()), |value| {
                    c::record_wire(value, followed.as_ref())
                });
            }
        }
        "acyclic.stream.v1.HeadsResponse" => {
            admission!(w::HeadsResponse, c::heads_from_response)
        }
        "acyclic.stream.v1.TailResponse" => {
            admission!(w::TailResponse, c::watermarks_from_wire)
        }
        "acyclic.stream.v1.ReadKeySeek" => {
            project::<w::ReadKeySeek, _, _>(
                bytes,
                |value| c::read_key_from_wire(Some(value)),
                |value| c::read_key_to_wire(&value).expect("present seek remains present"),
            );
        }
        "acyclic.stream.v1.ReadResponse" => {
            admission!(w::ReadResponse, c::read_response_batch);
            admission!(w::ReadResponse, c::read_response_records);
        }
        "acyclic.objects.v1.AvailabilityCursor" => {
            super::check_semantic::<
                acyclic_objects::wire::AvailabilityCursor,
                acyclic_objects::AvailabilityCursor,
            >(bytes);
        }
        "acyclic.actors.v1.DeploymentRequest" => {
            admission!(acyclic_actors::wire::DeploymentRequest, |wire| {
                acyclic_actors::contract::validate_deployment(&wire)
            });
        }
        "acyclic.actors.v1.UpdateNamespaceRequest" => {
            admission!(acyclic_actors::wire::UpdateNamespaceRequest, |wire| {
                acyclic_actors::contract::validate_update(&wire)
            });
        }
        "acyclic.actors.v1.DeleteAllRequest" => {
            admission!(acyclic_actors::wire::DeleteAllRequest, |wire| {
                acyclic_actors::contract::validate_delete(&wire)
            });
        }
        "acyclic.actors.v1.AddSubscriptionRequest" => {
            admission!(acyclic_actors::wire::AddSubscriptionRequest, |wire| {
                acyclic_actors::contract::validate_add_subscription(&wire)
            });
        }
        "inference.customer.v1.RetainWarmRequest" => {
            inference::<acyclic_inference::wire::RetainWarmRequest>(
                bytes,
                "retain_warm_request",
                |_| &[],
            );
        }
        "inference.customer.v1.RenewWarmRequest" => {
            inference::<acyclic_inference::wire::RenewWarmRequest>(
                bytes,
                "renew_warm_request",
                |_| &[],
            );
        }
        "inference.customer.v1.MutationReceipt" => {
            inference::<acyclic_inference::wire::MutationReceipt>(
                bytes,
                "mutation_receipt",
                |_| &[],
            );
        }
        "inference.customer.v1.ContextView" => {
            inference::<acyclic_inference::wire::ContextView>(bytes, "context_view", |wire| {
                &wire.revision
            });
        }
        "inference.customer.v1.WarmView" => {
            inference::<acyclic_inference::wire::WarmView>(bytes, "warm_view", |_| &[]);
            inference::<acyclic_inference::wire::WarmView>(bytes, "warm_commitment", |wire| {
                &wire.commitment
            });
        }
        "inference.customer.v1.RunView" => {
            inference::<acyclic_inference::wire::RunView>(bytes, "run_view", |wire| &wire.run_id);
        }
        "inference.customer.v1.EvaluationSpec" => {
            inference::<acyclic_inference::wire::EvaluationSpec>(bytes, "evaluation_spec", |_| &[]);
        }
        "inference.customer.v1.EvaluationView" => {
            inference::<acyclic_inference::wire::EvaluationView>(
                bytes,
                "evaluation_view",
                |wire| &wire.evaluation_id,
            );
        }
        "inference.customer.v1.RunEvent" => {
            inference::<acyclic_inference::wire::RunEvent>(bytes, "run_event", |_| &[]);
        }
        _ => {}
    }
}

/// Reuse actual Stream writer output, including a contextual origin spelling.
pub(super) fn seeds(
    streams: &[(String, Vec<u8>)],
) -> Result<Vec<(&'static str, &'static str, Vec<u8>)>, acyclic_stream::StreamError> {
    use acyclic_stream::{StreamPath, wire, wire_codec};
    let mut seeds = Vec::new();
    for (name, bytes) in streams {
        let message = match name.as_str() {
            "changes-append-record" => "acyclic.stream.v1.AppendRecord",
            "changes-record" => "acyclic.stream.v1.Record",
            "record-batch" => "acyclic.stream.v1.ReadResponse",
            _ => continue,
        };
        if name == "changes-record" {
            let original = StreamPath::new("codec/events")?;
            let record = wire_codec::record(
                wire::Record::decode(bytes.as_slice()).expect("real writer output decodes"),
                None,
            )?;
            seeds.push((
                message,
                "origin-elided",
                wire_codec::record_wire(record, Some(&original)).encode_to_vec(),
            ));
        }
        seeds.push((message, "semantic", bytes.clone()));
    }
    Ok(seeds)
}
