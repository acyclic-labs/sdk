//! Real wire decoding must pass semantic admission before constructing a domain value.
use acyclic_contract_derive::message;
use prost::{Message, Name};

#[derive(Debug, Default, PartialEq)]
enum Admission {
    #[default]
    EmptyIdentity,
    Unknown(i32),
    Oversized,
}

fn nonempty(value: String) -> Result<String, Admission> {
    if value.is_empty() {
        Err(Admission::EmptyIdentity)
    } else {
        Ok(value)
    }
}

#[message(error = Admission, package = "acyclic.test.v1", post = Record::admit)]
#[derive(Debug, PartialEq)]
struct Record {
    #[wire(tag = 1, from = nonempty)]
    identity: String,
    #[wire(tag = 2)]
    cursor: u64,
    #[wire(tag = 3)]
    payload: Vec<u8>,
}
impl Record {
    fn admit(&self) -> Result<(), Admission> {
        if self.payload.len() > 2 {
            Err(Admission::Oversized)
        } else {
            Ok(())
        }
    }
}

#[test]
fn wire_decode_cannot_bypass_semantic_admission() -> Result<(), prost::DecodeError> {
    let encoded = RecordProto {
        identity: String::new(),
        cursor: 0,
        payload: vec![],
    }
    .encode_to_vec();
    assert_eq!(
        Record::try_from(RecordProto::decode(encoded.as_slice())?),
        Err(Admission::EmptyIdentity)
    );
    let encoded = RecordProto {
        identity: "x".into(),
        cursor: 0,
        payload: vec![0; 3],
    }
    .encode_to_vec();
    assert_eq!(
        Record::try_from(RecordProto::decode(encoded.as_slice())?),
        Err(Admission::Oversized)
    );
    Ok(())
}

#[test]
fn exact_wire_and_schema_come_from_semantic_fields() -> Result<(), prost::DecodeError> {
    let expected = Record {
        identity: "x".into(),
        cursor: u64::MAX,
        payload: vec![0, 255],
    };
    let wire: RecordProto = expected.into();
    let bytes = wire.encode_to_vec();
    assert_eq!(
        bytes,
        vec![
            10, 1, b'x', 16, 255, 255, 255, 255, 255, 255, 255, 255, 255, 1, 26, 2, 0, 255
        ]
    );
    assert_eq!(
        Record::try_from(RecordProto::decode(bytes.as_slice())?),
        Ok(Record {
            identity: "x".into(),
            cursor: u64::MAX,
            payload: vec![0, 255]
        })
    );
    assert_eq!(RecordProto::full_name(), "acyclic.test.v1.Record");
    assert_eq!(
        RecordProto::schema(),
        "message Record {\n  string identity = 1;\n  uint64 cursor = 2;\n  bytes payload = 3;\n}\n"
    );
    Ok(())
}

impl From<std::convert::Infallible> for Admission {
    fn from(value: std::convert::Infallible) -> Self {
        match value {}
    }
}

#[message(error = Admission, package = "acyclic.test.v1")]
#[derive(Debug, PartialEq)]
struct Envelope {
    #[wire(tag = 1, message)]
    required: Record,
    #[wire(tag = 2, message)]
    optional: Option<Record>,
    #[wire(tag = 3, message)]
    repeated: Vec<Record>,
    #[wire(tag = 4)]
    revision: Option<u64>,
}

#[test]
fn nested_required_optional_and_repeated_ingress_share_admission() -> Result<(), prost::DecodeError>
{
    fn child(identity: &str) -> RecordProto {
        RecordProto {
            identity: identity.into(),
            cursor: 0,
            payload: vec![],
        }
    }
    for raw in [
        EnvelopeProto {
            required: None,
            optional: None,
            repeated: vec![],
            revision: None,
        },
        EnvelopeProto {
            required: Some(child("")),
            optional: None,
            repeated: vec![],
            revision: None,
        },
        EnvelopeProto {
            required: Some(child("x")),
            optional: Some(child("")),
            repeated: vec![],
            revision: None,
        },
        EnvelopeProto {
            required: Some(child("x")),
            optional: None,
            repeated: vec![child("")],
            revision: None,
        },
    ] {
        let bytes = raw.encode_to_vec();
        assert_eq!(
            Envelope::try_from(EnvelopeProto::decode(bytes.as_slice())?),
            Err(Admission::EmptyIdentity)
        );
    }
    for revision in [None, Some(0), Some(u64::MAX)] {
        let raw = EnvelopeProto {
            required: Some(child("x")),
            optional: Some(child("y")),
            repeated: vec![child("z")],
            revision,
        };
        let bytes = raw.encode_to_vec();
        let admitted = Envelope::try_from(EnvelopeProto::decode(bytes.as_slice())?);
        assert!(admitted.is_ok());
        if let Ok(value) = admitted {
            assert_eq!(value.revision, revision);
            let restored: EnvelopeProto = value.into();
            assert_eq!(restored.encode_to_vec(), bytes);
        }
    }
    assert_eq!(
        EnvelopeProto::schema(),
        "message Envelope {\n  Record required = 1;\n  Record optional = 2;\n  repeated Record repeated = 3;\n  optional uint64 revision = 4;\n}\n"
    );
    Ok(())
}

#[derive(Debug, PartialEq)]
struct CurrentHead;
impl TryFrom<bool> for CurrentHead {
    type Error = Admission;
    fn try_from(value: bool) -> Result<Self, Admission> {
        if value {
            Ok(Self)
        } else {
            Err(Admission::EmptyIdentity)
        }
    }
}
impl From<CurrentHead> for bool {
    fn from(_: CurrentHead) -> Self {
        true
    }
}

#[acyclic_contract_derive::oneof(error = Admission)]
#[derive(Debug, PartialEq)]
enum Choice {
    #[wire(tag = 1)]
    Cursor(u64),
    #[wire(tag = 2, bool)]
    CurrentHead(CurrentHead),
    #[wire(tag = 3)]
    InlineBytes(Vec<u8>),
    #[wire(tag = 4, message)]
    Record(Record),
}

#[message(error = Admission, package = "acyclic.test.v1")]
#[derive(Debug, PartialEq)]
struct Selector {
    #[wire(oneof = "1,2,3,4")]
    source: Choice,
}

#[test]
fn oneof_keeps_presence_and_rejects_unrepresentable_payloads() -> Result<(), prost::DecodeError> {
    for source in [
        None,
        Some(ChoiceProto::CurrentHead(false)),
        Some(ChoiceProto::Record(RecordProto {
            identity: "".into(),
            cursor: 0,
            payload: vec![],
        })),
    ] {
        let raw = SelectorProto { source };
        let bytes = raw.encode_to_vec();
        assert_eq!(
            Selector::try_from(SelectorProto::decode(bytes.as_slice())?),
            Err(Admission::EmptyIdentity)
        );
    }
    for (source, expected) in [
        (ChoiceProto::Cursor(0), vec![8, 0]),
        (ChoiceProto::CurrentHead(true), vec![16, 1]),
        (ChoiceProto::InlineBytes(vec![]), vec![26, 0]),
    ] {
        let raw = SelectorProto {
            source: Some(source),
        };
        assert_eq!(raw.encode_to_vec(), expected);
        let admitted = Selector::try_from(SelectorProto::decode(expected.as_slice())?);
        assert!(admitted.is_ok());
        if let Ok(value) = admitted {
            let wire: SelectorProto = value.into();
            assert_eq!(wire.encode_to_vec(), expected);
        }
    }
    assert_eq!(
        SelectorProto::schema(),
        "message Selector {\n  oneof source {\n    uint64 cursor = 1;\n    bool current_head = 2;\n    bytes inline_bytes = 3;\n    Record record = 4;\n  }\n}\n"
    );
    Ok(())
}

#[acyclic_contract_derive::enumeration(error = Admission, unknown = Admission::Unknown)]
enum State {
    Unspecified = 0,
    Ready = 1,
}

#[message(error = Admission, package = "acyclic.test.v1")]
#[derive(Debug, PartialEq)]
struct StateView {
    #[wire(tag = 1, enumeration = State)]
    state: State,
}

#[test]
fn unknown_enums_remain_raw_until_explicit_fallible_ingress() -> Result<(), prost::DecodeError> {
    for state in [i32::MIN, -1, 2, i32::MAX] {
        let bytes = StateViewProto { state }.encode_to_vec();
        let wire = StateViewProto::decode(bytes.as_slice())?;
        assert_eq!(wire.state, state);
        assert_eq!(StateView::try_from(wire), Err(Admission::Unknown(state)));
    }
    for (state, expected) in [(State::Unspecified, vec![]), (State::Ready, vec![8, 1])] {
        let wire: StateViewProto = StateView { state }.into();
        assert_eq!(wire.encode_to_vec(), expected);
        assert_eq!(
            StateView::try_from(StateViewProto::decode(expected.as_slice())?),
            Ok(StateView { state })
        );
    }
    assert_eq!(
        State::schema(),
        "enum State {\n  STATE_UNSPECIFIED = 0;\n  STATE_READY = 1;\n}\n"
    );
    assert_eq!(
        StateViewProto::schema(),
        "message StateView {\n  State state = 1;\n}\n"
    );
    Ok(())
}

#[acyclic_contract_derive::service]
enum SessionService {
    Unary {
        request: RecordProto,
        response: StateViewProto,
    },
    #[wire(client_streaming)]
    Upload {
        request: RecordProto,
        response: StateViewProto,
    },
    #[wire(server_streaming)]
    Follow {
        request: RecordProto,
        response: StateViewProto,
    },
    #[wire(client_streaming, server_streaming)]
    Exchange {
        request: RecordProto,
        response: StateViewProto,
    },
}

#[acyclic_contract_derive::file(
    family = "test",
    messages(SelectorProto, StateViewProto, RecordProto, EnvelopeProto),
    enums(State),
    services(SessionService)
)]
struct TestFile;

#[test]
fn real_protoc_accepts_compiler_linked_schema_and_all_stream_shapes()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    TestFile::render(root.path())?;
    let proto = root.path().join("test/v1/test.proto");
    let first = std::fs::read(&proto)?;
    TestFile::render(root.path())?;
    assert_eq!(std::fs::read(&proto)?, first);
    let output = root.path().join("test.bin");
    let result = std::process::Command::new(protoc_bin_vendored::protoc_bin_path()?)
        .arg(format!("--proto_path={}", root.path().display()))
        .arg(format!("--descriptor_set_out={}", output.display()))
        .arg(&proto)
        .output()?;
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let bytes = std::fs::read(output)?;
    let set = prost_types::FileDescriptorSet::decode(bytes.as_slice())?;
    assert_eq!(set.file.len(), 1);
    let file = set.file.first().ok_or("missing file descriptor")?;
    assert_eq!(file.package.as_deref(), Some(TestFile::PACKAGE));
    assert_eq!(
        file.message_type
            .iter()
            .map(|m| m.name.as_deref())
            .collect::<Vec<_>>(),
        vec![
            Some("Envelope"),
            Some("Record"),
            Some("Selector"),
            Some("StateView")
        ]
    );
    let service = file.service.first().ok_or("missing service descriptor")?;
    assert_eq!(service.name.as_deref(), Some(SessionService::PROTO_NAME));
    assert_eq!(
        service
            .method
            .iter()
            .map(|m| (
                m.client_streaming.unwrap_or(false),
                m.server_streaming.unwrap_or(false)
            ))
            .collect::<Vec<_>>(),
        vec![(false, false), (true, false), (false, true), (true, true)]
    );
    let selector = file
        .message_type
        .iter()
        .find(|m| m.name.as_deref() == Some("Selector"))
        .ok_or("missing Selector")?;
    assert_eq!(selector.oneof_decl.len(), 1);
    assert_eq!(
        selector.field.iter().map(|f| f.number).collect::<Vec<_>>(),
        vec![Some(1), Some(2), Some(3), Some(4)]
    );
    Ok(())
}
