//! Socket contract ingress and schema regression tests.
use acyclic_contract_derive::{message, oneof};
use prost::Message;

#[derive(Debug, Default, PartialEq)]
enum Invalid {
    #[default]
    Missing,
}

impl From<std::convert::Infallible> for Invalid {
    fn from(value: std::convert::Infallible) -> Self {
        match value {}
    }
}

#[message(error = Invalid, file = ShapesFile)]
#[derive(Debug, PartialEq)]
struct Shapes {
    #[wire(tag = 1, string)]
    names: Vec<String>,
    #[wire(tag = 2, uint32)]
    ports: Vec<u32>,
    #[wire(tag = 3, double)]
    jitter: f64,
    #[wire(tag = 4)]
    bytes: Vec<u8>,
    #[wire(oneof = "5")]
    event: Event,
}

#[oneof(error = Invalid, file = ShapesFile)]
#[derive(Debug, PartialEq)]
enum Event {
    #[wire(tag = 5, string)]
    Error(String),
}

#[acyclic_contract_derive::file(family = "shapes", messages(ShapesProto))]
struct ShapesFile;

#[test]
fn repeated_scalars_double_bytes_and_error_case_preserve_wire_shape()
-> Result<(), prost::DecodeError> {
    let wire = ShapesProto {
        names: vec!["a".into(), "b".into()],
        ports: vec![443, 8443],
        jitter: 0.25,
        bytes: vec![0, 255],
        event: Some(EventProto::Error("closed".into())),
    };
    let decoded = ShapesProto::decode(wire.encode_to_vec().as_slice())?;
    assert_eq!(
        Shapes::try_from(decoded),
        Ok(Shapes {
            names: vec!["a".into(), "b".into()],
            ports: vec![443, 8443],
            jitter: 0.25,
            bytes: vec![0, 255],
            event: Event::Error("closed".into()),
        })
    );
    let schema = ShapesProto::schema();
    assert!(schema.contains("repeated string names = 1;"));
    assert!(schema.contains("repeated uint32 ports = 2;"));
    assert!(schema.contains("double jitter = 3;"));
    assert!(schema.contains("bytes bytes = 4;"));
    assert!(schema.contains("string error = 5;"));
    Ok(())
}
