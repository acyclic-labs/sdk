//! Exercise the shared full-frame codec, independently of either log's policy.
use acyclic_native_runtime::LogFrame;

const DOMAIN: &[u8] = b"codec-frame-test-v1\0";

fn write(format: LogFrame, body: &[u8], output: &mut Vec<u8>) -> std::io::Result<()> {
    let split = body.len() / 2;
    let start = output.len();
    format.encode(&[&body[..split], &body[split..]], output)?;
    let encoded = &output[start..];
    assert!(
        format.is_valid(encoded),
        "shared frame writer output must be admitted"
    );
    assert_eq!(
        format.read(encoded, encoded.len() as u64)?.as_deref(),
        Some(body),
        "shared frame reader must retain original writer body"
    );
    Ok(())
}

pub(super) fn check(bytes: &[u8]) {
    let mut writer = Vec::new();
    for wide in [false, true] {
        writer.clear();
        let written = write(
            LogFrame {
                domain: DOMAIN,
                wide,
                minimum_body: 0,
                maximum_body: u64::MAX,
            },
            bytes,
            &mut writer,
        );
        if !wide && u32::try_from(bytes.len()).is_err() {
            assert_eq!(
                written.map_err(|error| error.kind()),
                Err(std::io::ErrorKind::InvalidInput)
            );
        } else {
            written.unwrap();
        }
    }
    // The first sixteen bytes are caller configuration, not a production
    // frame parser. Every body/prefix/checksum is admitted by LogFrame itself.
    let Some((config, frame)) = bytes.split_at_checked(16) else {
        return;
    };
    let minimum_body = u64::from_le_bytes(config[..8].try_into().unwrap());
    let maximum_body = u64::from_le_bytes(config[8..].try_into().unwrap());
    for wide in [false, true] {
        let format = LogFrame {
            domain: DOMAIN,
            wide,
            minimum_body,
            maximum_body,
        };
        let valid = format.is_valid(frame);
        let decoded = format.read(frame, frame.len() as u64);
        let body = decoded.ok().flatten();
        assert_eq!(
            valid,
            body.is_some(),
            "slice and finite-reader admission agree"
        );
        if let Some(body) = body {
            let mut encoded = Vec::new();
            write(format, &body, &mut encoded).unwrap();
            assert_eq!(frame.get(..encoded.len()).unwrap(), encoded.as_slice());
            // A following frame is allowed; only the admitted first frame is
            // used for corruption/EOF controls, never the caller's trailing data.
            let cut = &encoded[..encoded.len() - 1];
            assert!(!format.is_valid(cut));
            assert!(format.read(cut, cut.len() as u64).ok().flatten().is_none());
            *encoded.last_mut().unwrap() ^= 1;
            assert!(!format.is_valid(&encoded));
            assert!(
                format
                    .read(encoded.as_slice(), encoded.len() as u64)
                    .ok()
                    .flatten()
                    .is_none()
            );
            let foreign = LogFrame {
                domain: b"foreign-codec-frame-test-v1\0",
                ..format
            };
            assert!(!foreign.is_valid(frame));
        }
    }
}

/// Actual narrow/wide writers, with empty and split nonempty bodies. Only
/// caller bounds precede each output; no production frame bytes are fabricated.
pub fn seeds() -> std::io::Result<Vec<(String, Vec<u8>)>> {
    let mut outputs = Vec::new();
    for wide in [false, true] {
        for (name, body) in [("empty", b"".as_slice()), ("split", b"codec".as_slice())] {
            let format = LogFrame {
                domain: DOMAIN,
                wide,
                minimum_body: 0,
                maximum_body: u64::MAX,
            };
            let mut bytes = Vec::new();
            bytes.extend_from_slice(&format.minimum_body.to_le_bytes());
            bytes.extend_from_slice(&format.maximum_body.to_le_bytes());
            write(format, body, &mut bytes)?;
            check(&bytes);
            outputs.push((format!("shared-frame-{wide}-{name}"), bytes));
        }
    }
    Ok(outputs)
}
