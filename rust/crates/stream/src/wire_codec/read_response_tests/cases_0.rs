use super::{read_response_records, read_response_wire, wire, zstd_read_response};

fn records(from: u64, count: u64) -> Vec<wire::Record> {
    (from..from + count)
        .map(|sequence| wire::Record {
            sequence,
            value: vec![u8::try_from(sequence % 251).unwrap_or(0); 100].into(),
            commit_id: vec![1; 32].into(),
            committed_at_micros: sequence,
        })
        .collect()
}

#[test]
fn compressed_frame_decodes_to_the_same_records() {
    for count in [0, 1, 64] {
        let expected = records(5, count);
        let frame = zstd_read_response(expected.clone());
        assert!(
            count < 64 || frame.data.len() < usize::try_from(frame.decoded_length).unwrap_or(0)
        );
        assert_eq!(read_response_records(frame).ok(), Some(expected.clone()));
        assert_eq!(
            read_response_records(read_response_wire(expected.clone())).ok(),
            Some(expected)
        );
    }
}

#[test]
fn empty_zstd_data_is_refused_and_an_empty_plain_frame_is_accepted() {
    let empty = wire::ReadResponse {
        codec: wire::Codec::Zstd as i32,
        data: Default::default(),
        decoded_length: 0,
    };
    assert!(read_response_records(empty).is_err());
    assert_eq!(
        read_response_records(read_response_wire(Vec::new())).ok(),
        Some(Vec::new())
    );
}

#[test]
fn mixed_frames_reassemble_in_order() {
    let expected = records(0, 30);
    let frames = expected
        .chunks(7)
        .enumerate()
        .map(|(index, chunk)| {
            if index % 2 == 0 {
                zstd_read_response(chunk.to_vec())
            } else {
                read_response_wire(chunk.to_vec())
            }
        })
        .collect::<Vec<_>>();
    let mut result = Vec::new();
    for frame in frames {
        result.extend(read_response_records(frame).expect("valid frame"));
    }
    assert_eq!(result, expected);
}

#[test]
fn oversized_declared_length_is_refused_before_decompressing() {
    let maximum = crate::MAX_COMMAND_BYTES as u64;
    for length in [maximum + 1, u64::MAX] {
        for codec in [wire::Codec::None, wire::Codec::Zstd] {
            let frame = wire::ReadResponse {
                codec: codec as i32,
                data: vec![0xff; 16].into(),
                decoded_length: length,
            };
            assert!(read_response_records(frame).is_err());
        }
    }
    let mut expanding = zstd_read_response(records(0, 64));
    expanding.decoded_length = 100;
    assert!(read_response_records(expanding).is_err());
}

#[test]
fn mismatched_length_unknown_codec_and_corrupt_data_are_refused() {
    let mut long = zstd_read_response(records(0, 3));
    long.decoded_length += 1;
    assert!(read_response_records(long).is_err());
    let mut plain = read_response_wire(records(0, 3));
    plain.decoded_length -= 1;
    assert!(read_response_records(plain).is_err());
    for codec in [0, 3] {
        let mut unknown = read_response_wire(records(0, 3));
        unknown.codec = codec;
        assert!(read_response_records(unknown).is_err());
    }
    let mut corrupt = zstd_read_response(records(0, 30));
    corrupt.data.truncate(corrupt.data.len() / 2);
    assert!(read_response_records(corrupt).is_err());
}
