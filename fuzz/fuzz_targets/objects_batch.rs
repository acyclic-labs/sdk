#![no_main]
use acyclic_objects::v1::{request, wire};
use libfuzzer_sys::fuzz_target;
use prost::Message;

fuzz_target!(|bytes: &[u8]| {
    if bytes.len() > 16 * 1024 * 1024 {
        return;
    }
    // Length-delimited envelopes let mutations reach framing across RPC chunks.
    let mut input = bytes;
    let Ok(first) = wire::PutObjectsRequest::decode_length_delimited(&mut input) else {
        return;
    };
    let Some(wire::put_objects_request::Frame::Header(header)) = first.frame else {
        return;
    };
    let Ok(mut parser) = request::BatchUploadFraming::new(&header) else {
        return;
    };
    let (mut index, mut completed) = (0usize, false);
    while !input.is_empty() {
        let Ok(frame) = wire::PutObjectsRequest::decode_length_delimited(&mut input) else {
            return;
        };
        let canonical = frame.encode_to_vec();
        assert_eq!(wire::PutObjectsRequest::decode(canonical.as_slice()).unwrap(), frame);
        use wire::{put_objects_chunk::Frame as C, put_objects_request::Frame as F};
        let expected = !completed && match &frame.frame {
            Some(F::Chunk(chunk)) if chunk.index as usize == index && index < header.objects.len() => match &chunk.frame {
                Some(C::Body(body)) => body.len() <= 65_536,
                Some(C::Complete(true)) => true,
                _ => false,
            },
            Some(F::Complete(true)) => index == header.objects.len(),
            _ => false,
        };
        assert_eq!(parser.accept(&frame).is_ok(), expected);
        if !expected { return; }
        match frame.frame {
            Some(F::Chunk(wire::PutObjectsChunk { frame: Some(C::Complete(true)), .. })) => index += 1,
            Some(F::Complete(true)) => completed = true,
            _ => {},
        }
    }
    assert_eq!(parser.finish().is_ok(), completed);
});
