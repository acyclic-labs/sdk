use super::*;

#[test]
fn body_decoding_retains_the_owned_wire_buffer() -> Result<(), Box<dyn std::error::Error>> {
    for length in [128, 1024, 1024 * 1024] {
        let body = Bytes::from(vec![37; length]);
        let put = wire::PutBlobRequest {
            digest: Some(Digest::of(&body).into()),
            body: body.clone(),
            references: Vec::new(),
        };
        let encoded = Bytes::from(put.encode_to_vec());
        let allocation = encoded.as_ptr_range();
        let decoded: wire::PutBlobRequest = decode(encoded.clone())?;
        assert!(allocation.contains(&decoded.body.as_ptr()));
        // The former borrowed decoder copies the same body into another allocation.
        let copied = wire::PutBlobRequest::decode(encoded.as_ref())?;
        assert!(!allocation.contains(&copied.body.as_ptr()));
        drop(encoded);
        assert_eq!(decoded, put);

        let get = wire::GetBlobResponse {
            body: body.clone(),
            ..Default::default()
        };
        let encoded = Bytes::from(get.encode_to_vec());
        let allocation = encoded.as_ptr_range();
        let decoded: wire::GetBlobResponse = decode(encoded.clone())?;
        assert!(allocation.contains(&decoded.body.as_ptr()));
        let copied = wire::GetBlobResponse::decode(encoded.as_ref())?;
        assert!(!allocation.contains(&copied.body.as_ptr()));
        drop(encoded);
        assert_eq!(decoded, get);
        eprintln!(
            "body_bytes={length} borrowed_decode_copied={} owned_decode_copied=0",
            copied.body.len()
        );
    }
    Ok(())
}
