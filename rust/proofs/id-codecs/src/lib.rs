//! Bounded proofs call the owning types' actual codecs, including seal custody.
#[cfg(kani)]
mod version {
    #[kani::proof]
    #[kani::unwind(16)]
    fn version_admission_borrows_payload_and_refuses_before_decoding() {
        use acyclic_stream::{StreamError, persistence::header};
        let bytes: [u8; 12] = kani::any();
        let length: usize = kani::any();
        kani::assume(length <= bytes.len());
        let input = &bytes[..length];
        let result = header(input, b"kind-v", b"1\0");
        if let Ok(payload) = result {
            assert!(input.starts_with(b"kind-v1\0"));
            assert_eq!(payload, &input[8..]);
            assert_eq!(payload.as_ptr(), input[8..].as_ptr());
        } else if input.starts_with(b"kind-v") {
            assert_eq!(result, Err(StreamError::Unsupported));
        } else {
            assert_eq!(result, Err(StreamError::InvalidArgument));
        }
        for version in [b'0', b'2', u8::MAX] {
            let mut obsolete = *b"kind-v1\0";
            obsolete[6] = version;
            assert_eq!(
                header(&obsolete, b"kind-v", b"1\0"),
                Err(StreamError::Unsupported)
            );
        }
        assert_eq!(header(b"kind-v1\0", b"kind-v", b"1\0"), Ok(&[][..]));
    }
}

#[cfg(kani)]
mod proofs {
    use acyclic_fs::{Digest, GenerationId, ObjectId, ObjectKind, ObjectStorage};

    fn object() -> ObjectId {
        let tag: u8 = kani::any();
        kani::assume((1..=8).contains(&tag));
        ObjectId {
            kind: ObjectKind::from_canonical_tag(tag).unwrap(),
            digest: Digest::from_bytes(kani::any()),
            storage: if kani::any() {
                Some(ObjectStorage {
                    locator: Digest::from_bytes(kani::any()),
                    root_file_id: acyclic_fs::FileId::from_bytes(kani::any()),
                    origin_generation: Digest::from_bytes(kani::any()),
                })
            } else {
                None
            },
        }
    }

    #[kani::proof]
    #[kani::unwind(114)]
    fn every_object_kind_and_storage_association_round_trips() {
        let value = object();
        let encoded = value.to_bytes();
        assert_eq!(
            encoded.len(),
            if value.storage.is_some() { 113 } else { 33 }
        );
        assert_eq!(ObjectId::try_from(encoded.as_slice()), Ok(value));
    }

    #[kani::proof]
    #[kani::unwind(114)]
    fn generation_preserves_the_complete_root() {
        let root = ObjectId {
            kind: ObjectKind::GenerationRoot,
            ..object()
        };
        let value = GenerationId::from_root(root);
        let encoded = value.to_bytes();
        assert_eq!(GenerationId::from_bytes(encoded.as_slice()), Some(value));
        assert_eq!(value.object_id(), root);
        // The original raw-digest decoder must never accept a full identity.
        assert!(GenerationId::from_bytes(root.digest.as_bytes()).is_none());
    }

    #[kani::proof]
    #[kani::unwind(115)]
    fn arbitrary_bytes_have_one_canonical_identity_or_are_rejected() {
        let bytes: [u8; 114] = kani::any();
        let length: u8 = kani::any();
        kani::assume(length <= 114);
        let input = &bytes[..usize::from(length)];
        if let Ok(value) = ObjectId::try_from(input) {
            assert_eq!(value.to_bytes().as_slice(), input);
            assert!(length == 33 || length == 113);
            assert_eq!(
                GenerationId::from_bytes(input).is_some(),
                value.kind == ObjectKind::GenerationRoot
            );
        } else {
            assert!(GenerationId::from_bytes(input).is_none());
        }
    }

    #[kani::proof]
    #[kani::unwind(114)]
    fn another_seal_is_another_identity_even_for_the_same_plaintext() {
        let mut root = ObjectId {
            kind: ObjectKind::GenerationRoot,
            ..object()
        };
        let storage = ObjectStorage {
            locator: Digest::from_bytes(kani::any()),
            root_file_id: acyclic_fs::FileId::from_bytes(kani::any()),
            origin_generation: Digest::from_bytes(kani::any()),
        };
        root.storage = Some(storage);
        let mut other = root;
        let mut locator = *storage.locator.as_bytes();
        locator[0] ^= 1;
        other.storage = Some(ObjectStorage {
            locator: Digest::from_bytes(locator),
            ..storage
        });
        assert_ne!(root.to_bytes(), other.to_bytes());
        assert_ne!(
            GenerationId::from_root(root),
            GenerationId::from_root(other)
        );
    }

    #[kani::proof]
    #[kani::unwind(33)]
    fn fixed_width_digest_codecs_preserve_every_bit() {
        let bytes: [u8; 32] = kani::any();
        assert_eq!(Digest::from_bytes(bytes).into_bytes(), bytes);
        assert_eq!(
            *acyclic_objects::Digest::from_bytes(bytes).as_bytes(),
            bytes
        );
        assert_eq!(
            *acyclic_stream::CommitId::from_bytes(bytes).as_bytes(),
            bytes
        );
    }

    #[kani::proof]
    #[kani::unwind(17)]
    fn uuid_codecs_preserve_every_bit() {
        let bytes: [u8; 16] = kani::any();
        assert_eq!(
            acyclic_fs::AuthorityId::from_bytes(bytes).into_bytes(),
            bytes
        );
        assert_eq!(
            acyclic_fs::OperationId::from_bytes(bytes).into_bytes(),
            bytes
        );
        assert_eq!(acyclic_fs::VolumeId::from_bytes(bytes).into_bytes(), bytes);
        assert_eq!(
            acyclic_fs::CheckoutId::from_bytes(bytes).into_bytes(),
            bytes
        );
        assert_eq!(acyclic_fs::MountId::from_bytes(bytes).into_bytes(), bytes);
        assert_eq!(acyclic_fs::WatchId::from_bytes(bytes).into_bytes(), bytes);
        assert_eq!(acyclic_fs::FileId::from_bytes(bytes).into_bytes(), bytes);
    }
}

#[cfg(kani)]
mod framing {
    use acyclic_stream::{
        StreamError, wire,
        wire_codec::session::{FrameDecoder, grant_credit},
    };

    fn lengths(bytes: &[u8], chunk: usize) -> Result<Vec<usize>, StreamError> {
        let mut decoder = FrameDecoder::with_decoder(usize::MAX, |body| Ok(body.len()));
        let mut frames = Vec::new();
        for mut input in bytes.chunks(chunk) {
            while !input.is_empty() {
                let before = input.len();
                if let Some(length) = decoder.read(&mut input)? {
                    frames.push(length);
                }
                assert!(input.len() < before);
            }
        }
        decoder.finish()?;
        Ok(frames)
    }

    #[kani::proof]
    #[kani::unwind(17)]
    fn every_varint_prefix_and_eof_is_independent_of_chunking() {
        let bytes: [u8; 12] = kani::any();
        let length: u8 = kani::any();
        let chunk: u8 = kani::any();
        kani::assume(length <= 12);
        kani::assume(chunk > 0 && chunk <= 12);
        let input = &bytes[..usize::from(length)];
        assert_eq!(lengths(input, 12), lengths(input, usize::from(chunk)));
    }

    #[kani::proof]
    fn overflowing_credit_grants_never_partially_change_the_window() {
        let before = wire::SessionWindow {
            bytes: kani::any(),
            frames: kani::any(),
        };
        let grant = wire::SessionWindow {
            bytes: kani::any(),
            frames: kani::any(),
        };
        let mut after = before;
        match grant_credit(&mut after, &grant) {
            Ok(()) => {
                assert!(after.bytes >= before.bytes && after.frames >= before.frames);
                assert_eq!(after.bytes - before.bytes, grant.bytes);
                assert_eq!(after.frames - before.frames, grant.frames);
            }
            Err(_) => {
                assert_eq!(after, before);
                assert!(
                    before.bytes.checked_add(grant.bytes).is_none()
                        || before.frames.checked_add(grant.frames).is_none()
                );
            }
        }
    }

    #[kani::proof]
    #[kani::unwind(11)]
    fn a_dropped_partial_frame_is_a_typed_eof_error() {
        let mut decoder = FrameDecoder::with_decoder(usize::MAX, |body| Ok(body.len()));
        let mut prefix: &[u8] = &[1];
        assert_eq!(decoder.read(&mut prefix), Ok(None));
        assert_eq!(decoder.finish(), Err(StreamError::InvalidResponse));
        let mut body: &[u8] = &[0];
        assert_eq!(decoder.read(&mut body), Ok(Some(1)));
        assert_eq!(decoder.finish(), Ok(()));
    }
}
