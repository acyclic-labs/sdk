//! Bounded proofs call the owning types' actual codecs, including seal custody.
#[cfg(kani)]
mod wire_metadata {
    #[kani::proof]
    #[kani::unwind(4)]
    fn scalar_metadata_preserves_all_facts_and_refuses_missing_opaque_bodies() {
        use acyclic_fs::{WorkspaceMetadata, binding_contract::*, kernel::MetadataField};
        let input = WorkspaceMetadata {
            posix_mode: kani::any(),
            posix_uid: kani::any(),
            posix_gid: kani::any(),
            posix_flags: kani::any(),
            windows_attributes: kani::any(),
            created_ns: kani::any(),
            modified_ns: kani::any(),
            accessed_ns: kani::any(),
            changed_ns: kani::any(),
            has_named_attributes: false,
            has_acl: false,
            has_security_descriptor: false,
        };
        let actual = metadata_from_proto(Some(metadata_to_proto(input))).unwrap();
        macro_rules! fact {
            ($($field:ident),+ $(,)?) => {$(
                assert_eq!(actual.$field, input.$field.map_or(MetadataField::Unavailable, MetadataField::Value));
            )+};
        }
        fact!(
            posix_mode,
            posix_uid,
            posix_gid,
            posix_flags,
            windows_attributes,
            created_ns,
            modified_ns,
            accessed_ns,
            changed_ns
        );
        assert_eq!(actual.named_attributes, MetadataField::Unavailable);
        assert_eq!(actual.acl, MetadataField::Unavailable);
        assert_eq!(actual.security_descriptor, MetadataField::Unavailable);
        for field in 0..3 {
            let mut missing = input;
            match field {
                0 => missing.has_named_attributes = true,
                1 => missing.has_acl = true,
                _ => missing.has_security_descriptor = true,
            }
            assert!(metadata_from_proto(Some(metadata_to_proto(missing))).is_err());
        }
    }
}

#[cfg(kani)]
mod resource_component {
    #[kani::proof]
    #[kani::unwind(32)]
    fn bounded_resource_components_are_lossless_and_have_one_spelling() {
        use acyclic_auth::{decode_component, encode_component};
        let bytes: [u8; 4] = kani::any();
        let length: usize = kani::any();
        kani::assume(length <= bytes.len());
        let mut encoded = encode_component(&bytes[..length]);
        assert_eq!(decode_component(&encoded).unwrap(), &bytes[..length]);
        encoded.push('%');
        assert!(matches!(
            decode_component(&encoded),
            Err(acyclic_auth::Error::Invalid)
        ));
        let input: [u8; 12] = kani::any();
        let length: usize = kani::any();
        kani::assume(length <= input.len());
        if let Ok(text) = std::str::from_utf8(&input[..length])
            && let Ok(decoded) = decode_component(text)
        {
            assert_eq!(encode_component(&decoded), text);
        }
    }
}

#[cfg(kani)]
mod journal_prefix {
    #[kani::proof]
    fn full_frame_extents_refuse_overflow_without_clamping() {
        use acyclic_native_runtime::LogFrame;
        let format = LogFrame {
            domain: b"extent-proof\0",
            wide: kani::any(),
            minimum_body: kani::any(),
            maximum_body: kani::any(),
        };
        let body: u64 = kani::any();
        let exact =
            u128::from(body) + format.prefix_bytes() as u128 + LogFrame::CHECKSUM_BYTES as u128;
        let representable = (format.wide || body <= u64::from(u32::MAX))
            && exact <= u128::from(u64::MAX)
            && exact <= usize::MAX as u128;
        assert_eq!(format.frame_bytes(body).is_some(), representable);
        if let Some(bytes) = format.frame_bytes(body) {
            assert_eq!(u128::from(bytes), exact);
        }
        assert_eq!(
            format.maximum_frame_bytes(),
            format.frame_bytes(format.maximum_body)
        );
    }

    #[kani::proof]
    #[kani::unwind(9)]
    fn impossible_declared_frames_refuse_before_body_io() {
        use acyclic_native_runtime::LogFrame;
        use std::io::{self, Read};

        struct PrefixOnly<'a> {
            remaining: &'a [u8],
            body_reads: usize,
        }
        impl Read for PrefixOnly<'_> {
            fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
                if out.is_empty() {
                    return Ok(0);
                }
                if self.remaining.is_empty() {
                    self.body_reads += 1;
                    return Err(io::ErrorKind::BrokenPipe.into());
                }
                let count = out.len().min(self.remaining.len());
                out[..count].copy_from_slice(&self.remaining[..count]);
                self.remaining = &self.remaining[count..];
                Ok(count)
            }
        }

        let format = LogFrame {
            domain: b"extent-proof\0",
            wide: kani::any(),
            minimum_body: 0,
            maximum_body: u64::MAX,
        };
        let bytes: [u8; 8] = kani::any();
        let prefix = &bytes[..format.prefix_bytes()];
        let length = format.body_length(prefix).unwrap();
        kani::assume(format.frame_bytes(length).is_none() || length > isize::MAX as u64);
        let mut reader = PrefixOnly {
            remaining: prefix,
            body_reads: 0,
        };
        let error = format.read(&mut reader, u64::MAX).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(reader.remaining.is_empty());
        assert_eq!(reader.body_reads, 0);
    }

    #[kani::proof]
    #[kani::unwind(9)]
    fn log_prefixes_preserve_lengths_and_refuse_unrepresentable_or_outside_bounds() {
        use acyclic_native_runtime::LogFrame;
        let wide: bool = kani::any();
        let format = LogFrame {
            domain: b"prefix-proof\0",
            wide,
            minimum_body: kani::any(),
            maximum_body: kani::any(),
        };
        let body: u64 = kani::any();
        let admitted = (format.minimum_body..=format.maximum_body).contains(&body)
            && (wide || u32::try_from(body).is_ok());
        let encoded = format.prefix(body);
        assert_eq!(encoded.is_some(), admitted);
        if let Some((prefix, width)) = encoded {
            assert_eq!(width, if wide { 8 } else { 4 });
            assert_eq!(format.body_length(&prefix[..width]), Some(body));
            let cut: usize = kani::any();
            kani::assume(cut < width);
            assert_eq!(format.body_length(&prefix[..cut]), None);
        }
        let bytes: [u8; 8] = kani::any();
        if let Some(length) = format.body_length(&bytes) {
            let (canonical, width) = format
                .prefix(length)
                .expect("an admitted prefix length is representable in the same width");
            assert_eq!(&canonical[..width], &bytes[..width]);
            assert!(length >= format.minimum_body && length <= format.maximum_body);
        }
    }
}

#[cfg(kani)]
mod version {
    #[kani::proof]
    #[kani::unwind(64)]
    fn bounded_actor_triples_round_trip_and_refuse_resource_encodings() {
        use acyclic_auth::addressing::{ActorReference, IdentityError, ResourceIdentity};
        let account: [u8; 4] = kani::any();
        let namespace: [u8; 4] = kani::any();
        let id: [u8; 4] = kani::any();
        let Ok(value) = ActorReference::new(&account, &namespace, &id) else {
            return;
        };
        let encoded = value.encode();
        assert_eq!(ActorReference::decode(&encoded), Ok(value));
        let resource = ResourceIdentity::new(&account, &id).unwrap().encode();
        assert!(ActorReference::decode(&resource).is_err());
        assert!(ResourceIdentity::decode(&encoded).is_err());
        let version = encoded.len() - (3 * 8 + account.len() + namespace.len() + id.len()) - 1;
        let other: u8 = kani::any();
        kani::assume(other != 1);
        let mut refused = encoded;
        refused[version] = other;
        refused.truncate(version + 1);
        assert_eq!(
            ActorReference::decode(&refused),
            Err(IdentityError::UnsupportedVersion)
        );
    }

    #[kani::proof]
    #[kani::unwind(41)]
    fn usn_checkpoint_preserves_accepted_bytes_and_refuses_other_versions() {
        use acyclic_fs::windows_usn_codec::{WindowsUsnCheckpoint, WindowsUsnError};
        let bytes: [u8; 40] = kani::any();
        if let Ok(value) = WindowsUsnCheckpoint::from_bytes(&bytes) {
            assert_eq!(value.to_bytes(), bytes);
            assert!(value.next_usn() <= i64::MAX as u64);
            for length in 8..40 {
                assert!(matches!(
                    WindowsUsnCheckpoint::from_bytes(&bytes[..length]),
                    Err(WindowsUsnError::InvalidCheckpoint)
                ));
            }
        }
        let mut refused = *b"ACYUSN\0\x01";
        let version: u8 = kani::any();
        kani::assume(version != 1);
        refused[7] = version;
        assert!(matches!(
            WindowsUsnCheckpoint::from_bytes(&refused),
            Err(WindowsUsnError::UnsupportedVersion)
        ));
    }

    #[kani::proof]
    #[kani::unwind(64)]
    fn bounded_resource_pairs_round_trip_and_refuse_other_versions() {
        use acyclic_auth::addressing::{IdentityError, ResourceIdentity};
        let account: [u8; 4] = kani::any();
        let id: [u8; 4] = kani::any();
        let value = ResourceIdentity::new(&account, &id).unwrap();
        let encoded = value.encode();
        assert_eq!(ResourceIdentity::decode(&encoded), Ok(value));
        let mut old = encoded.clone();
        // The final header byte precedes two fixed-length fields.
        let version = encoded.len() - (8 + account.len() + 8 + id.len()) - 1;
        let other: u8 = kani::any();
        kani::assume(other != 1);
        old[version] = other;
        old.truncate(version + 1);
        assert_eq!(
            ResourceIdentity::decode(&old),
            Err(IdentityError::UnsupportedVersion)
        );
        assert_eq!(
            ResourceIdentity::decode(&encoded[version + 1..]),
            Err(IdentityError::UnsupportedVersion)
        );
    }

    #[kani::proof]
    #[kani::unwind(16)]
    fn version_admission_borrows_payload_and_refuses_before_decoding() {
        use acyclic_stream::{StreamError, persistence::header};
        use std::cell::Cell;
        let bytes: [u8; 12] = kani::any();
        let length: usize = kani::any();
        kani::assume(length <= bytes.len());
        let input = &bytes[..length];
        let invalid = Cell::new(0);
        let unsupported = Cell::new(0);
        let result = acyclic_auth::format::header(
            input,
            b"kind-v",
            b"1\0",
            || {
                invalid.set(invalid.get() + 1);
                StreamError::InvalidArgument
            },
            || {
                unsupported.set(unsupported.get() + 1);
                StreamError::Unsupported
            },
        );
        assert_eq!(result, header(input, b"kind-v", b"1\0"));
        if let Ok(payload) = result {
            assert!(input.starts_with(b"kind-v1\0"));
            assert_eq!(payload, &input[8..]);
            assert_eq!(payload.as_ptr(), input[8..].as_ptr());
            assert_eq!((invalid.get(), unsupported.get()), (0, 0));
        } else if input.starts_with(b"kind-v") {
            assert_eq!(result, Err(StreamError::Unsupported));
            assert_eq!((invalid.get(), unsupported.get()), (0, 1));
        } else {
            assert_eq!(result, Err(StreamError::InvalidArgument));
            assert_eq!((invalid.get(), unsupported.get()), (1, 0));
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
    #[kani::unwind(122)]
    fn every_object_prefix_preserves_the_following_field() {
        let value = object();
        let encoded = value.to_bytes();
        let following: [u8; 8] = kani::any();
        let following_length: u8 = kani::any();
        kani::assume(following_length <= 8);
        let following_length = usize::from(following_length);
        let mut bytes = [0; 121];
        let width = encoded.len();
        bytes[..width].copy_from_slice(encoded.as_slice());
        bytes[width..width + following_length].copy_from_slice(&following[..following_length]);
        let input = &bytes[..width + following_length];
        let (decoded, tail) = ObjectId::from_prefix(input).unwrap();
        assert_eq!(decoded, value);
        assert_eq!(tail, &following[..following_length]);
        assert_eq!(tail.as_ptr(), input[width..].as_ptr());
        assert_eq!(ObjectId::try_from(input).is_ok(), following_length == 0);
        let cut: u8 = kani::any();
        kani::assume(usize::from(cut) < width);
        assert!(ObjectId::from_prefix(&input[..usize::from(cut)]).is_err());
    }

    #[kani::proof]
    #[kani::unwind(122)]
    fn arbitrary_object_prefixes_have_one_exact_identity_and_borrowed_tail() {
        let bytes: [u8; 121] = kani::any();
        let length: u8 = kani::any();
        kani::assume(length <= 121);
        let input = &bytes[..usize::from(length)];
        match ObjectId::from_prefix(input) {
            Ok((value, tail)) => {
                let encoded = value.to_bytes();
                assert!(input.starts_with(encoded.as_slice()));
                assert_eq!(tail, &input[encoded.len()..]);
                assert_eq!(tail.as_ptr(), input[encoded.len()..].as_ptr());
                assert_eq!(ObjectId::try_from(input).is_ok(), tail.is_empty());
                assert_eq!(
                    GenerationId::from_bytes(input).is_some(),
                    tail.is_empty() && value.kind == ObjectKind::GenerationRoot
                );
            }
            Err(_) => {
                assert!(ObjectId::try_from(input).is_err());
                assert!(GenerationId::from_bytes(input).is_none());
            }
        }
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
        let mut changed = storage;
        let field: u8 = kani::any();
        let byte: u8 = kani::any();
        let bit: u8 = kani::any();
        kani::assume(field < 3 && byte < 32 && bit < 8);
        match field {
            0 => {
                let mut bytes = *storage.locator.as_bytes();
                bytes[usize::from(byte)] ^= 1 << bit;
                changed.locator = Digest::from_bytes(bytes);
            }
            1 => {
                kani::assume(byte < 16);
                let mut bytes = storage.root_file_id.into_bytes();
                bytes[usize::from(byte)] ^= 1 << bit;
                changed.root_file_id = acyclic_fs::FileId::from_bytes(bytes);
            }
            _ => {
                let mut bytes = *storage.origin_generation.as_bytes();
                bytes[usize::from(byte)] ^= 1 << bit;
                changed.origin_generation = Digest::from_bytes(bytes);
            }
        }
        other.storage = Some(changed);
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

    #[kani::proof]
    #[kani::unwind(17)]
    fn grpc_header_admission_and_eof_are_independent_of_chunking() {
        let header: [u8; 5] = kani::any();
        let chunk: u8 = kani::any();
        kani::assume(chunk > 0 && chunk <= 5);
        let decode = |width: usize| {
            let mut decoder = FrameDecoder::grpc();
            let mut frame = None;
            for mut input in header.chunks(width) {
                while !input.is_empty() {
                    let before = input.len();
                    if let Some(value) = decoder.read(&mut input)? {
                        assert!(frame.replace(value).is_none());
                    }
                    assert!(input.len() < before);
                }
            }
            decoder.finish()?;
            Ok::<_, StreamError>(frame)
        };
        let result = decode(5);
        assert_eq!(result, decode(usize::from(chunk)));
        let length = u32::from_be_bytes(header[1..].try_into().unwrap()) as usize;
        match header[0] {
            1 => assert_eq!(result, Err(StreamError::Unsupported)),
            0 if length > acyclic_stream::MAX_COMMAND_BYTES => {
                assert_eq!(result, Err(StreamError::LimitExceeded));
            }
            0 if length == 0 => assert_eq!(result, Ok(Some(wire::SessionFrame::default()))),
            _ => assert_eq!(result, Err(StreamError::InvalidResponse)),
        }
    }

    #[kani::proof]
    #[kani::unwind(33)]
    fn credit_debits_preserve_both_balances_or_refuse_without_mutation() {
        use acyclic_stream::wire_codec::session::{consume_credit, encode_grpc_frame};
        let frame = wire::SessionFrame {
            channel_id: kani::any(),
            generation: kani::any(),
            body: Some(wire::session_frame::Body::Data(wire::SessionData::default())),
        };
        // The real encoder determines the charge; no wire-size formula is
        // duplicated in the proof. Its five transport bytes are not data credit.
        let bytes = (encode_grpc_frame(&frame).unwrap().len() - 5) as u64;
        let before = wire::SessionWindow {
            bytes: kani::any(),
            frames: kani::any(),
        };
        let mut after = before;
        match consume_credit(&mut after, &frame) {
            Ok(()) => {
                assert_ne!(frame.channel_id, 0);
                assert_eq!(before.bytes.checked_sub(bytes), Some(after.bytes));
                assert_eq!(before.frames.checked_sub(1), Some(after.frames));
            }
            Err(StreamError::InvalidArgument) => {
                assert_eq!(frame.channel_id, 0);
                assert_eq!(after, before);
            }
            Err(error) => {
                assert_eq!(error, StreamError::Capacity);
                assert_ne!(frame.channel_id, 0);
                assert!(before.bytes < bytes || before.frames == 0);
                assert_eq!(after, before);
            }
        }
        let control = wire::SessionFrame {
            body: Some(wire::session_frame::Body::Window(before)),
            ..frame
        };
        after = before;
        assert_eq!(consume_credit(&mut after, &control), Ok(()));
        assert_eq!(after, before);
    }
}
