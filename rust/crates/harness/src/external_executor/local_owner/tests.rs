//! Authority cannot move to another workspace; local endpoints stay on loopback.
use super::*;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]
    #[test]
    fn credentials_round_trip_without_widening_workspace(
        secret in any::<[u8;32]>(), scope in any::<[u8;32]>(), bit in 0u8..64,
    ) {
        let scope = blake3::Hash::from_bytes(scope).to_hex().to_string();
        for workspace in [&scope, "owner"] {
            let encoded = credential::credential(&secret, workspace);
            prop_assert_eq!(credential::verify(&secret, &encoded).unwrap(), workspace);
            let (head, tail) = encoded.split_once('.').unwrap();
            let mut changed = tail.as_bytes().to_vec();
            changed[bit as usize] = if changed[bit as usize] == b'0' { b'1' } else { b'0' };
            let changed = format!("{head}.{}", String::from_utf8(changed).unwrap());
            prop_assert!(credential::verify(&secret, &changed).is_err());
            let widened = format!("{head}x.{tail}");
            prop_assert!(credential::verify(&secret, &widened).is_err());
        }
    }

    #[test]
    fn address_round_trip_never_accepts_remote_or_cleartext_endpoints(
        port in 1u16..=u16::MAX,
        instance in any::<[u8;16]>(),
        certificate in "[a-zA-Z0-9]{1,64}",
    ) {
        let address = Address { endpoint:format!("https://127.0.0.1:{port}"), certificate, instance:uuid::Uuid::from_bytes(instance) };
        let encoded = serde_json::to_vec(&address).unwrap();
        let decoded = Address::from_bytes(&encoded).unwrap();
        prop_assert_eq!(&decoded.endpoint, &address.endpoint);
        prop_assert_eq!(&decoded.certificate, &address.certificate);
        prop_assert_eq!(decoded.instance, address.instance);
        for endpoint in [format!("http://127.0.0.1:{port}"), format!("https://example.org:{port}"), "https://127.0.0.1:0".into()] {
            let changed = Address { endpoint, ..address.clone() };
            prop_assert!(Address::from_bytes(&serde_json::to_vec(&changed).unwrap()).is_err());
        }
    }
}

#[test]
fn owner_parser_seed_corpus_uses_the_production_decoders() {
    for seed in [
        include_bytes!("../../../../harness-fuzz/corpus/external-hooks/local-owner-address")
            .as_slice(),
        include_bytes!("../../../../harness-fuzz/corpus/external-hooks/local-owner-credential")
            .as_slice(),
    ] {
        fuzz_input(seed);
    }
}

#[test]
fn owner_boundary_futures_stay_below_sixteen_kibibytes() {
    let run = run_local_owner();
    let stop = stop_local_owner();
    let binding = binding(Path::new("."));
    let sizes = [
        ("owner", std::mem::size_of_val(&run)),
        ("stop", std::mem::size_of_val(&stop)),
        ("binding", std::mem::size_of_val(&binding)),
    ];
    for (name, size) in sizes {
        eprintln!("local {name} future: {size} bytes");
    }
    for (name, size) in sizes {
        assert!(size < 16 * 1024, "oversized {name} future: {size}");
    }
}
