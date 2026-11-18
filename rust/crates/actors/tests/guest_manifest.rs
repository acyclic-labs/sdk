//! Guest manifest wire round-trip refuses unrepresentable interrupt filters.
use acyclic_actors::conformance::Package;
use acyclic_actors::guest::wire::{decode_manifest, encode_manifest};
use bytes::Bytes;

#[test]
fn interrupt_filters_round_trip_and_refuse_empty_or_undeclared_prefixes()
-> Result<(), Box<dyn std::error::Error>> {
    let package = Package::new()?;
    let mut manifest = decode_manifest(&encode_manifest(package.manifest()))?;
    let encoded = encode_manifest(&manifest);
    assert_eq!(encode_manifest(&decode_manifest(&encoded)?), encoded);
    manifest.interrupts[0].value_prefix = Bytes::new();
    assert!(decode_manifest(&encode_manifest(&manifest)).is_err());
    manifest.interrupts[0].value_prefix = Bytes::from_static(b"stop");
    manifest.interrupts[0].subscription = "another-subscription".into();
    assert!(decode_manifest(&encode_manifest(&manifest)).is_err());
    Ok(())
}
