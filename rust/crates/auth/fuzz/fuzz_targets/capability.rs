#![no_main]
use acyclic_auth::contract::{Block, BlockProto, Capability, CapabilityProto};
use libfuzzer_sys::fuzz_target;
use prost::Message;

fuzz_target!(|bytes: &[u8]| {
    if let Ok(wire) = CapabilityProto::decode(bytes)
        && let Ok(value) = Capability::try_from(wire)
    {
        for block in &value.blocks {
            if let Ok(wire) = BlockProto::decode(block.payload.as_slice())
                && let Ok(block) = Block::try_from(wire)
            {
                let wire: BlockProto = block.clone().into();
                assert!(BlockProto::decode(wire.encode_to_vec().as_slice())
                    .is_ok_and(|wire| Block::try_from(wire) == Ok(block)));
            }
        }
        let wire: CapabilityProto = value.clone().into();
        let decoded = CapabilityProto::decode(wire.encode_to_vec().as_slice());
        assert!(decoded.is_ok_and(|wire| Capability::try_from(wire) == Ok(value)));
    }
});
