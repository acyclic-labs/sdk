include!(concat!(env!("OUT_DIR"), "/wire.rs"));
pub fn dependency_witness() -> relocation_helper::Witness {
    relocation_helper::Witness
}
