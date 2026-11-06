/// Native classic-BPF instruction. Preparation, decoding and application
/// borrow this same layout without translating or copying its policy.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub(crate) struct Instruction {
    pub(super) code: u16,
    pub(super) jt: u8,
    pub(super) jf: u8,
    pub(super) k: u32,
}

const _: () = assert!(core::mem::size_of::<Instruction>() == 8);
