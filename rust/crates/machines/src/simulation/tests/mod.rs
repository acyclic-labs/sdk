use super::*;

fn request(key: IdempotencyKey) -> CreateMachine {
    CreateMachine::new(
        key,
        Image::custom([7; 32]).unwrap_or_else(|_| unreachable!()),
        [8; 32],
    )
}

fn key(suffix: u8) -> IdempotencyKey {
    IdempotencyKey::parse(&format!("00000000-0000-0000-0000-0000000001{suffix:02x}"))
        .unwrap_or_else(|_| unreachable!())
}

fn two() -> NonZeroU32 {
    NonZeroU32::new(2).unwrap_or(NonZeroU32::MIN)
}

mod forks;
mod operations;
mod replay;
