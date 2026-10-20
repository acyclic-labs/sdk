use acyclic_auth::contract::Limits;

pub fn limits(value: Option<u64>) -> Limits {
    Limits {
        rps: value,
        burst: value,
        bytes_per_s: value,
        channels: value,
        concurrency: value,
        bytes: value,
        spend_micros: value,
        tokens: value,
    }
}
