#[serde(transparent)]
struct Wrapper {
    #[serde(skip_serializing)]
    cache: (),
    value: String,
}
