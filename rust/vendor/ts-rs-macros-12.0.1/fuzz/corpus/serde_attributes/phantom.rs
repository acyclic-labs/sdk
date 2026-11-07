#[serde(transparent)]
struct Wrapper(String, std::marker::PhantomData<()>);
