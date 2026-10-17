use super::*;
use tokio::runtime::{Runtime, RuntimeFlavor};

#[test]
fn only_the_surviving_owner_uses_a_multithread_runtime() {
    let cases: [(fn() -> io::Result<Runtime>, RuntimeFlavor); 2] = [
        (runtime, RuntimeFlavor::CurrentThread),
        (owner_runtime, RuntimeFlavor::MultiThread),
    ];
    for (create, expected) in cases {
        let runtime = create().unwrap();
        assert_eq!(runtime.handle().runtime_flavor(), expected);
    }
}
