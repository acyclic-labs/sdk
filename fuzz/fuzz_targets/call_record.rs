#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: &[u8]| {
    acyclic_actors::call::codec_fuzz::check(input);
    futures::executor::block_on(acyclic_actors::call::model::check(input))
        .expect("original committed call schedule");
});
