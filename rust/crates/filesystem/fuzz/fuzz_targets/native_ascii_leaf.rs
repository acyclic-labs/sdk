//! Exact native component validation, encoding and examined-prefix accounting.
#![no_main]

libfuzzer_sys::fuzz_target!(|data: [u8; 88]| {
    acyclic_fs::native_host::check_prepared_ascii_leaf(data);
    let position = usize::from(data[0]) % data.len();
    let mut name = data.map(|byte| byte & 0x7f);
    name[position] = 0;
    acyclic_fs::native_host::check_native_root_name_prefix(
        std::str::from_utf8(&name).expect("ASCII root input"),
    );
});
