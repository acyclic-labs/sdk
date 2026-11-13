use acyclic_fs::receive_packet_device_reply_for_test as receive_device;
#[path = "../../../src/native_mount/fuse/mount/reply_check.rs"]
mod check;
libfuzzer_sys::fuzz_target!(|input: &[u8]| check::check(input));
