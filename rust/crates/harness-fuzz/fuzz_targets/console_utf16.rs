#![no_main]
#[path = "../../harness/src/client/native_input/utf16.rs"]
mod utf16;

libfuzzer_sys::fuzz_target!(|bytes: &[u8]| {
    let units: Vec<u16> = bytes
        .chunks_exact(2)
        .map(|pair| u16::from_le_bytes([pair[0], pair[1]]))
        .take_while(|unit| *unit != 0x1a)
        .collect();
    let chunk = usize::from(bytes.first().copied().unwrap_or(0) % 16) + 1;
    let mut decoder = utf16::Utf16::default();
    let mut output = Vec::new();
    let mut buffer = [0; 51];
    for part in units.chunks(chunk) {
        let count = decoder.decode(part, &mut buffer);
        output.extend_from_slice(&buffer[..count]);
    }
    let count = decoder.decode(&[], &mut buffer);
    output.extend_from_slice(&buffer[..count]);
    assert_eq!(output, String::from_utf16_lossy(&units).into_bytes());
    assert!(decoder.ended);
});
