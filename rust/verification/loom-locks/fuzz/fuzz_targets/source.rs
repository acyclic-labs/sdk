#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|source: &str| {
    if let Ok(file) = syn::parse_file(source) {
        let rendered = quote::quote!(#file).to_string();
        assert_eq!(
            loom_lock_scan::scan(source).ok(),
            loom_lock_scan::scan(&rendered).ok()
        );
    }
});
