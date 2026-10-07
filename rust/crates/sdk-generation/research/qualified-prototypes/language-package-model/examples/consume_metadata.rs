use language_package_model::{Language, read_language_metadata, validate_language_metadata};
use std::{env, path::PathBuf};

fn main() {
    let mut args = env::args().skip(1);
    let path = PathBuf::from(args.next().expect("metadata path"));
    let patch_sha256 = args.next().expect("generator patch sha256");
    let metadata = validate_language_metadata(&path, Language::Ruby, &patch_sha256)
        .expect("Ruby metadata must bind the maintained generator patch");
    let raw = read_language_metadata(&path).expect("metadata must remain readable");
    assert_eq!(metadata, raw);
    assert_eq!(metadata.async_future_hooks.len(), 4);
    assert_eq!(metadata.async_exports.len(), 11);
    println!("RUBY_LANGUAGE_METADATA_CONSUMER_PASS");
}
