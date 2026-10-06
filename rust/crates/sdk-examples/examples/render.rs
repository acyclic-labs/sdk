use acyclic_sdk_examples::render_all;

fn main() {
    for snippet in render_all() {
        println!(
            "--- {} / {} ({}) [{}] ---",
            snippet.metadata.id,
            snippet.metadata.language,
            snippet.metadata.family,
            snippet.capability.as_str()
        );
        println!("{}\n", snippet.code);
    }
}
