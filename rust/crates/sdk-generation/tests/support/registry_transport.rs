fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    assert!(args.windows(2).any(|pair| pair[0] == "--user-agent"
        && pair[1].starts_with("acyclic-sdk-docs/")
        && pair[1].contains("https://github.com/acyclic-labs/sdk")));
    let records = std::fs::read_to_string(
        std::env::current_exe()
            .unwrap()
            .parent()
            .unwrap()
            .join("registry-responses.txt"),
    )
    .unwrap();
    let mut lines = records.lines();
    let url = args.last().unwrap();
    while let (Some(suffix), Some(response)) = (lines.next(), lines.next()) {
        if url.ends_with(suffix) {
            print!("{response}\n200");
            return;
        }
    }
    print!("\n404");
}
