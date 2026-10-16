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
    let lines = records.lines().collect::<Vec<_>>();
    for pair in lines.chunks_exact(2) {
        if args.last().unwrap().ends_with(pair[0]) {
            print!("{}\n200", pair[1]);
            return;
        }
    }
    print!("\n404");
}
