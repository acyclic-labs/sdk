use std::env;
use std::io::{Read, Write};
use std::process::{Command, Stdio};

const BODY: &[u8] = br#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#;

fn main() {
    if env::args().nth(1).as_deref() == Some("--server") {
        let server = env::args().nth(2).expect("server identity");
        assert_eq!(env::var("ACYCLIC_WORKFLOW_TOKEN").as_deref(), Ok("qualified"));
        let cwd = env::current_dir().unwrap();
        assert!(cwd.join("acyclic-workflow.rs").is_file());
        std::fs::write(cwd.join(format!(".acyclic-workflow-child-{server}")), b"mounted")
            .unwrap();
        serve_one_frame();
        return;
    }
    assert_eq!(env::var("ACYCLIC_WORKFLOW_TOKEN").as_deref(), Ok("qualified"));
    assert!(env::current_dir().unwrap().join("acyclic-workflow.rs").is_file());
    let output = env::args().nth(1).expect("workflow output path");
    let executable = env::current_exe().unwrap();
    let mut servers = (0..2)
        .map(|server| {
            let mut child = Command::new(&executable)
                .args(["--server", &server.to_string()])
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .spawn()
                .unwrap();
            let mut stdin = child.stdin.take().unwrap();
            write!(
                stdin,
                "Content-Length: {}\r\n\r\n",
                BODY.len()
            )
            .unwrap();
            stdin.write_all(BODY).unwrap();
            drop(stdin);
            child
        })
        .collect::<Vec<_>>();
    for (server, child) in servers.iter_mut().enumerate() {
        let mut response = Vec::new();
        child.stdout.take().unwrap().read_to_end(&mut response).unwrap();
        assert!(child.wait().unwrap().success());
        assert!(response.ends_with(BODY));
        let marker = env::current_dir()
            .unwrap()
            .join(format!(".acyclic-workflow-child-{server}"));
        assert_eq!(std::fs::read(&marker).unwrap(), b"mounted");
    }
    std::fs::write(output, b"isolated").unwrap();
    print!("ACYCLIC_OVERLAY_WORKFLOW_OK");
}

fn serve_one_frame() {
    let stdin = std::io::stdin();
    let mut input = stdin.lock();
    let mut header = Vec::new();
    while !header.ends_with(b"\r\n\r\n") {
        let mut byte = [0];
        input.read_exact(&mut byte).unwrap();
        header.push(byte[0]);
        assert!(header.len() <= 128, "oversized frame header");
    }
    let header = std::str::from_utf8(&header[..header.len() - 4]).unwrap();
    let length = header
        .strip_prefix("Content-Length: ")
        .unwrap()
        .parse::<usize>()
        .unwrap();
    assert_eq!(length, BODY.len());
    let mut body = vec![0; length];
    input.read_exact(&mut body).unwrap();
    assert_eq!(body, BODY);
    print!("Content-Length: {}\r\n\r\n", body.len());
    std::io::stdout().write_all(&body).unwrap();
}
