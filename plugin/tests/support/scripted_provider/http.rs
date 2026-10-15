use serde_json::json;
use std::{
    io::{Read as _, Write as _},
    net::TcpStream,
    time::Duration,
};

pub(super) fn write_conflict(stream: &mut TcpStream, message: &str) {
    write_response(
        stream,
        409,
        "application/json",
        &json!({"error":message}).to_string(),
    );
}

pub(super) fn read_request(stream: &mut TcpStream) -> Option<(String, String, Vec<u8>)> {
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .ok()?;
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 4096];
    let header_end = loop {
        let read = stream.read(&mut buffer).ok()?;
        if read == 0 {
            return None;
        }
        bytes.extend_from_slice(buffer.get(..read)?);
        if let Some(index) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            break index + 4;
        }
        if bytes.len() > 1024 * 1024 {
            return None;
        }
    };
    let headers = std::str::from_utf8(bytes.get(..header_end)?).ok()?;
    let mut lines = headers.lines();
    let mut request_line = lines.next()?.split_whitespace();
    let method = request_line.next()?.to_owned();
    let path = request_line.next()?.to_owned();
    let fields = lines
        .filter_map(|line| line.split_once(':'))
        .collect::<Vec<_>>();
    let content_length = fields
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, value)| value.trim().parse::<usize>().ok());
    let chunked = fields.iter().any(|(name, value)| {
        name.eq_ignore_ascii_case("transfer-encoding")
            && value
                .split(',')
                .any(|encoding| encoding.trim().eq_ignore_ascii_case("chunked"))
    });
    if chunked {
        while !bytes
            .get(header_end..)
            .is_some_and(|body| body.windows(5).any(|window| window == b"0\r\n\r\n"))
        {
            let read = stream.read(&mut buffer).ok()?;
            if read == 0 {
                return None;
            }
            bytes.extend_from_slice(buffer.get(..read)?);
        }
        return Some((method, path, decode_chunked(bytes.get(header_end..)?)?));
    }
    let content_length = content_length.unwrap_or(0);
    while bytes.len() < header_end + content_length {
        let read = stream.read(&mut buffer).ok()?;
        if read == 0 {
            return None;
        }
        bytes.extend_from_slice(buffer.get(..read)?);
    }
    Some((
        method,
        path,
        bytes.get(header_end..header_end + content_length)?.to_vec(),
    ))
}

fn decode_chunked(bytes: &[u8]) -> Option<Vec<u8>> {
    let mut cursor = 0;
    let mut decoded = Vec::new();
    loop {
        let remainder = bytes.get(cursor..)?;
        let line_end = remainder.windows(2).position(|window| window == b"\r\n")?;
        let size = std::str::from_utf8(remainder.get(..line_end)?)
            .ok()?
            .split(';')
            .next()?
            .trim();
        let size = usize::from_str_radix(size, 16).ok()?;
        cursor = cursor.checked_add(line_end + 2)?;
        if size == 0 {
            return Some(decoded);
        }
        let end = cursor.checked_add(size)?;
        decoded.extend_from_slice(bytes.get(cursor..end)?);
        if bytes.get(end..end + 2)? != b"\r\n" {
            return None;
        }
        cursor = end + 2;
    }
}

pub(super) fn write_response(stream: &mut TcpStream, status: u16, content_type: &str, body: &str) {
    let reason = if status == 200 { "OK" } else { "Error" };
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.write_all(response.as_bytes());
    let _ = stream.flush();
}
