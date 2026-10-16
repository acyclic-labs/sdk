use super::{Error, JsonCodecCharge, Result, Value, boundary, serialization::Scope};

// This decoder consumes the already bounded, validated serializer output.
// In particular, admission errors never pass through serde_json::Error::custom.
// Strings are unescaped in that output buffer, without a second scratch buffer.
pub(super) fn value(bytes: &mut [u8], scope: &mut Scope<'_>) -> Result<Value> {
    let mut decoder = Decoder {
        bytes,
        position: 0,
        scope,
    };
    let value = decoder.value()?;
    decoder.space();
    if decoder.position != decoder.bytes.len() {
        return Err(invalid());
    }
    Ok(value)
}

fn invalid() -> Error {
    boundary(
        "invalid admitted JSON",
        Some(crate::wire::ErrorCode::Invalid),
    )
}

struct Decoder<'a, 'b, 'c> {
    bytes: &'a mut [u8],
    position: usize,
    scope: &'b mut Scope<'c>,
}

impl Decoder<'_, '_, '_> {
    fn space(&mut self) {
        while self
            .bytes
            .get(self.position)
            .is_some_and(u8::is_ascii_whitespace)
        {
            self.position += 1;
        }
    }

    fn take(&mut self, byte: u8) -> bool {
        self.space();
        if self.bytes.get(self.position) == Some(&byte) {
            self.position += 1;
            true
        } else {
            false
        }
    }

    fn require(&mut self, byte: u8) -> Result<()> {
        if self.take(byte) {
            Ok(())
        } else {
            Err(invalid())
        }
    }

    fn hex(&mut self) -> Result<u16> {
        let mut value = 0;
        for _ in 0..4 {
            let byte = *self.bytes.get(self.position).ok_or_else(invalid)?;
            self.position += 1;
            value = (value << 4) | (byte as char).to_digit(16).ok_or_else(invalid)? as u16;
        }
        Ok(value)
    }

    fn string(&mut self) -> Result<String> {
        self.require(b'"')?;
        let start = self.position;
        let mut end = start;
        loop {
            let mut byte = *self.bytes.get(self.position).ok_or_else(invalid)?;
            self.position += 1;
            if byte == b'"' {
                break;
            }
            if byte == b'\\' {
                byte = *self.bytes.get(self.position).ok_or_else(invalid)?;
                self.position += 1;
                byte = match byte {
                    b'"' | b'\\' | b'/' => byte,
                    b'b' => 8,
                    b'f' => 12,
                    b'n' => b'\n',
                    b'r' => b'\r',
                    b't' => b'\t',
                    b'u' => {
                        let first = self.hex()?;
                        let code = if (0xd800..=0xdbff).contains(&first) {
                            self.require(b'\\')?;
                            self.require(b'u')?;
                            let second = self.hex()?;
                            if !(0xdc00..=0xdfff).contains(&second) {
                                return Err(invalid());
                            }
                            0x10000 + ((u32::from(first) - 0xd800) << 10) + u32::from(second)
                                - 0xdc00
                        } else {
                            u32::from(first)
                        };
                        let character = char::from_u32(code).ok_or_else(invalid)?;
                        let mut encoded = [0; 4];
                        let encoded = character.encode_utf8(&mut encoded).as_bytes();
                        self.bytes[end..end + encoded.len()].copy_from_slice(encoded);
                        end += encoded.len();
                        continue;
                    }
                    _ => return Err(invalid()),
                };
            } else if byte < 32 {
                return Err(invalid());
            }
            self.bytes[end] = byte;
            end += 1;
        }
        let text = std::str::from_utf8(&self.bytes[start..end]).map_err(|_| invalid())?;
        let bytes = text.len() as u64;
        self.scope.event(JsonCodecCharge::CopyAdmission { bytes })?;
        if bytes != 0 {
            self.scope
                .event(JsonCodecCharge::AllocationOperation { bytes })?;
        }
        let mut copied = String::new();
        copied
            .try_reserve_exact(text.len())
            .map_err(Error::Allocation)?;
        copied.push_str(text);
        self.scope.event(JsonCodecCharge::Copy { bytes })?;
        Ok(copied)
    }

    fn grow<T>(&mut self, values: &mut Vec<T>, scratch: bool) -> Result<()> {
        if values.len() == values.capacity() {
            let bytes = ((values.len() + 1) * std::mem::size_of::<T>()) as u64;
            if scratch {
                self.scope.retain(std::mem::size_of::<T>() as u64)?;
            }
            self.scope
                .event(JsonCodecCharge::AllocationOperation { bytes })?;
            values.try_reserve_exact(1).map_err(Error::Allocation)?;
        }
        Ok(())
    }

    fn object(&mut self) -> Result<Value> {
        let mut entries = Vec::new();
        if !self.take(b'}') {
            loop {
                let key = self.string()?;
                self.require(b':')?;
                let value = self.value()?;
                self.grow(&mut entries, true)?;
                entries.push((key, value, entries.len()));
                if self.take(b'}') {
                    break;
                }
                self.require(b',')?;
            }
        }
        // Sort without allocating, keeping the last value for duplicate keys.
        entries.sort_unstable_by(|a, b| a.0.cmp(&b.0).then(a.2.cmp(&b.2)));
        let scratch = (entries.capacity() * std::mem::size_of::<(String, Value, usize)>()) as u64;
        let mut values = serde_json::Map::new();
        for (key, value, _) in entries.drain(..) {
            if !values.contains_key(&key) {
                // std's B-tree has eleven slots per node. Ascending inserts
                // split a full node into six left entries and five right
                // entries before inserting the new rightmost entry. A root
                // split also allocates its replacement parent. Admit these
                // together: a denied split must not charge half an insertion.
                let mut length = values.len() + 1;
                let mut operations = u64::from(length == 1);
                while length >= 12 && length % 6 == 0 {
                    operations += 1 + u64::from(length == 12);
                    length = length / 6 - 1;
                }
                if operations != 0 {
                    self.scope
                        .event(JsonCodecCharge::AllocationOperations { operations })?;
                }
            }
            values.insert(key, value);
        }
        drop(entries);
        self.scope.release(scratch)?;
        Ok(Value::Object(values))
    }

    fn value(&mut self) -> Result<Value> {
        self.space();
        match self.bytes.get(self.position).copied().ok_or_else(invalid)? {
            b'"' => self.string().map(Value::String),
            b'{' => {
                self.position += 1;
                self.object()
            }
            b'[' => {
                self.position += 1;
                let mut values = Vec::new();
                if !self.take(b']') {
                    loop {
                        let value = self.value()?;
                        self.grow(&mut values, false)?;
                        values.push(value);
                        if self.take(b']') {
                            break;
                        }
                        self.require(b',')?;
                    }
                }
                Ok(Value::Array(values))
            }
            _ => {
                let start = self.position;
                while self
                    .bytes
                    .get(self.position)
                    .is_some_and(|b| !b.is_ascii_whitespace() && !b",]}".contains(b))
                {
                    self.position += 1;
                }
                // Successful scalar decoding has no heap storage or scratch.
                serde_json::from_slice(&self.bytes[start..self.position]).map_err(|_| invalid())
            }
        }
    }
}
