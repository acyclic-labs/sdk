use super::{Error, JsonCodecCharge, Result, Value, boundary, serialization::Scope};

// This decoder consumes the already bounded, validated serializer output.
// In particular, admission errors never pass through serde_json::Error::custom.
// Strings are unescaped in that output buffer, without a second scratch buffer.
pub(super) fn value(bytes: &mut [u8], table: &[u8], scope: &mut Scope<'_>) -> Result<Value> {
    #[cfg(all(
        any(test, feature = "test-support"),
        feature = "filesystem",
        not(target_arch = "wasm32")
    ))]
    {
        acyclic_differential::record(("decode document", bytes.len() as u64));
        acyclic_differential::record(("decode index", table.len() as u64));
    }
    let mut decoder = Decoder {
        bytes,
        table,
        array: 0,
        position: 0,
        scope,
    };
    let value = decoder.value()?;
    decoder.space();
    if decoder.position != decoder.bytes.len()
        || super::arrays::bytes(decoder.array)? != table.len()
    {
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
    table: &'a [u8],
    array: usize,
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

    fn object(&mut self) -> Result<Value> {
        let mut entries: Vec<(String, Value, usize)> = Vec::new();
        let mut scratch = 0;
        if !self.take(b'}') {
            loop {
                let key = self.string()?;
                self.require(b':')?;
                let value = self.value()?;
                if entries.len() == entries.capacity() {
                    let capacity = entries.len().checked_mul(2).ok_or_else(invalid)?.max(1);
                    let bytes = super::checked(
                        (capacity as u64)
                            .checked_mul(std::mem::size_of::<(String, Value, usize)>() as u64),
                        "JSON object scratch overflow",
                    )?;
                    self.scope.retain(bytes - scratch)?;
                    self.scope
                        .event(JsonCodecCharge::AllocationOperation { bytes })?;
                    entries
                        .try_reserve_exact(capacity - entries.len())
                        .map_err(Error::Allocation)?;
                    scratch =
                        (entries.capacity() * std::mem::size_of::<(String, Value, usize)>()) as u64;
                    self.scope.retain(scratch - bytes)?;
                }
                entries.push((key, value, entries.len()));
                if self.take(b'}') {
                    break;
                }
                self.require(b',')?;
            }
        }
        // Sort without allocating, keeping the last value for duplicate keys.
        entries.sort_unstable_by(|a, b| a.0.cmp(&b.0).then(a.2.cmp(&b.2)));
        let unique = usize::from(!entries.is_empty())
            + entries
                .windows(2)
                .filter(|pair| pair[0].0 != pair[1].0)
                .count();
        // Conservative occupancy bound, shared with the retained-byte audit:
        // eleven slots per node, at least five entries in each non-root node,
        // and at least one in the root. This does not predict split positions.
        let operations = if unique == 0 {
            0
        } else {
            1 + (unique as u64 - 1) / 5
        };
        if operations != 0 {
            self.scope
                .event(JsonCodecCharge::AllocationOperations { operations })?;
        }
        let mut values = serde_json::Map::new();
        for (key, value, _) in entries.drain(..) {
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
                let count = super::arrays::count(self.table, self.array)?;
                self.array += 1;
                let bytes = super::checked(
                    (count as u64).checked_mul(std::mem::size_of::<Value>() as u64),
                    "JSON array storage overflow",
                )?;
                if bytes != 0 {
                    self.scope
                        .event(JsonCodecCharge::AllocationOperation { bytes })?;
                }
                let mut values = Vec::new();
                values.try_reserve_exact(count).map_err(Error::Allocation)?;
                if !self.take(b']') {
                    loop {
                        let value = self.value()?;
                        if values.len() == count {
                            return Err(invalid());
                        }
                        values.push(value);
                        if self.take(b']') {
                            break;
                        }
                        self.require(b',')?;
                    }
                }
                if values.len() != count {
                    return Err(invalid());
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
                let scalar = &self.bytes[start..self.position];
                match scalar {
                    b"null" => Ok(Value::Null),
                    b"true" => Ok(Value::Bool(true)),
                    b"false" => Ok(Value::Bool(false)),
                    _ => {
                        let text = std::str::from_utf8(scalar).map_err(|_| invalid())?;
                        if scalar != b"-0" && !scalar.iter().any(|b| b".eE".contains(b)) {
                            if text.starts_with('-') {
                                if let Ok(number) = text.parse::<i64>() {
                                    return Ok(Value::from(number));
                                }
                            } else if let Ok(number) = text.parse::<u64>() {
                                return Ok(Value::from(number));
                            }
                        }
                        // Serde's float_roundtrip slow path uses heap-backed
                        // big integers. std's correctly rounded parser keeps
                        // its decimal scratch on the stack instead.
                        let number = text.parse::<f64>().map_err(|_| invalid())?;
                        serde_json::Number::from_f64(number)
                            .map(Value::Number)
                            .ok_or_else(invalid)
                    }
                }
            }
        }
    }
}
