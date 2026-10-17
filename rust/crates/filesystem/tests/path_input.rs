use acyclic_fs::{
    VolumeLimits,
    kernel::{LogicalName, NameEncoding, TreePageError},
    path::PortablePath,
};

const FOLDS: &[(&str, &str)] = &[
    ("A", "a"),
    ("Straße", "strasse"),
    ("ẞ", "ss"),
    ("ΟΣ", "οσ"),
    ("ος", "οσ"),
    ("ΟΣΑ", "οσα"),
    ("İ", "i\u{307}"),
    ("ﬃ", "ffi"),
    ("K", "k"),
    ("Ꭰꭰ", "ᎠᎠ"),
];

#[derive(Debug)]
pub struct Seen {
    site: &'static str,
    equal: bool,
}

fn judge(event: &Seen, site: &str) -> Result<(), String> {
    if event.site != site || event.equal {
        Ok(())
    } else {
        Err(site.into())
    }
}
type Invariant = (&'static str, fn(&Seen) -> Result<(), String>);
pub const INVARIANTS: [Invariant; 5] = [
    ("portable", |event| judge(event, "portable")),
    ("utf8-fold", |event| judge(event, "utf8-fold")),
    ("full-fold", |event| judge(event, "full-fold")),
    ("posix", |event| judge(event, "posix")),
    ("utf16", |event| judge(event, "utf16")),
];
pub type Parse = fn(&str, VolumeLimits) -> Result<PortablePath, acyclic_fs::path::PathError>;
pub type New = fn(NameEncoding, Vec<u8>, u32) -> Result<LogicalName, TreePageError>;
pub type Fold = fn(&LogicalName) -> Vec<u8>;

fn record<E>(
    see: &mut impl FnMut(Seen) -> Result<(), E>,
    site: &'static str,
    equal: bool,
) -> Result<(), E> {
    see(Seen { site, equal })
}

pub fn check(data: &[u8]) {
    observe(
        data,
        PortablePath::parse,
        LogicalName::new,
        LogicalName::case_fold_key,
        &mut |event| {
            for (name, invariant) in INVARIANTS {
                if let Err(detail) = invariant(&event) {
                    panic!("{name}: {detail}");
                }
            }
            Ok::<_, std::convert::Infallible>(())
        },
    )
    .unwrap_or_else(|never| match never {});
}

pub fn observe<E>(
    data: &[u8],
    parse: Parse,
    new: New,
    fold: Fold,
    see: &mut impl FnMut(Seen) -> Result<(), E>,
) -> Result<(), E> {
    let limits = VolumeLimits::default();
    if let Ok(text) = std::str::from_utf8(data)
        && let Ok(path) = parse(text, limits)
    {
        record(see, "portable", path.as_str() == text)?;
        record(
            see,
            "portable",
            parse(path.as_str(), limits).is_ok_and(|other| other == path),
        )?;
        record(see, "portable", path.is_within(&path))?;
        record(
            see,
            "portable",
            path.is_within(&parse("/", limits).unwrap()),
        )?;
    }
    native_names(data, new, see)?;
    if let Ok(text) = std::str::from_utf8(data) {
        for component in text.split('/') {
            let Ok(name) = LogicalName::new(
                NameEncoding::Utf8,
                component.as_bytes().to_vec(),
                limits.maximum_component_bytes,
            ) else {
                continue;
            };
            let alias = LogicalName::new(
                NameEncoding::Utf8,
                component.to_ascii_uppercase().into_bytes(),
                limits.maximum_component_bytes,
            )
            .unwrap();
            let key = fold(&name);
            record(see, "utf8-fold", key == fold(&alias))?;
            if let Ok(folded) = LogicalName::new(
                NameEncoding::Utf8,
                key.clone(),
                limits.maximum_component_bytes,
            ) {
                record(see, "utf8-fold", fold(&folded) == key)?;
            }
        }
    }
    // Independent full-fold mappings compose without locale/context.
    let parts: Vec<_> = data
        .iter()
        .take(16)
        .copied()
        .chain(std::iter::once(0))
        .map(|index| FOLDS[usize::from(index) % FOLDS.len()])
        .collect();
    let text: String = parts.iter().map(|&(text, _)| text).collect();
    let expected: String = parts.iter().map(|&(_, folded)| folded).collect();
    let name = LogicalName::new(
        NameEncoding::Utf8,
        text.into_bytes(),
        limits.maximum_component_bytes,
    )
    .unwrap();
    record(see, "full-fold", fold(&name) == expected.as_bytes())?;
    let folded = LogicalName::new(
        NameEncoding::Utf8,
        expected.into_bytes(),
        limits.maximum_component_bytes,
    )
    .unwrap();
    record(see, "full-fold", fold(&folded) == folded.as_bytes())?;
    let path = format!(
        "/{}",
        parts
            .iter()
            .map(|&(text, _)| text)
            .collect::<Vec<_>>()
            .join("/")
    );
    let canonical = parse(&path, limits).unwrap();
    record(see, "portable", canonical.depth() == parts.len())?;
    for suffix in ["/", "//leaf", "/.", "/..", "/\0"] {
        record(
            see,
            "portable",
            parse(&format!("{path}{suffix}"), limits).is_err(),
        )?;
    }
    let child = parse(&format!("{path}/child"), limits).unwrap();
    let sibling = parse(&format!("{path}-sibling"), limits).unwrap();
    record(see, "portable", child.is_within(&canonical))?;
    record(see, "portable", !sibling.is_within(&canonical))?;
    record(see, "portable", !canonical.is_within(&child))?;
    Ok(())
}

fn native_names<E>(
    data: &[u8],
    new: New,
    see: &mut impl FnMut(Seen) -> Result<(), E>,
) -> Result<(), E> {
    let maximum = VolumeLimits::default().maximum_component_bytes;
    // Prefixes force opaque non-Unicode cases. Empty input still tests both encodings.
    for chunk in data.chunks(64).chain(std::iter::once(&[][..])) {
        let posix: Vec<_> = [b'n', 0xff, b'\\']
            .into_iter()
            .chain(chunk.iter().map(|&byte| match byte {
                0 | b'/' => b'x',
                byte => byte,
            }))
            .collect();
        let windows: Vec<_> = std::iter::once(u16::from(b'n'))
            .chain(chunk.chunks(2).map(|pair| {
                match u16::from_le_bytes([pair[0], pair.get(1).copied().unwrap_or(0)]) {
                    0 | 47 | 92 => u16::from(b'x'),
                    unit => unit,
                }
            }))
            .chain([0xd800, 0xd800])
            .flat_map(u16::to_le_bytes)
            .collect();
        for (encoding, bytes, site) in [
            (NameEncoding::PosixBytes, posix, "posix"),
            (NameEncoding::WindowsUtf16Le, windows, "utf16"),
        ] {
            let name = new(encoding, bytes.clone(), maximum);
            record(see, site, name.is_ok())?;
            // A deleted admission invariant must proceed past the exposing
            // event without an unrelated unwrap becoming its detector.
            let Ok(name) = name else { continue };
            record(see, site, name.encoding() == encoding)?;
            record(see, site, name.as_bytes() == bytes)?;
            record(see, site, name.case_fold_key() == bytes)?;
            let length = u32::try_from(bytes.len()).unwrap();
            record(
                see,
                site,
                new(encoding, bytes.clone(), length).is_ok_and(|other| other == name),
            )?;
            record(see, site, new(encoding, bytes.clone(), length - 1).is_err())?;
            for unit in [0, u16::from(b'/'), u16::from(b'\\')] {
                if encoding == NameEncoding::PosixBytes && unit == u16::from(b'\\') {
                    continue;
                }
                let mut invalid = bytes.clone();
                if encoding == NameEncoding::WindowsUtf16Le {
                    invalid.extend_from_slice(&unit.to_le_bytes());
                } else {
                    invalid.push(u8::try_from(unit).unwrap());
                }
                record(see, site, new(encoding, invalid, maximum).is_err())?;
            }
        }
    }
    for invalid in ["", ".", ".."] {
        record(
            see,
            "posix",
            new(
                NameEncoding::PosixBytes,
                invalid.as_bytes().to_vec(),
                maximum,
            )
            .is_err(),
        )?;
        record(
            see,
            "utf16",
            new(
                NameEncoding::WindowsUtf16Le,
                invalid.encode_utf16().flat_map(u16::to_le_bytes).collect(),
                maximum,
            )
            .is_err(),
        )?;
    }
    record(
        see,
        "utf16",
        new(NameEncoding::WindowsUtf16Le, vec![b'n'], maximum).is_err(),
    )?;
    Ok(())
}
