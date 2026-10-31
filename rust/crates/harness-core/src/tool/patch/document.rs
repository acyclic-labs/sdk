//! Codex's multi-file envelope; all update hunks use the original patch engine.
use super::{charge, invalid, reserve_vec};
use crate::{NativeCustody, Result};

/// One borrowed operation from a completely validated patch envelope.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FileChange<'a> {
    /// Each body line starts with `+`; remove that one byte when publishing.
    Add { path: &'a str, body: &'a str },
    /// Remove the exact selected source generation.
    Delete { path: &'a str },
    /// Exact update hunks, optionally followed by publication at another path.
    Update {
        path: &'a str,
        move_to: Option<&'a str>,
        diff: &'a str,
    },
}

fn header(line: &str) -> bool {
    line.starts_with("*** Add File: ")
        || line.starts_with("*** Delete File: ")
        || line.starts_with("*** Update File: ")
        || line == "*** End Patch\n"
        || line == "*** End Patch"
}

fn filename<'a>(line: &'a str, prefix: &str, meter: &NativeCustody) -> Result<&'a str> {
    line.strip_prefix(prefix)
        .and_then(|line| line.strip_suffix('\n'))
        .filter(|path| !path.is_empty())
        .ok_or_else(|| invalid(meter, "patch requires a nonempty filename and LF"))
}

/// Parses the pinned Codex grammar before any write. Descriptors borrow the
/// input; the sole allocation holds one descriptor per selected file. Path
/// authorization and immutable source selection belong to the original writer.
pub fn parse_document<'a>(
    patch: &'a str,
    meter: &mut NativeCustody,
) -> Result<Vec<FileChange<'a>>> {
    charge(meter, 0, patch.len() as u64)?;
    let mut lines = patch.split_inclusive('\n').peekable();
    if lines.next() != Some("*** Begin Patch\n") {
        return Err(invalid(meter, "patch must begin with *** Begin Patch"));
    }
    let mut changes = Vec::new();
    let mut offset = "*** Begin Patch\n".len();
    while let Some(line) = lines.next() {
        offset += line.len();
        if matches!(line, "*** End Patch" | "*** End Patch\n") {
            return if changes.is_empty() || lines.next().is_some() {
                Err(invalid(
                    meter,
                    "patch requires changes and a final *** End Patch",
                ))
            } else {
                Ok(changes)
            };
        }
        let change = if line.starts_with("*** Add File: ") {
            let path = filename(line, "*** Add File: ", meter)?;
            let start = offset;
            while let Some(&line) = lines.peek() {
                if header(line) {
                    break;
                }
                if !line.starts_with('+') || !line.ends_with('\n') {
                    return Err(invalid(
                        meter,
                        "added file lines must start with + and end with LF",
                    ));
                }
                lines.next();
                offset += line.len();
            }
            if start == offset {
                return Err(invalid(meter, "added file requires content lines"));
            }
            FileChange::Add {
                path,
                body: &patch[start..offset],
            }
        } else if line.starts_with("*** Delete File: ") {
            FileChange::Delete {
                path: filename(line, "*** Delete File: ", meter)?,
            }
        } else if line.starts_with("*** Update File: ") {
            let path = filename(line, "*** Update File: ", meter)?;
            let move_to = if let Some(&line) = lines
                .peek()
                .filter(|line| line.starts_with("*** Move to: "))
            {
                let destination = filename(line, "*** Move to: ", meter)?;
                lines.next();
                offset += line.len();
                Some(destination)
            } else {
                None
            };
            let start = offset;
            let mut at_end = false;
            while let Some(&line) = lines.peek() {
                if header(line) {
                    break;
                }
                if at_end || !line.ends_with('\n') {
                    return Err(invalid(meter, "invalid patch change after end of file"));
                }
                if line == "*** End of File\n" {
                    if start == offset {
                        return Err(invalid(meter, "patch end of file requires a change"));
                    }
                    at_end = true;
                } else if !matches!(line.as_bytes().first(), Some(b' ' | b'+' | b'-'))
                    && line != "@@\n"
                    && !line.starts_with("@@ ")
                {
                    return Err(invalid(meter, "invalid patch update line"));
                }
                if line == "@@ \n" {
                    return Err(invalid(meter, "patch anchor is empty"));
                }
                lines.next();
                offset += line.len();
            }
            FileChange::Update {
                path,
                move_to,
                diff: &patch[start..offset],
            }
        } else {
            return Err(invalid(
                meter,
                "patch requires an add, delete or update header",
            ));
        };
        let length = changes.len() + 1;
        reserve_vec(meter, &mut changes, length)?;
        changes.push(change);
    }
    Err(invalid(meter, "patch must end with *** End Patch"))
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        #[test]
        fn envelope_composes_the_original_exact_update_engine(old in "[a-zλ🦀]{1,24}", new in "[A-Zλ🦀]{0,24}") {
            let patch = format!("*** Begin Patch\n*** Add File: new.txt\n+{new}\n*** Update File: existing.txt\n*** Move to: moved.txt\n-{old}\n+{new}\n*** Delete File: gone.txt\n*** End Patch\n");
            let mut meter = None;
            let changes = parse_document(&patch, &mut meter).unwrap();
            let FileChange::Update {diff, ..} = changes[1] else { unreachable!() };
            let actual = super::super::apply_update(&format!("{old}\n"), diff, super::super::PatchLimits {
                maximum_bytes: 4096, maximum_work: 4096, maximum_hunks: 4,
            }, &mut meter).unwrap();
            prop_assert_eq!(actual, format!("{new}\n"));
            // Omit the final delimiter: no prefix of a multi-file document may
            // be admitted as a complete batch.
            prop_assert!(parse_document(patch.strip_suffix("*** End Patch\n").unwrap(), &mut meter).is_err());
            // The original engine rejects wrong context; an envelope does not
            // introduce a second, more permissive update implementation.
            prop_assert!(super::super::apply_update("# unrelated\n", diff, super::super::PatchLimits {
                maximum_bytes: 4096, maximum_work: 4096, maximum_hunks: 4,
            }, &mut meter).is_err());
        }
    }

    #[test]
    fn invalid_later_changes_refuse_the_entire_document() {
        for suffix in [
            "*** Add File: bad\nnot-prefixed\n*** End Patch\n",
            "*** Delete File: \n*** End Patch\n",
            "*** Update File: bad\n@@ \n-x\n+y\n*** End Patch\n",
            "*** Update File: bad\n@@\n-x\n+y\n*** End of File\n+later\n*** End Patch\n",
            "*** End Patch\ntrailing\n",
            "*** Delete File: bad",
        ] {
            let patch = format!("*** Begin Patch\n*** Add File: good\n+valid\n{suffix}");
            assert!(parse_document(&patch, &mut None).is_err());
        }
        let patch = "*** Begin Patch\n*** Update File: old\n*** Move to: new\n*** End Patch";
        assert!(matches!(
            &parse_document(patch, &mut None).unwrap()[0],
            FileChange::Update {
                path: "old",
                move_to: Some("new"),
                diff: ""
            }
        ));
    }
}
