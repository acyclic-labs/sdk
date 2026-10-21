use acyclic_harness_core::{
    Result,
    model::{ModelContentPart, ModelRequest},
};
use std::borrow::Cow;

// N-05, ANT-TOOL-01, OAR-TOOL-01, OAC-TOOL-03, GEM-TOOL-01.
// A provider-invalid canonical name gets an alias derived from the name alone,
// so it never depends on its position among the request's tools: `.` becomes
// `__` (`mcp.server.tool` -> `mcp__server__tool`) and any other unsupported
// byte becomes `_`. An alias that is too long, or that is already taken,
// instead keeps a readable prefix and appends a salted digest of the
// canonical name. Every retained alias is allocated at its exact size.
const DIGEST_HEX: usize = 16;
const MAXIMUM: usize = crate::protocol::name::TOOL.maximum;
const DIGEST_DOMAIN: &[u8] = b"acyclic:tool-name:v1\0";

struct Binding<'a> {
    canonical: &'a str,
    wire: Cow<'a, str>,
}

/// An alias whose first candidate was taken, and its latest salt.
struct Resalted {
    index: usize,
    salt: u32,
    alias: String,
}

/// Work done while building a table, compared exactly with its native claim.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Built {
    /// Heap allocations, retained and temporary.
    pub(crate) allocations: u64,
    /// Heap capacity allocated, retained and temporary.
    pub(crate) bytes: u64,
    /// Candidate names checked against occupied names.
    pub(crate) probes: u64,
    pub(crate) hashed: u64,
    pub(crate) wire_sorts: u64,
    pub(crate) wire_comparisons: u64,
}

impl Built {
    pub(crate) fn within(&self, claim: &Self) -> bool {
        self.allocations <= claim.allocations
            && self.bytes <= claim.bytes
            && self.probes <= claim.probes
    }
}

/// Bindings sorted by canonical name, plus their indices sorted by wire name.
pub(crate) struct Names<'a> {
    bindings: Vec<Binding<'a>>,
    by_wire: Vec<usize>,
}

fn names(request: &ModelRequest) -> impl Iterator<Item = &str> {
    request.tools.iter().map(|tool| tool.name.as_str()).chain(
        request
            .messages
            .iter()
            .flat_map(|message| message.content.parts())
            .filter_map(|part| match part {
                ModelContentPart::ToolCall(call) => Some(call.name.as_str()),
                ModelContentPart::ToolResult { name, .. } => Some(name.as_str()),
                _ => None,
            }),
    )
}

fn unchanged(name: &str) -> bool {
    crate::protocol::name::TOOL.accepts(name)
}

fn needs_prefix(name: &str) -> bool {
    !name
        .as_bytes()
        .first()
        .is_some_and(|byte| byte.is_ascii_alphabetic() || *byte == b'_')
}

/// The untruncated readable alias length, saturating just past the maximum.
fn readable_len(name: &str) -> usize {
    let mut length = usize::from(needs_prefix(name));
    for byte in name.bytes() {
        if length > MAXIMUM {
            break;
        }
        length += if byte == b'.' { 2 } else { 1 };
    }
    length
}

/// Appends the readable alias, cut at `length` bytes; never reallocates when
/// `alias` has room for `length` more bytes.
fn push_readable(alias: &mut String, name: &str, length: usize) {
    let end = alias.len() + length;
    let mut push = |character: char| {
        if alias.len() < end {
            alias.push(character);
        }
    };
    if needs_prefix(name) {
        push('_');
    }
    for byte in name.bytes() {
        match byte {
            b'.' => {
                push('_');
                push('_');
            }
            byte if byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-') => {
                push(char::from(byte));
            }
            _ => push('_'),
        }
    }
}

/// The readable alias at its exact size, if it fits the provider maximum.
fn readable(name: &str) -> Option<String> {
    let length = readable_len(name);
    (length <= MAXIMUM).then(|| {
        let mut alias = String::with_capacity(length);
        push_readable(&mut alias, name, length);
        alias
    })
}

/// Writes the salted digest alias into `buffer`, which holds `MAXIMUM` bytes.
fn digest_into(
    buffer: &mut String,
    name: &str,
    salt: u32,
    built: &mut Built,
    fund: &mut impl FnMut(u64) -> Result<()>,
) -> Result<()> {
    let hashed = (DIGEST_DOMAIN.len() + size_of::<u32>() + name.len()) as u64;
    #[cfg(test)]
    #[cfg(not(target_arch = "wasm32"))]
    let skip = tests::SKIP_HASH_ADMISSION.with(std::cell::Cell::get);
    #[cfg(not(all(test, not(target_arch = "wasm32"))))]
    let skip = false;
    if !skip {
        fund(hashed)?;
    }
    #[cfg(test)]
    #[cfg(not(target_arch = "wasm32"))]
    tests::ACTUAL_HASHED.with(|counter| counter.set(counter.get() + hashed));
    built.hashed += hashed;
    buffer.clear();
    push_readable(
        buffer,
        name,
        readable_len(name).min(MAXIMUM - DIGEST_HEX - 1),
    );
    let mut hasher = blake3::Hasher::new();
    hasher.update(DIGEST_DOMAIN);
    hasher.update(&salt.to_le_bytes());
    hasher.update(name.as_bytes());
    buffer.push('_');
    buffer.push_str(&hasher.finalize().to_hex()[..DIGEST_HEX]);
    Ok(())
}

/// An exactly sized copy, so retained capacity equals the alias length.
fn exact(alias: &str) -> String {
    let mut owned = String::with_capacity(alias.len());
    owned.push_str(alias);
    owned
}

impl<'a> Names<'a> {
    pub(crate) fn controls(
        &self,
        adapter: crate::Adapter,
        options: &mut serde_json::Value,
    ) -> Result<()> {
        let pointer = match adapter {
            crate::Adapter::Anthropic | crate::Adapter::Responses => "/tool_choice/name",
            crate::Adapter::Chat => "/tool_choice/function/name",
            crate::Adapter::Gemini => "/toolConfig/functionCallingConfig/allowedFunctionNames",
        };
        if let Some(value) = options.pointer_mut(pointer) {
            if adapter == crate::Adapter::Gemini {
                for name in value
                    .as_array_mut()
                    .ok_or_else(|| crate::invalid("allowed tool names must be an array"))?
                {
                    self.control_name(name)?;
                }
            } else {
                self.control_name(value)?;
            }
        }
        if matches!(adapter, crate::Adapter::Responses | crate::Adapter::Chat) {
            if let Some(tools) = options.pointer_mut("/tool_choice/tools") {
                for tool in tools
                    .as_array_mut()
                    .ok_or_else(|| crate::invalid("allowed tools must be an array"))?
                {
                    let pointer = if adapter == crate::Adapter::Chat {
                        "/function/name"
                    } else {
                        "/name"
                    };
                    if let Some(name) = tool.pointer_mut(pointer) {
                        self.control_name(name)?;
                    }
                }
            }
        }
        Ok(())
    }

    fn control_name(&self, value: &mut serde_json::Value) -> Result<()> {
        let name = value
            .as_str()
            .ok_or_else(|| crate::invalid("selected tool name must be a string"))?;
        let alias = self.wire(name)?;
        if alias != name {
            *value = alias.into_owned().into();
        }
        Ok(())
    }

    #[cfg(all(not(target_arch = "wasm32"), any(test, feature = "fuzzing")))]
    pub(crate) fn assert_round_trip(request: &'a ModelRequest) {
        let table = Self::new(request);
        for adapter in [
            crate::Adapter::Anthropic,
            crate::Adapter::Responses,
            crate::Adapter::Chat,
            crate::Adapter::Gemini,
        ] {
            let mut options = request.model.options.clone();
            let _ = table.controls(adapter, &mut options);
        }
        let mut seen = std::collections::BTreeSet::new();
        for name in table.bindings.iter().map(|binding| binding.canonical) {
            let wire = table.wire(name).unwrap();
            assert!(!wire.is_empty() && wire.len() <= 64);
            assert!(wire.as_bytes()[0].is_ascii_alphabetic() || wire.starts_with('_'));
            assert!(
                wire.bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
            );
            assert!(
                seen.insert(wire.to_string()),
                "distinct canonical names collided"
            );
            let mut event = acyclic_harness_core::model::ModelEvent::ToolCall(
                acyclic_harness_core::model::ToolCall::new(
                    "call".into(),
                    wire.into_owned(),
                    serde_json::json!({}),
                ),
            );
            table.restore(&mut event).unwrap();
            let acyclic_harness_core::model::ModelEvent::ToolCall(call) = event else {
                unreachable!()
            };
            assert_eq!(call.name, name);
            let _ = table.canonical(name); // Also exercise malformed/untrusted alias strings.
        }
    }

    pub(crate) fn new(request: &'a ModelRequest) -> Self {
        Self::build(request, names(request).count(), &mut |_| Ok(()))
            .expect("unfunded tool-name construction cannot deny admission")
            .0
    }

    /// The native claim for a table over `count` names. Bytes: every binding,
    /// the wire index, the re-salt list, the candidate buffer, and two
    /// exactly sized aliases per name (a first alias and its replacement).
    /// Probes: one per re-salted name plus one per declared name equal to one
    /// of its candidates, so at most twice the name count.
    pub(crate) fn claim(count: u64) -> Option<Built> {
        if count == 0 {
            return Some(Built::default());
        }
        let per_name = (std::mem::size_of::<Binding<'_>>()
            + std::mem::size_of::<usize>()
            + std::mem::size_of::<Resalted>()
            + 2 * MAXIMUM) as u64;
        Some(Built {
            allocations: count.checked_mul(2)?.checked_add(4)?,
            bytes: count.checked_mul(per_name)?.checked_add(MAXIMUM as u64)?,
            probes: count.checked_mul(2)?,
            ..Built::default()
        })
    }

    #[cfg(test)]
    pub(crate) fn built(request: &'a ModelRequest) -> (Self, Built) {
        Self::build(request, names(request).count(), &mut |_| Ok(())).unwrap()
    }

    /// Each retained alias's length and capacity.
    #[cfg(test)]
    pub(crate) fn aliases(&self) -> impl Iterator<Item = (usize, usize)> + '_ {
        self.bindings
            .iter()
            .filter_map(|binding| match &binding.wire {
                Cow::Owned(alias) => Some((alias.len(), alias.capacity())),
                Cow::Borrowed(_) => None,
            })
    }

    /// The digest candidate for `name` at `salt`.
    #[cfg(test)]
    pub(crate) fn candidate(name: &str, salt: u32) -> String {
        let mut buffer = String::with_capacity(MAXIMUM);
        digest_into(&mut buffer, name, salt, &mut Built::default(), &mut |_| {
            Ok(())
        })
        .unwrap();
        buffer
    }

    fn build(
        request: &'a ModelRequest,
        count: usize,
        fund: &mut impl FnMut(u64) -> Result<()>,
    ) -> Result<(Self, Built)> {
        let mut built = Built::default();
        if count == 0 {
            return Ok((
                Self {
                    bindings: Vec::new(),
                    by_wire: Vec::new(),
                },
                built,
            ));
        }
        fn charge(built: &mut Built, bytes: usize) {
            built.allocations += 1;
            built.bytes += bytes as u64;
        }
        let mut bindings = Vec::with_capacity(count);
        charge(&mut built, count * std::mem::size_of::<Binding<'_>>());
        bindings.extend(names(request).map(|canonical| Binding {
            canonical,
            wire: Cow::Borrowed(canonical),
        }));
        bindings.sort_unstable_by(|a, b| a.canonical.cmp(b.canonical));
        bindings.dedup_by(|a, b| a.canonical == b.canonical);
        let mut buffer = String::new();
        for binding in &mut bindings {
            if !unchanged(binding.canonical) {
                let alias = if let Some(alias) = readable(binding.canonical) {
                    alias
                } else {
                    if buffer.capacity() == 0 {
                        buffer.reserve_exact(MAXIMUM);
                        charge(&mut built, MAXIMUM);
                    }
                    digest_into(&mut buffer, binding.canonical, 0, &mut built, fund)?;
                    exact(&buffer)
                };
                charge(&mut built, alias.capacity());
                binding.wire = Cow::Owned(alias);
            }
        }
        // One sort by wire: a declared name sorts ahead of an equal alias, and
        // the first alias in canonical order keeps a shared candidate.
        let order = |bindings: &[Binding<'_>], a: usize, b: usize| {
            let (a, b) = (&bindings[a], &bindings[b]);
            a.wire
                .cmp(&b.wire)
                .then_with(|| unchanged(b.canonical).cmp(&unchanged(a.canonical)))
                .then(a.canonical.cmp(b.canonical))
        };
        let mut by_wire = Vec::with_capacity(bindings.len());
        charge(&mut built, bindings.len() * std::mem::size_of::<usize>());
        by_wire.extend(0..bindings.len());
        sort_wire(&mut by_wire, &bindings, &mut built, order);
        let mut resalted: Vec<Resalted> = Vec::new();
        for pair in by_wire.windows(2) {
            if bindings[pair[0]].wire == bindings[pair[1]].wire {
                if resalted.capacity() == 0 {
                    resalted.reserve_exact(bindings.len());
                    charge(&mut built, bindings.len() * std::mem::size_of::<Resalted>());
                }
                resalted.push(Resalted {
                    index: pair[1],
                    salt: 0,
                    alias: String::new(),
                });
            }
        }
        if !resalted.is_empty() && buffer.capacity() == 0 {
            buffer.reserve_exact(MAXIMUM);
            charge(&mut built, MAXIMUM);
        }
        // A candidate is taken only by an original wire equal to it: an
        // unchanged declared name, a kept alias, or a first alias still
        // awaiting replacement. Distinct names' digests differ unless the
        // digest itself collides, which the repeat pass below repairs.
        loop {
            for entry in resalted.iter_mut().filter(|entry| entry.alias.is_empty()) {
                let canonical = bindings[entry.index].canonical;
                loop {
                    entry.salt += 1;
                    digest_into(&mut buffer, canonical, entry.salt, &mut built, fund)?;
                    built.probes += 1;
                    let taken = by_wire
                        .binary_search_by(|&index| bindings[index].wire.as_ref().cmp(&buffer))
                        .is_ok();
                    if !taken {
                        entry.alias = exact(&buffer);
                        charge(&mut built, entry.alias.capacity());
                        break;
                    }
                }
            }
            resalted.sort_unstable_by(|a, b| a.alias.cmp(&b.alias));
            let mut repeated = false;
            for index in 1..resalted.len() {
                if resalted[index].alias == resalted[index - 1].alias {
                    resalted[index].alias = String::new();
                    repeated = true;
                }
            }
            if !repeated {
                break;
            }
        }
        let changed = !resalted.is_empty();
        for entry in resalted {
            bindings[entry.index].wire = Cow::Owned(entry.alias);
        }
        #[cfg(test)]
        #[cfg(not(target_arch = "wasm32"))]
        let changed = changed || tests::REDUNDANT_WIRE_SORT.with(std::cell::Cell::get);
        if changed {
            sort_wire(&mut by_wire, &bindings, &mut built, order);
        }
        let table = Self { bindings, by_wire };
        debug_assert!(
            Self::claim(count as u64).is_some_and(|claim| built.within(&claim)),
            "tool name table exceeded its claim: {built:?}"
        );
        Ok((table, built))
    }

    /// The provider name for `name`, borrowed from the table or from `name`.
    pub(crate) fn wire<'s>(&'s self, name: &'s str) -> Result<Cow<'s, str>> {
        if unchanged(name) {
            return Ok(Cow::Borrowed(name));
        }
        let index = self
            .bindings
            .binary_search_by(|binding| binding.canonical.cmp(name))
            .map_err(|_| crate::invalid("missing tool name binding"))?;
        Ok(Cow::Borrowed(self.bindings[index].wire.as_ref()))
    }

    /// The canonical name behind a provider alias; `None` for a wire name
    /// that is itself canonical or undeclared, which tool admission validates.
    /// A name outside the provider grammar was never declared, so it rejects
    /// the response.
    pub(crate) fn canonical(&self, wire: &str) -> Result<Option<&'a str>> {
        match self
            .by_wire
            .binary_search_by(|&index| self.bindings[index].wire.as_ref().cmp(wire))
        {
            Ok(found) => {
                let binding = &self.bindings[self.by_wire[found]];
                Ok((binding.canonical != wire).then_some(binding.canonical))
            }
            Err(_) if unchanged(wire) => Ok(None),
            Err(_) => Err(crate::invalid("provider named an undeclared tool alias")),
        }
    }

    #[cfg(all(not(target_arch = "wasm32"), any(test, feature = "fuzzing")))]
    pub(crate) fn restore(
        &self,
        event: &mut acyclic_harness_core::model::ModelEvent,
    ) -> Result<()> {
        if let acyclic_harness_core::model::ModelEvent::ToolCall(call) = event {
            if let Some(name) = self.canonical(&call.name)? {
                call.name = name.into();
            }
        }
        Ok(())
    }

    pub(crate) fn funded(
        request: &'a ModelRequest,
        work: acyclic_harness_core::NativeWork<'_>,
    ) -> Result<(Self, acyclic_harness_core::NativeCustody)> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = work {
            let count = names(request).count() as u64;
            let claim = Self::claim(count)
                .ok_or_else(|| crate::invalid("tool name binding size overflow"))?;
            let custody = work.claim(
                claim.bytes,
                claim.allocations,
                acyclic_fs::WorkCounters {
                    bytes_copied: claim.bytes,
                    items_examined: count
                        .checked_add(claim.probes)
                        .ok_or_else(|| crate::invalid("tool name binding size overflow"))?,
                    ..acyclic_fs::WorkCounters::UNCHARGED
                },
            )?;
            let (table, built) = Self::build(request, count as usize, &mut |bytes_hashed| {
                work.charge(acyclic_fs::WorkCounters {
                    bytes_hashed,
                    ..acyclic_fs::WorkCounters::UNCHARGED
                })?;
                Ok(())
            })?;
            // The claim is a bound the build must respect, not an estimate.
            if !built.within(&claim) {
                return Err(crate::invalid("tool name table exceeded its native claim"));
            }
            return Ok((table, Some(custody)));
        }
        let _ = work;
        Ok((Self::new(request), None))
    }
}

fn sort_wire<'a>(
    indices: &mut [usize],
    bindings: &[Binding<'a>],
    built: &mut Built,
    order: impl Fn(&[Binding<'a>], usize, usize) -> std::cmp::Ordering,
) {
    built.wire_sorts += 1;
    indices.sort_unstable_by(|&a, &b| {
        built.wire_comparisons += 1;
        order(bindings, a, b)
    });
}

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
#[path = "tool_names_budget_tests.rs"]
mod tests;
