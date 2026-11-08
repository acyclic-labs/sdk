//! One retained loader cut, including semantic refusals and absent imports.
use super::*;

/// One original module observation, including a missing static import.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModuleRead {
    /// Resolved path in the caller's pinned tree.
    pub path: String,
    /// Digest of the original bytes, or `None` when the tree held no file.
    pub digest: Option<[u8; 32]>,
}

/// The original dependency walk, retained before evaluating a module.
/// Reusing this value never reads or parses its modules again. The caller
/// retains the pinned tree's authorization and generation; this cut does not
/// authorize later use or detect changes to a different tree.
pub struct Prepared {
    path: String,
    loaded: Loaded,
}

pub(super) struct Loaded {
    reads: Vec<ModuleRead>,
    modules: std::result::Result<Vec<(String, Module)>, String>,
    digest: [u8; 32],
}

/// A deterministic semantic decision and the exact inputs that produced it.
/// Invalid source, missing imports and runtime refusals retain their identity.
/// Operational read failures never produce this value.
#[derive(Debug)]
pub struct Evaluation {
    identity: [u8; 32],
    result: Result<Evaluated>,
}

impl Evaluation {
    /// Digest of the evaluator identity, root and original reads (including
    /// absence), canonical base and surface. This is descriptive evidence,
    /// never a principal, policy, host-selection or publication authority.
    #[must_use]
    pub const fn identity(&self) -> [u8; 32] {
        self.identity
    }

    /// Borrow the semantic decision without losing its input identity.
    pub fn result(&self) -> std::result::Result<&Evaluated, &Error> {
        self.result.as_ref()
    }

    /// Return the original typed result after retaining any required evidence.
    pub fn into_result(self) -> Result<Evaluated> {
        self.result
    }
}

/// Retain the existing loader's observations and scripts, or its semantic
/// refusal. Read failures keep their typed operational cause. Preparation
/// performs one dependency walk; no consumer needs a second collector.
pub fn prepare<'a>(
    path: &'a str,
    source: &'a [u8],
    tree: &'a dyn Tree,
) -> acyclic_stream::BoxProviderFuture<'a, Result<Prepared>> {
    acyclic_stream::erase_provider_future(|| prepare_inner(path, source, tree))
}

pub(super) async fn prepare_inner(path: &str, source: &[u8], tree: &dyn Tree) -> Result<Prepared> {
    let loaded = load(path, source, tree, true).await?;
    #[cfg(any(test, feature = "test-support"))]
    model::root_retained();
    Ok(Prepared {
        path: path.to_owned(),
        loaded,
    })
}

pub(super) async fn load(
    path: &str,
    source: &[u8],
    tree: &dyn Tree,
    retain_reads: bool,
) -> Result<Loaded> {
    let mut observations = Observations {
        reads: retain_reads.then(Vec::new),
        digest: blake3::Hasher::new_derive_key(VERSION),
    };
    let modules = closure(path, source, tree, &mut observations).await?;
    Ok(Loaded {
        reads: observations.reads.unwrap_or_default(),
        modules,
        digest: observations.digest.finalize().into(),
    })
}

// Hash each original observation once. Only reusable or evidence-retaining
// callers own its path; inline evaluation borrows its caller's root path.
struct Observations {
    reads: Option<Vec<ModuleRead>>,
    digest: blake3::Hasher,
}

impl Observations {
    fn observe(&mut self, path: &str, bytes: Option<&[u8]>) {
        let digest: Option<[u8; 32]> = bytes.map(|bytes| blake3::hash(bytes).into());
        self.digest.update(&(path.len() as u64).to_be_bytes());
        self.digest.update(path.as_bytes());
        self.digest.update(&[u8::from(digest.is_some())]);
        if let Some(digest) = digest {
            self.digest.update(&digest);
        }
        if let Some(reads) = &mut self.reads {
            #[cfg(any(test, feature = "test-support"))]
            model::read_retained();
            reads.push(ModuleRead {
                path: path.to_owned(),
                digest,
            });
        }
    }
}

impl Prepared {
    /// Original reads in loader order, starting with the supplied root.
    #[must_use]
    pub fn reads(&self) -> &[ModuleRead] {
        &self.loaded.reads
    }

    /// Evaluate this original cut. A hit in the evaluator's single retained
    /// result also avoids entering QuickJS. No I/O or module parsing occurs.
    pub fn evaluate(&self, base: &Value, surface: Surface) -> Result<Evaluation> {
        self.evaluate_cached(base, surface, cache())
    }

    pub(super) fn evaluate_cached(
        &self,
        base: &Value,
        surface: Surface,
        cache: &Cache,
    ) -> Result<Evaluation> {
        self.loaded
            .evaluate_cached(&self.path, base, surface, cache)
    }
}

impl Loaded {
    pub(super) fn into_reads(self) -> Vec<ModuleRead> {
        self.reads
    }

    pub(super) fn evaluate_cached(
        &self,
        path: &str,
        base: &Value,
        surface: Surface,
        cache: &Cache,
    ) -> Result<Evaluation> {
        let base = crate::contract::canonical_json_bytes(base)?;
        let mut key = blake3::Hasher::new_derive_key(VERSION);
        key.update(&self.digest);
        key.update(&[u8::from(surface == Surface::Data)]);
        key.update(&base);
        let key: [u8; 32] = key.finalize().into();
        let cached = cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .filter(|(cached_key, _)| cached_key == &key)
            .map(|(_, outcome)| outcome.clone());
        let outcome = match cached {
            Some(outcome) => outcome,
            None => {
                let outcome = self
                    .modules
                    .as_ref()
                    .map_err(Clone::clone)
                    .and_then(|modules| run(modules, &base, surface))
                    .map(Arc::new);
                *cache
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) =
                    Some((key, outcome.clone()));
                outcome
            }
        };
        let result = outcome
            .map(|outcome| {
                let (value, form) = outcome.as_ref().clone();
                Evaluated {
                    value,
                    form,
                    digest: self.digest,
                }
            })
            .map_err(|reason| Error::Invalid(format!("{path}: {reason}")));
        Ok(Evaluation {
            identity: key,
            result,
        })
    }
}

/// The module at `path` and everything it imports statically, each after
/// the modules it imports (the engine links a module as it is declared), so
/// the root is last. A read error is the outer error; an unusable closure is
/// the inner one.
async fn closure(
    path: &str,
    source: &[u8],
    tree: &dyn Tree,
    observations: &mut Observations,
) -> Result<std::result::Result<Vec<(String, Module)>, String>> {
    observations.observe(path, Some(source));
    let mut modules = BTreeMap::new();
    match strip(path, source) {
        Ok(module) => modules.insert(path.to_owned(), (module, true)),
        Err(error) => return Ok(Err(error)),
    };
    // The retained module records whether its dependency walk is still active.
    // A completed shared dependency is reused without scanning the active path.
    let mut stack = vec![(path.to_owned(), 0_usize)];
    let mut order = Vec::new();
    while let Some((current, index)) = stack.last_mut() {
        let Some(next) = modules[current.as_str()].0.imports.get(*index) else {
            if let Some((_, active)) = modules.get_mut(current.as_str()) {
                *active = false;
            }
            order.push(std::mem::take(current));
            stack.pop();
            continue;
        };
        *index += 1;
        #[cfg(any(test, feature = "test-support"))]
        model::dependency_checked();
        if let Some((_, active)) = modules.get(next) {
            if *active {
                return Ok(Err(format!("import cycle through {next}")));
            }
            continue;
        }
        let next = next.clone();
        let bytes = tree.read(&next).await?;
        observations.observe(&next, bytes.as_deref());
        let Some(bytes) = bytes else {
            return Ok(Err(format!(
                "{current} imports {next}, which no layer holds"
            )));
        };
        match strip(&next, &bytes) {
            Ok(module) => modules.insert(next.clone(), (module, true)),
            Err(error) => return Ok(Err(error)),
        };
        stack.push((next, 0));
    }
    Ok(Ok(order
        .into_iter()
        .filter_map(|name| modules.remove(&name).map(|(module, _)| (name, module)))
        .collect()))
}
