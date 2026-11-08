//! Provenance stays with the original fold, including its semantic failure.
use super::*;

/// What the original root read held at a pinned layer generation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LayerState {
    /// This generation did not hold the root path.
    Absent,
    /// This layer disabled copies below it.
    Off,
    /// This generation held source; only effective layers are evaluated.
    Present,
}

/// One supplied layer, in its original precedence order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LayerInput {
    /// Semantic layer position.
    pub name: String,
    /// Original pinned generation, including for absent files.
    pub generation: String,
    /// Original root observation.
    pub state: LayerState,
}

/// The actual dependency observations of one attempted layer evaluation.
#[derive(Debug)]
pub struct ModuleCut {
    /// Index into [`Composition::layers`].
    pub layer: usize,
    /// Evaluator, root, reads, base and surface identity, even on refusal.
    pub identity: [u8; 32],
    /// Original observations, beginning with this layer's root.
    pub reads: Vec<ModuleRead>,
}

/// A completed fold or semantic refusal with its original input evidence.
/// Reading this retained decision performs no I/O or evaluation. This is
/// descriptive evidence; callers retain their authenticated grant, selection
/// context and generation owners, and recheck authority before publication.
#[derive(Debug)]
pub struct Composition {
    layers: Vec<LayerInput>,
    modules: Vec<ModuleCut>,
    identity: [u8; 32],
    result: Result<(Value, Vec<Link>)>,
}

impl Composition {
    /// Every supplied generation and root presence, including disabled layers.
    #[must_use]
    pub fn layers(&self) -> &[LayerInput] {
        &self.layers
    }

    /// Only the actual evaluations, through the first semantic refusal.
    #[must_use]
    pub fn modules(&self) -> &[ModuleCut] {
        &self.modules
    }

    /// Identity of the original ordered stack and semantic evaluation cut.
    #[must_use]
    pub const fn identity(&self) -> [u8; 32] {
        self.identity
    }

    /// Borrow the result without discarding failed-evaluation evidence.
    pub fn result(&self) -> std::result::Result<&(Value, Vec<Link>), &Error> {
        self.result.as_ref()
    }

    /// Consume the original result after retaining any required evidence.
    pub fn into_result(self) -> Result<(Value, Vec<Link>)> {
        self.result
    }
}

/// The same fold as [`compose`], retaining original reads on semantic failure.
/// A typed operational tree failure remains the outer error. Layers above a
/// failing module are not loaded, and layers below the highest `off` marker
/// are not evaluated. No consumer needs to reconstruct a dependency walk.
pub fn compose_with_evidence<'a>(
    path: &'a str,
    layers: &'a [Layer<'a>],
    tree: &'a dyn Tree,
    base: Value,
) -> acyclic_stream::BoxProviderFuture<'a, Result<Composition>> {
    acyclic_stream::erase_provider_future(|| compose_inner(path, layers, tree, base))
}

pub(super) async fn compose_inner(
    path: &str,
    layers: &[Layer<'_>],
    tree: &dyn Tree,
    base: Value,
) -> Result<Composition> {
    let mut evidence = Evidence::new(path, layers);
    let result = fold(path, layers, tree, base, Some(&mut evidence)).await?;
    // With no effective module there is no evaluation key to bind the base.
    if evidence.modules.is_empty() {
        evidence.identity.update(&[0]);
        if let Ok((value, _)) = &result {
            evidence
                .identity
                .update(&crate::contract::canonical_json_bytes(value)?);
        }
    } else {
        evidence.identity.update(&[1]);
    }
    Ok(Composition {
        layers: evidence.layers,
        modules: evidence.modules,
        identity: evidence.identity.finalize().into(),
        result,
    })
}

pub(super) async fn compose_plain(
    path: &str,
    layers: &[Layer<'_>],
    tree: &dyn Tree,
    base: Value,
) -> Result<(Value, Vec<Link>)> {
    fold(path, layers, tree, base, None).await?
}

struct Evidence {
    layers: Vec<LayerInput>,
    modules: Vec<ModuleCut>,
    identity: blake3::Hasher,
}

impl Evidence {
    fn new(path: &str, layers: &[Layer<'_>]) -> Self {
        let mut identity = blake3::Hasher::new_derive_key("acyclic.harness.config.composition.v1");
        identity.update(VERSION.as_bytes());
        identity.update(&(path.len() as u64).to_be_bytes());
        identity.update(path.as_bytes());
        identity.update(&crate::layers::stack_digest(
            layers
                .iter()
                .map(|layer| (layer.name, layer.generation.as_bytes())),
        ));
        let inputs = layers
            .iter()
            .map(|layer| {
                let (state, tag) = match layer.bytes {
                    None => (LayerState::Absent, 0),
                    Some(bytes) if crate::layers::is_off(bytes) => (LayerState::Off, 1),
                    Some(_) => (LayerState::Present, 2),
                };
                identity.update(&[tag]);
                LayerInput {
                    name: layer.name.to_owned(),
                    generation: layer.generation.to_owned(),
                    state,
                }
            })
            .collect();
        Self {
            layers: inputs,
            modules: Vec::new(),
            identity,
        }
    }
}

// One fold owns precedence, evaluation and typed-error ordering. Existing
// callers do not allocate a composition evidence buffer they will discard.
async fn fold(
    path: &str,
    layers: &[Layer<'_>],
    tree: &dyn Tree,
    base: Value,
    mut evidence: Option<&mut Evidence>,
) -> Result<Result<(Value, Vec<Link>)>> {
    let mut value = base;
    let mut chain = Vec::new();
    // `held` remains the single implementation of precedence and off cuts.
    for (index, layer) in crate::layers::held_indexed(layers) {
        let Some(source) = layer.bytes else { continue };
        let prepared = prepared::prepare_inner(path, source, tree).await?;
        let decision = prepared.evaluate(&value, Surface::Compose)?;
        if let Some(evidence) = evidence.as_deref_mut() {
            evidence.identity.update(&decision.identity());
            evidence.modules.push(ModuleCut {
                layer: index,
                identity: decision.identity(),
                reads: prepared.into_reads(),
            });
        }
        match decision.into_result() {
            Ok(evaluated) => {
                chain.push(Link {
                    layer: layer.name.to_owned(),
                    generation: layer.generation.to_owned(),
                    module_digest: blake3::Hash::from_bytes(evaluated.digest)
                        .to_hex()
                        .to_string(),
                    form: evaluated.form,
                });
                value = evaluated.value;
            }
            Err(error) => {
                return Ok(Err(error));
            }
        }
    }
    Ok(Ok((value, chain)))
}
