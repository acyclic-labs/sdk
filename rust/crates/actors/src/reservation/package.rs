use super::{Book, decode, encode, invalid};
use crate::{
    domain::{ActorLimits, CodeSha256},
    guest::{
        CancellationToken, Checkpoint, CodecIdentity, Delivery, GuestFuture, Handler, Manifest,
        Transition,
    },
};

/// The reservation package's canonical checkpoint format.
pub fn codec() -> CodecIdentity {
    CodecIdentity {
        name: "acyclic.actors.reservations".into(),
        version: "1".into(),
    }
}

/// Initial explicit capacity for Store::create's original companion checkpoint.
pub fn initial_checkpoint(capacity: u64) -> Result<Checkpoint, crate::guest::GuestError> {
    Ok(Checkpoint {
        codec: codec(),
        bytes: encode(&Book::new(capacity)?)?,
    })
}

/// Production reservation Handler for any native or strict-wire hosted actor.
/// An artifact can instead embed the same Book in its own checkpoint/codec.
pub struct Package {
    manifest: Manifest,
}

impl Package {
    /// Admit the original artifact identity and explicit execution/checkpoint
    /// limits. Each actor binds `inbox` and its capacity when it is created.
    pub fn new(code: CodeSha256, limits: ActorLimits) -> Self {
        Self {
            manifest: Manifest {
                code,
                codec: codec(),
                subscriptions: vec!["inbox".into()],
                interrupts: Vec::new(),
                bindings: Vec::new(),
                limits,
            },
        }
    }
}

impl Handler for Package {
    fn manifest(&self) -> &Manifest {
        &self.manifest
    }
    fn on_record<'a>(
        &'a self,
        delivery: Delivery,
        checkpoint: Option<&'a Checkpoint>,
        _: CancellationToken,
    ) -> GuestFuture<'a, Transition> {
        Box::pin(async move {
            let checkpoint = checkpoint
                .filter(|checkpoint| checkpoint.codec == codec())
                .ok_or_else(invalid)?;
            let mut book: Book = decode(&checkpoint.bytes)?;
            book.validate()?;
            let step = book.step(&delivery)?;
            Ok(Transition {
                response: step.response,
                checkpoint: Checkpoint {
                    codec: codec(),
                    bytes: encode(&book)?,
                },
                alarm: step.alarm,
                effects: Vec::new(),
                participant: None,
            })
        })
    }
}
