//! W3C PROV-O in RDF N-Triples, streamed from the existing metrics projection.
//!
//! Identifiers are domain-separated and source-derived, so independent shards
//! can be concatenated. Duplicate RDF triples have set semantics; no run-wide
//! deduplication table, cross-page join or inferred timestamp is needed.
//! See <https://www.w3.org/TR/prov-o/> and <https://www.w3.org/TR/n-triples/>.

use super::{Batch, Table, export_error};
use crate::Result;
use arrow_array::{Array, StringArray, UInt32Array, UInt64Array};
use oxrdf::{
    LiteralRef, NamedNodeRef, TermRef, TripleRef,
    vocab::{rdf, xsd},
};
use oxttl::{NTriplesSerializer, ntriples::WriterNTriplesSerializer};
use std::{fmt::Write as _, io::Write};

const NS: &str = "urn:acyclic:research:";
type Id = arrayvec::ArrayString<{ NS.len() + 64 }>;

fn id(domain: &str, parts: &[&[u8]]) -> Id {
    let mut hash = blake3::Hasher::new_derive_key("acyclic.research.provenance.v1");
    for part in std::iter::once(domain.as_bytes()).chain(parts.iter().copied()) {
        hash.update(&(part.len() as u64).to_le_bytes());
        hash.update(part);
    }
    let mut iri = Id::new();
    // Both lengths are fixed, exactly the identifier's inline capacity.
    iri.push_str(NS);
    iri.push_str(hash.finalize().to_hex().as_str());
    iri
}

fn node(id: &Id) -> NamedNodeRef<'_> {
    // The only constructor is the fixed namespace followed by lowercase hex.
    NamedNodeRef::new_unchecked(id.as_str())
}

struct Output<W: Write>(WriterNTriplesSerializer<W>);

impl<W: Write> Output<W> {
    fn triple(&mut self, subject: &Id, predicate: &'static str, object: TermRef<'_>) -> Result<()> {
        self.0
            .serialize_triple(TripleRef::new(
                node(subject),
                NamedNodeRef::new_unchecked(predicate),
                object,
            ))
            .map_err(export_error)
    }

    fn relation(&mut self, subject: &Id, predicate: &'static str, object: &Id) -> Result<()> {
        self.triple(subject, predicate, node(object).into())
    }

    fn class(&mut self, subject: &Id, class: &'static str) -> Result<()> {
        self.triple(
            subject,
            rdf::TYPE.as_str(),
            NamedNodeRef::new_unchecked(class).into(),
        )
    }

    fn text(&mut self, subject: &Id, predicate: &'static str, text: &str) -> Result<()> {
        self.triple(
            subject,
            predicate,
            LiteralRef::new_simple_literal(text).into(),
        )
    }

    fn agent(&mut self, path: &str, kind: &str) -> Result<Id> {
        let agent = id("agent", &[kind.as_bytes(), path.as_bytes()]);
        self.class(
            &agent,
            if kind == "agent" {
                "http://www.w3.org/ns/prov#SoftwareAgent"
            } else {
                "http://www.w3.org/ns/prov#Agent"
            },
        )?;
        self.text(&agent, "urn:acyclic:research:path", path)?;
        Ok(agent)
    }

    fn endpoint(&mut self, path: &str, kind: Option<&str>) -> Result<Id> {
        match kind {
            Some(kind @ ("agent" | "user" | "host")) => self.agent(path, kind),
            _ => {
                let endpoint = id(
                    "endpoint",
                    &[kind.unwrap_or("unknown").as_bytes(), path.as_bytes()],
                );
                self.class(&endpoint, "http://www.w3.org/ns/prov#Entity")?;
                self.text(&endpoint, "urn:acyclic:research:path", path)?;
                if let Some(kind) = kind {
                    self.text(&endpoint, "urn:acyclic:research:endpointType", kind)?;
                }
                Ok(endpoint)
            }
        }
    }
}

fn column<'a, A: 'static>(batch: &'a Batch, name: &str) -> Result<&'a A> {
    batch
        .data
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref())
        .ok_or_else(|| export_error(format_args!("PROV column {name} has another type")))
}

fn text<'a>(batch: &'a Batch, name: &str, row: usize) -> Result<Option<&'a str>> {
    let values = column::<StringArray>(batch, name)?;
    Ok((!values.is_null(row)).then(|| values.value(row)))
}

fn required<'a>(batch: &'a Batch, name: &str, row: usize) -> Result<&'a str> {
    text(batch, name, row)?
        .ok_or_else(|| export_error(format_args!("PROV column {name} is unknown")))
}

/// Writes one metrics batch as PROV-O RDF, without reading source history.
///
/// Artifacts remain opaque immutable references. Missing notice producers and
/// measurements remain unknown. Merely referencing an artifact does not attest
/// its producer. Fork/message edges express observed influence,
/// never inferred delegation, authorship, file reads or model comprehension.
/// This function adds no capture, IO wait, fsync, flush or publication step;
/// publish its output only after success through the caller's volume commit.
pub fn write(batch: &Batch, writer: impl Write) -> Result<()> {
    if batch.data.schema() != batch.table.schema() {
        return Err(export_error(
            "PROV batch schema differs from metrics v1 table",
        ));
    }
    let mut out = Output(NTriplesSerializer::new().for_writer(writer));
    for row in 0..batch.data.num_rows() {
        let path = required(batch, "agent_path", row)?;
        let operation = text(batch, "operation_id", row)?;
        let stream = required(batch, "source_stream", row)?;
        let kind = required(batch, "kind", row)?;
        let sequences = column::<UInt64Array>(batch, "sequence")?;
        if sequences.is_null(row) {
            return Err(export_error("PROV observation sequence is unknown"));
        }
        let sequence = sequences.value(row).to_le_bytes();
        let agent = out.agent(path, "agent")?;
        let observation = id(
            "observation",
            &[
                path.as_bytes(),
                operation.unwrap_or("").as_bytes(),
                stream.as_bytes(),
                &sequence,
                batch.table.name().as_bytes(),
                kind.as_bytes(),
            ],
        );
        out.class(&observation, "http://www.w3.org/ns/prov#Entity")?;
        out.text(
            &observation,
            "urn:acyclic:research:table",
            batch.table.name(),
        )?;
        out.text(&observation, "urn:acyclic:research:sourceStream", stream)?;
        out.text(&observation, "urn:acyclic:research:kind", kind)?;
        let mut number = arrayvec::ArrayString::<{ u64::MAX.ilog10() as usize + 1 }>::new();
        write!(&mut number, "{}", sequences.value(row)).map_err(export_error)?;
        out.triple(
            &observation,
            "urn:acyclic:research:sequence",
            LiteralRef::new_typed_literal(&number, xsd::UNSIGNED_LONG).into(),
        )?;
        let turn = if let Some(operation) = operation {
            let turn = id("turn", &[path.as_bytes(), operation.as_bytes()]);
            out.class(&turn, "http://www.w3.org/ns/prov#Activity")?;
            out.text(&turn, "urn:acyclic:research:operationId", operation)?;
            out.relation(&turn, "http://www.w3.org/ns/prov#wasAssociatedWith", &agent)?;
            Some(turn)
        } else {
            None
        };

        let call = if matches!(batch.table, Table::ModelCalls | Table::ToolCalls) {
            let operation = operation.ok_or_else(|| export_error("PROV call has no operation"))?;
            let steps = column::<UInt32Array>(batch, "step")?;
            if steps.is_null(row) {
                return Err(export_error("PROV call has no original step"));
            }
            let step = steps.value(row).to_le_bytes();
            let (domain, discriminator, request) = if batch.table == Table::ModelCalls {
                (
                    "model-call",
                    required(batch, "purpose", row)?,
                    required(batch, "request_digest", row)?,
                )
            } else {
                ("tool-call", required(batch, "call_id", row)?, "")
            };
            let call = id(
                domain,
                &[
                    path.as_bytes(),
                    operation.as_bytes(),
                    &step,
                    discriminator.as_bytes(),
                    request.as_bytes(),
                ],
            );
            out.class(&call, "http://www.w3.org/ns/prov#Activity")?;
            out.relation(&call, "http://www.w3.org/ns/prov#wasAssociatedWith", &agent)?;
            out.relation(
                &call,
                "urn:acyclic:research:partOf",
                turn.as_ref()
                    .ok_or_else(|| export_error("PROV call has no enclosing turn"))?,
            )?;
            Some(call)
        } else {
            None
        };
        let activity = call.as_ref().or(turn.as_ref());
        if let Some(activity) = activity {
            out.relation(
                &observation,
                "http://www.w3.org/ns/prov#wasGeneratedBy",
                activity,
            )?;
        }

        if matches!(batch.table, Table::Turns | Table::ModelCalls)
            && let Some(digest) = text(batch, "request_digest", row)?
        {
            let request = id(
                if batch.table == Table::ModelCalls {
                    "model-request"
                } else {
                    "turn-request"
                },
                &[digest.as_bytes()],
            );
            out.class(&request, "http://www.w3.org/ns/prov#Entity")?;
            out.text(&request, "urn:acyclic:research:requestDigest", digest)?;
            if let Some(activity) = activity {
                out.relation(activity, "http://www.w3.org/ns/prov#used", &request)?;
            }
        }
        if matches!(
            batch.table,
            Table::Turns | Table::ToolCalls | Table::Edges | Table::Outcomes
        ) && let Some(reference) = text(batch, "artifact", row)?
        {
            let file = id("file-reference", &[reference.as_bytes()]);
            out.class(&file, "http://www.w3.org/ns/prov#Entity")?;
            out.text(&file, "urn:acyclic:research:fileReference", reference)?;
            out.relation(&observation, "urn:acyclic:research:artifact", &file)?;
            if let Some(activity) = activity {
                if batch.table == Table::ToolCalls && kind == "admitted" {
                    out.relation(activity, "http://www.w3.org/ns/prov#used", &file)?;
                }
            }
        }
        if batch.table == Table::Agents
            && let Some(profile) = text(batch, "profile_digest", row)?
        {
            let profile_id = id("profile", &[profile.as_bytes()]);
            out.class(&profile_id, "http://www.w3.org/ns/prov#Entity")?;
            out.text(&profile_id, "urn:acyclic:research:profileDigest", profile)?;
            out.relation(
                &agent,
                "http://www.w3.org/ns/prov#wasInfluencedBy",
                &profile_id,
            )?;
        }
        if batch.table == Table::Edges {
            let source = text(batch, "source", row)?
                .map(|source| out.endpoint(source, text(batch, "source_type", row)?))
                .transpose()?;
            let target = text(batch, "target", row)?
                .map(|target| out.endpoint(target, text(batch, "target_type", row)?))
                .transpose()?;
            if let Some(source) = &source {
                out.relation(&observation, "urn:acyclic:research:source", source)?;
            }
            if let Some(target) = &target {
                out.relation(&observation, "urn:acyclic:research:target", target)?;
            }
            if kind == "file_read"
                && let (Some(activity), Some(source)) = (activity, &source)
            {
                out.relation(activity, "http://www.w3.org/ns/prov#used", source)?;
            }
            if matches!(kind, "fork" | "message" | "notice" | "child_done")
                && let (Some(source), Some(target)) = (&source, &target)
            {
                out.relation(target, "http://www.w3.org/ns/prov#wasInfluencedBy", source)?;
            }
        }
    }
    out.0.finish();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::metrics::{Row, batch};
    use oxrdf::{Term, Triple};
    use proptest::prelude::*;
    use std::collections::HashSet;

    fn graph(bytes: &[u8]) -> Result<HashSet<Triple>> {
        oxttl::NTriplesParser::new()
            .for_reader(bytes)
            .map(|triple| triple.map_err(export_error))
            .collect()
    }

    proptest! {
        #[test]
        fn source_identity_and_page_cuts_preserve_the_same_graph(
            path in any::<String>(), reference in any::<String>(),
        ) {
            // Generated column values, not an external recording. Adversarial
            // strings must remain one opaque literal through the real RDF codec.
            let mut rows = Vec::new();
            for sequence in 0..2 {
                let mut row = Row::new(&path, Some(crate::OperationId::from_bytes([7; 16])),
                    sequence, "journal", if sequence == 0 { "admitted" } else { "completed" });
                row.step = Some(0);
                row.call_id = Some("generated-call".into());
                row.artifact = Some(reference.clone());
                rows.push(row);
            }
            let all = batch(Table::ToolCalls, &rows)?;
            let mut complete = Vec::new();
            write(&all, &mut complete)?;
            let complete = graph(&complete)?;
            let mut split = Vec::new();
            for row in 0..2 {
                write(&Batch { table: all.table, data: all.data.slice(row, 1) }, &mut split)?;
            }
            prop_assert_eq!(&complete, &graph(&split)?);
            prop_assert!(complete.iter().any(|triple| triple.predicate.as_str() ==
                "urn:acyclic:research:fileReference" && matches!(&triple.object,
                    Term::Literal(value) if value.value() == reference)));
            let file = id("file-reference", &[reference.as_bytes()]);
            prop_assert!(!complete.iter().any(|triple| triple.subject.as_ref() == node(&file).into()
                && triple.predicate.as_str() == "http://www.w3.org/ns/prov#wasGeneratedBy"),
                "a reused reference cannot attest a new file-generation producer");
            let mut producers = std::collections::HashMap::new();
            for triple in &complete {
                if triple.predicate.as_str() == "http://www.w3.org/ns/prov#wasGeneratedBy" {
                    if let Some(previous) = producers.insert(&triple.subject, &triple.object) {
                        prop_assert_eq!(previous, &triple.object,
                            "one observation cannot have two generating activities");
                    }
                }
            }
            // A new immutable reference must never alias the old entity, even
            // when a caller observes the same path in a later generation.
            let changed = format!("{reference}\0");
            prop_assert_ne!(file, id("file-reference", &[changed.as_bytes()]));
            prop_assert_ne!(id("agent", &[b"agent", path.as_bytes()]),
                id("agent", &[b"user", path.as_bytes()]));
            prop_assert_ne!(id("agent", &[b"user", path.as_bytes()]),
                id("agent", &[b"host", path.as_bytes()]));
            let mut read = Row::new(&path, Some(crate::OperationId::from_bytes([7; 16])),
                3, "journal", "file_read");
            read.source = Some(reference.clone());
            read.source_type = Some("file_generation");
            let mut bytes = Vec::new();
            write(&batch(Table::Edges, &[read])?, &mut bytes)?;
            let reads = graph(&bytes)?;
            let source = id("endpoint", &[b"file_generation", reference.as_bytes()]);
            prop_assert!(reads.iter().any(|triple| triple.predicate.as_str() ==
                "http://www.w3.org/ns/prov#used" && triple.object.as_ref() == node(&source).into()));
            prop_assert!(!reads.iter().any(|triple| triple.subject.as_ref() == node(&source).into()
                && triple.predicate.as_str() == "http://www.w3.org/ns/prov#wasGeneratedBy"));
        }
    }
}
