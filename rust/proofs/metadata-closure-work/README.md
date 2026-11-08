# Metadata closure work refinement

Run `node rust/proofs/qualify.mjs metadata-closure-work`.

The existing metadata reader oracle now creates and commits two real public filesystem generations with identical topology, object classes and file content. Both have distinct file metadata from their directory. One contains only the original UID; the other adds the other scalar fields. It records both original canonical decoder traces and requires the public generation-closure proof's copy-work delta to equal their exact delta. This checks the production proof against the production decoder without restating field sizes or counters. The original detached-read credit-boundary checks remain in the same oracle.

The retained production mutation restores the original closure reader from aca113aac6501b758db5016099639b048465f858: one fixed item charge followed by a decoder that discards all observations. The unchanged oracle must reject its missing metadata copy delta. The qualifier restores exact original bytes and requires the same oracle to pass. The earlier reader mutation remains independently retained under metadata-reader-work.

The closure uses its existing ProofMeter::decode_steps and original allocation custody. No second meter, decoder, read, copy, allocation, syscall, durability barrier, round trip, task, timer or wait is introduced. One object read and one fixed twelve-field decode remain; primitive copy work is now admitted and recorded. Publication policy revision reads also use the one canonical metadata reader, removing duplicate scratch ownership. Existing decoder_work fuzz coverage and corpus remain unchanged.

Census: `rg -n -A 5 -B 2 'decode_file_metadata\(' rust/crates/filesystem/src --glob '*.rs'`. The bounded ordinary, merge, directory, publication and closure readers all preserve decoder observations. The conflict metadata byte import/export helpers have explicitly unbounded work boundaries and return no bounded work receipt; they are outside this bounded-reader refusal class. Test/fuzz canonical re-encodings intentionally observe separately from the charged production pass.
