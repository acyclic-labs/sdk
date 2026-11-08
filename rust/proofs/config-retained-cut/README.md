# Retained configuration observations

Run `node rust/proofs/qualify.mjs config-retained-cut`.

The existing `config_evaluation` fuzz target and its generated production
evaluator oracle now also call the public `config::prepare` boundary. An
unchanged root imports generated JSON through the original loader. Absence,
malformed bytes and admitted bytes must retain distinct observations and
decision identities, including semantic errors. Changed base, surface and
root must change the identity. Reusing the retained scripts must preserve the
same value or error without another QuickJS entry. The existing oracle still
checks raw TypeScript/JavaScript/JSON, dependency graphs, cycles, layered
composition, cache replacement and independent expected values.

The first mutation discards only an absent import's original read. The second
discards original module byte digests from the decision identity. Each must
fail the unchanged oracle before exact source restoration passes it.

This cut describes evaluation inputs. It does not authenticate a caller,
recheck current policy, select a new filesystem generation or publish a
receipt. Its caller must retain those original owners. Preparation keeps the
existing dependency walk and borrowed source parsing; retaining a cut adds
one path/digest observation per distinct original module read, with no copy
of module source bytes. Reusing it performs no tree reads or module parsing.
Canonicalizing a new base and encoding the returned value remain proportional
to their input and output sizes; no constant-time claim covers that work.
