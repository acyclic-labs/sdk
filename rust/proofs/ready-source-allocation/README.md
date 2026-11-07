# Ready records reuse their source registration

The existing allocation oracle appends 32 records through `MemoryStream`, reads
the committed records, registers them with the production `ReadySources`, and
observes each actual ready-record delivery through the physical allocator.
Every delivery must preserve the original record, sequence and order while
allocating zero heap objects. Provider reads and registration occur before
measurement.

Run `node rust/proofs/qualify.mjs ready-source-allocation` from the repository
root. The retained mutation inserts a physical allocation into the production
ready-record return branch. The unchanged oracle must fail with `ready record
scheduling allocated physical heap storage`. The shared qualifier restores exact
source bytes and requires the same test to pass. Compilation errors, empty
selections and timeouts do not qualify the negative control.

This replaces the test's former `SelectAll` scheduler arm with a mutation of the
actual operation. It qualifies allocation-free scheduling of these committed
records; it does not prove population-independent queue lookup, wake draining,
provider I/O or all stream operation costs. Existing source-cancellation and
producer-loss controls remain separate.
