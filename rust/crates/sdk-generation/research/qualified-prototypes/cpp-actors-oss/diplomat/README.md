# Diplomat 0.16.1 typed Actors prototype

This isolated crate exercises the maintained Diplomat C++ backend against the
actual `acyclic-actors` semantic domain. It does not add annotations to the
Actors production crate. Every opaque value stores a canonical
`acyclic_actors::domain` value; constructors call the domain constructors and
accessors read those values directly. No C++ request, response, or error
record mirrors the Rust fields.

The prototype covers caller supplied create and invoke requests. Create uses
the exact digest, positive limits, binding and subscription builders, order,
and idempotency fields. Invoke accepts caller supplied method, URL, arbitrary
body bytes, and ordered headers. `InvokeOutput` preserves response absence,
status, body bytes, and ordered header names and values. `ActorsError` keeps a
typed error kind and the canonical Rust display message.

The generated C++ headers, static library, positive consumer, and negative
strong type compile receipt are staged outside the source tree at
`Q:\cpp-actors-diplomat-package`. The exact run is recorded in
`qualification-receipt.txt`. Q: is used for all target and package output to
avoid consuming the system volume.

This is a qualification prototype rather than a production SDK cutover.
Foundation-port still needs to select the production annotation location and
metadata ownership, then add the remaining six request families and transport
operations from the same semantic source. The current source intentionally
does not claim cancellation or authenticated transport through Diplomat.
