# Ruby current Actors generic frozen qualification — WSL x86_64 — 2026-10-07

This artifact is generated from the exact current Actors producer at Q:/sdk/work/sdkgen-actors-c8-minimal, revision 737775737a54c31c1d50b11c677ca9ae900b45b2. The maintained UniFFI 0.31 Ruby backend is rebuilt from source with the async scheduler patch and the minimal generic metadata patch. The generated package has two source-owned component files (`acyclic_actors.rb` and `acyclic_actors_uniffi.rb`) and no handwritten operation wrappers or ABI mirror.

The final regenerated WSL package passed all eight operations over the task-owned TLS fixture, nominal and static type negatives, true-only CurrentHeadMarker, readonly/frozen guards, 2^63 and u64MAX wire round trips, and a real pending InspectActor cancellation. The fixture observed `started=8`, `aborted=8`, `active=0`; Ruby raised the typed `AcyclicActorsUniffi::BindingError::Cancelled`. The installed external gem consumer also passed all eight operations.

The source coverage check is `source-coverage.ps1`; its terminal output is retained beside the script. `LICENSE-NOTICE.txt` records the maintained generator MPL-2.0 provenance and the minimal patch-only distribution boundary. Earlier Mac receipt `current-c8-generic-mac-20261007` remains historical and is not retagged by this WSL artifact.
