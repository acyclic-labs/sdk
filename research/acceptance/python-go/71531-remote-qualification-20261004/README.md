# Python remote qualification on repaired Rust revision

This receipt set records actual Python gRPC calls against the repaired fixture built from Git revision `71531baac1a03e046a961e299a81931baa1bfb0c`. The generated Rust authority model is `7e6e7733c7b5a52fe124fbdb7a7972d2897c37766a0a0b81204093a395eb72c5`, and the authority manifest is copied beside the logs.

The generated Python wheel is `acyclic_sdk_transport-0.2.0-py3-none-any.whl`, SHA-256 `19DC8E788379B6FF5624BFCE4A18398FBA31883FC542FC95C407396783BDD544`. The fixture binary used for the run is SHA-256 `B97146A2DBD9748FCBF95BA21F0F0ADC5FDCF91C26F3763937CC44AE70AFA5EB`.

The runs used the package generated from this authority and a live local gRPC fixture with `execution_mode` set to `remote` in every scenario result:

- Actors: 8/8 passed.
- Stream: 10/10 passed, including server streaming and cancellation of `Follow`.
- Harness: 5/5 passed, including handshake, submit, replay, observe, and cancel.

The scenario log hashes after recording the package label `python-71531-generated-package` are:

- `python-actors-stream-scenario-log.json`: `54864A5791E5CEA94B70B8426B76EB21CE3DBA0F2943C7EE246FA083459A775A`
- `python-harness-scenario-log.json`: `AD65E39ABA5293B40A91C09184DACEACC232B02FDD3F33FA99BD0BDEE250D719`
- `rust-authority.json`: `A150DEDCAE1F8ABFF2F6E8C5D0151B9E1B24C091A7072C3B4B2B96C0C62090FE`

`failures/pre-repair-b529-harness.json` is retained as negative evidence from the earlier b529 fixture. It records the exact failures caused by the pre-repair runtime: handshake protocol default, submit missing admission operation identity, observe missing event protocol, and cancel identity mismatch; replay passed. That file is not promoted to the repaired revision.

These receipts qualify only the 23 Python scenarios recorded here. Existing Go and older 50-method receipts retain their own source/model/package identities and must be regenerated and rerun against this same frozen authority before a unified 106-method qualification can be claimed. No registry publishing, deployment, or main merge is part of this evidence.
