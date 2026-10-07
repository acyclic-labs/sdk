# Generated Python typing qualification fixtures

`positive.py` exercises the generated nominal constructors and typed fields:
`ActorId`, `CodeSha256`, `ActorLimits`, `Binding`, `SubscriptionStart`,
`SubscriptionSpec`, request records, headers, and invoke responses.

`negative.py` deliberately supplies wrong types. Both `mypy` 1.17.1 and
Pyright 1.1.404 report all six intended negative cases. Their complete output
is retained in `mypy-negative.txt` and `pyright-negative.json`.

The positive fixture exposed one generated binding typing defect in both
checkers: `SubscriptionStart.CURSOR(1)` is emitted as the nested `CURSOR`
class, while `SubscriptionSpec` annotates the parameter as `SubscriptionStart`.
The runtime enum is valid; the generated Python type relationship is not. The
full diagnostics are retained in `mypy-positive.txt` and
`pyright-positive.json`. This remains an upstream/generated-output gap rather
than a consumer-side cast or handwritten contract.
