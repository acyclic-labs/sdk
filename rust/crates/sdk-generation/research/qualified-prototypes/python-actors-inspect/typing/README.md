# Generated Python typing qualification fixtures

`positive.py` exercises the generated nominal constructors and typed fields:
`ActorId`, `CodeSha256`, `ActorLimits`, `Binding`, `SubscriptionStart`,
`SubscriptionSpec`, request records, headers, and the invoke response.

`negative.py` deliberately supplies wrong types. A maintained checker should
report the invalid string/bytes, integer/string, list element, and response
assignment cases. It is a checker input, never a runtime test.

The current machine has no `mypy` or `pyright` executable and neither package
is installed in the isolated wheel environment. Network package resolution
also did not complete, so this repository records the fixtures without
claiming a static-checker pass. Runtime and generated-annotation evidence is
in the adjacent qualification receipt.
