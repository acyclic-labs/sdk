`bun-node-tests.xml` is captured from Bun's native JUnit reporter running a
planted failing `describe('parent')` / `it('failure <&> 😀')` test, using Bun 1.4.2:
`bun test --reporter=junit --reporter-outfile=report.xml ./bun.test.ts`.
Environment properties and hostname attributes are omitted and the temporary source directory is replaced with
`/fixture`; test identities, counts, nesting and XML escaping remain original.
The retained decoder property mutates this actual report and requires malformed
XML to refuse, rather than manufacturing a parallel reporter format.
