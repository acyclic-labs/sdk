# Reference decode limits

The existing `canonical_binding_pages_reject_prefixes_duplicates_and_wrong_children`
property compares the production reference visitor with the canonical decoder on
its generated pages under the original page-item and name bounds. Collection
passes its selected volume limits to that visitor. Storage compositions without
a volume select limits from the already admitted complete object.

`mutations/reference_decode_limits.patch` restores the production defect: the
visitor discards the original limits and derives permissive limits from byte
length. The same property must reject it; no test-only decoder or alternate
reference implementation is used. Run the property with
`cargo test -p acyclic-fs canonical_binding_pages_reject_prefixes_duplicates_and_wrong_children`.

The change introduces no wait, provider call, allocation, copy or persistence
barrier. Selecting six scalar bounds takes constant work. Complete page decoding
and reference enumeration remain linear in the page's entries and bytes.
