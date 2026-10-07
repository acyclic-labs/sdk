# `safe_version` injectivity and static URL proof note

**Candidate source snapshot SHA-256:** `E627EAFBDD6C58784827520789C251171EDE7DBC7FDF6033EF5572A5AEBCD4AB`
**Candidate source:** `rust/crates/sdk-docs/src/lib.rs`, `safe_version` and
`hex_version` in the candidate source snapshot.

This note proves the replacement mapping in the candidate source snapshot. It
does not claim that the baseline commit or any older SDK docs implementation has
the same bytes or behavior. The recorded source hash covers the exact version-mapping bytes proved here.

## Scope and mapping

The domain is the set of non-empty Rust UTF-8 strings accepted by the candidate
`safe_version`, including its final 255-byte output-length guard. The implementation
rejects the empty string and rejects any mapped segment whose UTF-8 byte length
exceeds 255. A caller may apply additional input restrictions; restricting the
domain cannot introduce a collision.

The readable alphabet is lowercase ASCII letters, digits, `.`, `-`, and `_`.
The tilde `~` is deliberately excluded. A string is returned unchanged only if
every byte is in that alphabet, it is not `.` or `..`, it does not end in `.` or
space, and it is not a Windows reserved device segment. Every other accepted
string is encoded as `~` followed by two lowercase hexadecimal digits for each
byte of its UTF-8 representation, and the mapped output must be at most 255
bytes. Thus a fallback input has at most 127 UTF-8 bytes; a readable input has
at most 255 bytes.

Both output forms use only URI-unreserved characters (`a-z`, `0-9`, `.`, `-`,
`_`, `~`). In particular, the output contains no `%`, `/`, `?`, or `#`, so a
static HTTP server does not percent-decode the generated filename before its
filesystem lookup.

## Canonical decoding is a left inverse

Let `B(s)` be the UTF-8 byte sequence for an accepted Rust string `s`. Define
`D` on canonical outputs as follows:

- if the output does not begin with `~`, decode each readable character as its
  one-byte ASCII value;
- if the output begins with `~`, require an even-length remainder containing
  only lowercase hexadecimal digits, decode each pair to one byte, and decode
  the resulting bytes as UTF-8;
- reject every other form.

The readable alphabet excludes `~`, so the leading tilde identifies the full
hex form uniquely. In that form, fixed two-digit tokens give unique boundaries;
lowercase hexadecimal is canonical. For each input byte, the readable branch
returns that byte directly and the full-hex branch returns the byte represented
by its unique two-digit token. Therefore

    D(safe_version(s)) = B(s)

for every accepted `s`. This is an actual left-inverse proof; it does not depend
on a hash or an assumed collision probability.

## Injectivity before filesystem and URL normalization

Assume `safe_version(x) = safe_version(y)`. Applying `D` to both sides gives

    B(x) = D(safe_version(x))
         = D(safe_version(y))
         = B(y).

UTF-8 encoding is injective on Rust strings, so `x = y`. The argument covers
readable results, full-hex results, and a readable result compared with a
full-hex result because the latter begins with `~` and the former cannot.

The output alphabet is URI-unreserved, so URL parsing does not reinterpret a
generated name as an escape sequence or path separator. A static HTTP lookup
therefore reaches the same literal relative path used by the filesystem. This
is the integration property that percent-containing encodings did not provide.

## Windows case-insensitive comparison

Every canonical output is already lowercase ASCII. Windows case-insensitive
comparison therefore leaves the equality relation between canonical outputs
unchanged. The leading `~` also remains a literal separator between the two
mapping branches; no readable output can begin with it.

Consequently, if two canonical outputs compare equal under Windows filename
case folding, they are equal as canonical outputs, and the left-inverse argument
gives equal source versions. No Unicode case-folding algorithm is needed because
the output is restricted to ASCII.

## Reserved names and trailing-dot fallback

The readable branch is skipped when its result would be a Windows-reserved
device segment or would end in a Windows-trimmed dot or space. The full-hex
fallback begins with `~` and ends in a hexadecimal digit:

- it contains no raw reserved spelling such as `con`, `nul`, `com1`, or `lpt1`;
- it cannot become a reserved name through ASCII case folding;
- it cannot end in a dot or space;
- it cannot alias a readable result because the branch prefixes are disjoint.

Thus the Windows path normalization rules that motivate the fallback do not
identify two different accepted versions.

## Result

Under the stated canonical-output rules, `safe_version` is injective modulo
Windows case-insensitive filename comparison and is safe to use as a literal
static HTTP path component on its accepted domain. The proof is constructive:
decode the canonical name, recover the exact UTF-8 bytes, and then recover the
original Rust string. Input validity or filename length limits can reduce the
accepted domain, but cannot weaken this result.
