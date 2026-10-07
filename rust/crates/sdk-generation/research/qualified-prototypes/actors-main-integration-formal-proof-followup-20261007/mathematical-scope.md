# Actors domain mathematical proof scope

This note records the source-level argument for the two remaining unbounded
domain obligations. It supplements the bounded Kani harness receipt in this
directory; it does not promote the bounded harnesses into all-length claims.

## Source binding

The production candidate is `ec0496fea57f137f2ecf4e4aa29c4275dbf3b0da`.
The isolated proof-module follow-up is product commit
`6579d6fb8d5a212f6a3bd66fe5bce5ea2cf84fe8`.

Hashes immediately before the proof-source review:

| source | SHA-256 |
| --- | --- |
| `rust/crates/actors/src/domain.rs` | `53255601658C33F955EF8CB860FB71EC4682585D5F0E26CE20DE11616369515A` |
| `rust/crates/actors/src/domain/formal_proofs.rs` | `8DCDF326B21CDF066C66F263B90BB0DF2B6601DA7B4A27077FD105484BD38B3A` |
| `rust/crates/actors/src/lib.rs` | `7F7076432EDCBC2D88A18F534B3259669622183F5B1EB1A3B6F7E1CF530F7252` |
| `rust/crates/actors/src/contract.rs` | `4E01F66F5AEF315F25CCDC502C4B01634A7126F8F5105B4D338EB3FC5FACBEF0` |
| `rust/crates/actors/build.rs` | `8A02F03262256B58EEB6AA756BCA122984DB53D91420A548B33CC4708CD605C3` |
| `rust/crates/actors/Cargo.toml` | `D687485CFB5CDD125AD5E9D2DE008E018577593549510BEB19B04184A7B01F78` |

The proof module is included from production `domain.rs` through its
`#[cfg(kani)]` path and calls the production functions directly.

## CodeSha256 theorem and trust boundary

The quantified source theorem is:

```text
For every v : Vec<u8>,
CodeSha256::new(v) = Err(InvalidCodeSha256)
  iff v.len() != 32 or every byte of v is zero.

Otherwise CodeSha256::new(v) = Ok(d), and d.as_bytes() is the original
32-byte sequence in the original order.
```

The derivation follows the unchanged production control flow:

1. `valid_code_sha256(&v)` returns
   `v.len() == 32 && v.iter().any(|byte| *byte != 0)`.
2. A failed predicate returns `InvalidCodeSha256`.
3. A successful predicate reaches `v.try_into()` for `[u8; 32]`.
4. The standard `TryFrom<Vec<T>> for [T; N]` contract is trusted to succeed
   exactly when the vector length is `N` and to preserve element order.
5. `CodeSha256::as_bytes` borrows the stored array without transformation.

The Kani harness
`code_sha256_exact_32_symbolic_bytes_preserved` proves the fixed-domain
specialization for every symbolic `[u8; 32]` with at least one nonzero byte.
The 0, 31, 33, and all-zero-32 tests are concrete rejection examples only.

Kani 0.68 does not implement `kani::Arbitrary` for `Vec<u8>`, so an
`any::<Vec<u8>>()` harness cannot establish the quantified `Vec<u8>` theorem.
The all-length statement above is therefore a source proof conditional on the
standard-library array-conversion contract, not a claim that CBMC explored
arbitrary heap-backed vectors.

## ActorId theorem and trust boundary

The quantified source theorem is:

```text
For every s : String,
ActorId::new(s) = Err(EmptyActorId) iff s.is_empty().
For nonempty s, ActorId::new(s) = Ok(a) and a.as_str() == s.as_str().
```

The derivation is direct:

1. `ActorId::new` checks `String::is_empty`.
2. The empty branch returns `EmptyActorId`.
3. The nonempty branch moves the original `String` into `ActorId` without
   rebuilding, filtering, or normalizing it.
4. `as_str` borrows that same stored string.
5. `TryFrom<String> for ActorId` delegates directly to `ActorId::new`.

The argument trusts the standard `String::is_empty` contract and Rust move and
borrow identity. Kani 0.68 does not implement `kani::Arbitrary` for `String`;
an attempted symbolic-string harness fails at compilation. No bounded string
harness is being used as a substitute for this unbounded source argument.

These arguments concern only the named semantic constructors and accessors.
They do not prove transport behavior, whole-request validation, serialization
of arbitrary strings, or whole-crate semantics.
