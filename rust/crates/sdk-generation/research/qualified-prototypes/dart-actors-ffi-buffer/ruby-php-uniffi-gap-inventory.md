# UniFFI Ruby/PHP gap inventory

Date: 2026-10-07. This inventory records generator availability and qualification evidence; it does not claim that an installed probe is a production SDK.

## Ruby

- **Maintained producer:** UniFFI’s official user guide lists Ruby as shipped support and says most documented features work, while also warning that the team keeps Ruby working and generally does not add new features. [Official user guide](https://mozilla.github.io/uniffi-rs/)
- **Generation path:** the official tutorial invokes `uniffi-bindgen generate ... --language ruby` and emits a `.rb` binding. [Official tutorial](https://github.com/mozilla/uniffi-rs/blob/main/docs/manual/src/tutorial/foreign_language_bindings.md)
- **Version evidence:** the local all-eight installed-gem artifact is `Q:/sdk/work/actors-uniffi-ruby-0322-all8-current-20261007`; the historical production gem artifact is `Q:/sdk/work/actors-uniffi-ruby-0322-gem-20261007`. Treat these as installed/probe evidence at UniFFI 0.32.2 and historical 0.31 production evidence respectively. No claim is made here that 0.31 was upgraded in place.
- **Qualification gap:** run the current Rust facade through the selected 0.32.2 producer and package the resulting gem; exercise all eight installed operations (public client/authentication, inflight cancellation, server abort, constructors, errors, bytes, and `u64`) against the matching native library. A constructor-only probe is insufficient.

## PHP

- **Official status:** UniFFI’s official README lists Kotlin, Swift, Python, and Ruby as supported languages and enumerates third-party bindings such as C#, Go, Dart, Java, Node, and Haskell; PHP is absent from both lists. [Official README](https://github.com/mozilla/uniffi-rs)
- **Repository status:** the official repository has no PHP binding generator in its supported-language list or source layout. The absence is evidence of an unqualified official path, not proof that no private or experimental PHP wrapper exists.
- **Practical target:** PHP remains outstanding. Do not mark it excluded without a concrete maintained prototype. The minimum evidence should include a pinned UniFFI producer, generated PHP source, a PHP FFI/runtime dependency plan, native artifact loading, typed errors, bytes, `u64`, constructors, authentication, inflight cancellation, and server abort across the same all-eight operation set used for the other clients.

## Decision

Ruby is a maintained upstream backend with a known feature-maintenance caveat and local installed-gem evidence. PHP is an open qualification target pending a concrete maintained producer/prototype and end-to-end artifact evidence.
