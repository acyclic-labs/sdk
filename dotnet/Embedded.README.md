# Acyclic embedded Stream SDK for .NET

`Acyclic.Sdk.Embedded` is a thin .NET facade over the Rust-owned
`sdk-embedded-prototype` C ABI. It does not translate the Stream algorithm
into C#; NuGet selects the matching Rust native asset from the package RID.

The current ABI exposes append, finite read, follow, cancellation, owned byte
buffers, and provider recovery. The remote Stream RPCs that are not part of
this embedded ABI remain in the generated remote package and are not silently
reimplemented here.
