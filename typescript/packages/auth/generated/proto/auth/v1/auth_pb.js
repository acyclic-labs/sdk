// Normative acyclic.auth.v1 wire rules:
// Capability.version is exactly 2 and enters each block transcript as LE u32
// after its LE u64 index. Unsupported versions have a distinct error.
// Opaque Capability tokens require version (field 4) first, then signed blocks
// and the tip proof. Use the canonical capability writer; generic protobuf field
// ordering is not the canonical token encoding. Admit the version before any
// nested block or secret allocation. Version 1 used trailing version ordering;
// neither that spelling nor its signature transcript is accepted.
// Block 0 contains only the PUBLIC Birth + complete KeyCertificate chain.
// No block contains an account bearer or a signing key (except the tip proof).
// Verify Birth with environment trust, each key with the trusted account issuer
// root (genesis or authenticated keyring rotation, NEVER an offline leaf), block 0
// with the certified leaf, and block k with block k-1.next_public_key.
// Certificate signatures use BIRTH_DOMAIN / KEY_DOMAIN followed by canonical
// protobuf claims; snapshots use KEYRING_DOMAIN followed by canonical snapshot.
// Domains are acyclic.auth.v1/{birth,key,keyring,environment,allowance}
// followed by a NUL byte. Environment keys sign Births, account keyrings and
// allowances; the single keyring root signs KeyCertificates and revocations.
// Allowance caveats are verified offline against environment trust.
// Trust Streams: trust/v1/<env> carries environment keyrings; its child
// trust/v1/<env>/accounts/<account> carries keyrings and revocations in order.
// Canonical ingress rejects unknown/duplicate fields and misplaced certificates.
// Check ALL ancestor key ids/epochs and parent_revocation_ids, not just the leaf.
// A revoked block or online key invalidates every descendant. Online issuance
// MUST include all parent block ids and the complete parent certificate ancestry.
// Issuer-certified Principal claims cannot appear in attenuation blocks.
// LineageAppend changes acting identity (last id, else certified root id) only.
// It cannot add roles/groups/attributes. Subject references bind at the declaring
// block, not the final tip; lineage read matching is ancestor-or-self, pins are
// single-valued acting-id or literal. All grant/filter/pin/holder/time/audience
// caveats intersect; each grant set is a union. Owner policy Deny always wins.
// Audience is an exact canonical ResourcePattern (prefix=false); no slash trim.
// All timestamps are Unix SECONDS UTC; validity is [not_before, expires_at),
// at most 30 days. Values after 9999-12-31 are invalid (catches modern millis).
// MaxDepth is an absolute terminal block index (root=0), minimum over all blocks.
// Sealing appends no block and consumes no depth. Limits share the chain root:
// rates/concurrency at admission, cumulative lifetime totals globally at the
// metering owner/billing ledger. Delegation and renewal never reset counters.
// ExecutionGrant policy_generation, usage_owner and reserved_capacity_units are
// signed owner-admission facts, not facts inferred by the offline verifier. The
// execution owner checks every applicable certificate and caveat grant set
// against its current policy and shared reservation/usage ledger before launch.
// max_concurrency intersects with the request's shared-ledger concurrency facts;
// absent optional execution fields impose no additional constraint.
// Label is advisory, scoped to its block revocation id, and grants no authority.
// CallerContext is host-admitted invocation metadata, never a bearer or proof.
// Its caller remains the original customer across accounts; audience is the exact
// recipient account/ns/id. Delegation is ordered first delegate to presenter.
// Hosts authenticate dispatch binding and every authority independently, then
// recheck current trust/revocation, minimum expiry, target and owner policy on
// scoped handle use. Decoding this shape does not authenticate its claims.
// Guest memory receives only the invocation-scoped opaque capability handle,
// never an account credential, refresh secret, signing key or append authority.
// Product resource encodings/actions and static/dynamic extractor vocabularies
// live in validation/v1/options.proto FileOptions. Owners encode substitutions
// BEFORE expansion: Objects bucket / entire opaque key as separate components;
// other products canonicalize paths then encode each component. Never normalize
// Objects keys or decode before exact/whole-segment-prefix matching. New product catalogs
// do not require changing the verifier. HTTP-only routes use FileOptions.http_routes.
// Resource prefixes match complete slash-delimited component lists, never
// partial components: objects:/logs covers that bucket and its keys, but never
// objects:/logs-private. A final slash on a prefix is a segment separator.
// Opaque Objects keys remain encoded as one component; use bucket scope or exact
// encoded keys, not a partial encoded-key prefix.
// RecordAttributes is single-valued: every duplicate name is rejected at decode,
// including equal-value duplicates. All record consumers use this admitted type;
// certified principal attribute decoding applies the same uniqueness rule.
// External WebSocket destinations use typed DNS/IP and nonzero port facts, not
// resource grants. DNS is ASCII case-insensitive (one trailing dot is equivalent).
// Wildcard *.example.com matches subdomains at a dot boundary, never the apex.
// IP/CIDR rules match literal IP facts of the same family; no DNS lookup occurs.
// Each socket rule is host AND port (empty ports means 443); rules are a union,
// and ancestor certificates plus caveat grant sets are all intersected.
// A child certificate preserves principal kind/id and inherited lineage, and
// may only remove roles/groups/attested attributes. No child can certify a claim
// absent from its parent; only lineage may append.

// @generated by protoc-gen-es v2.17.0 with parameter "target=js+dts,import_extension=js"
// @generated from file auth/v1/auth.proto (package acyclic.auth.v1, syntax proto3)
/* eslint-disable */

import { enumDesc, fileDesc, messageDesc, tsEnum } from "@bufbuild/protobuf/codegenv2";
import { file_validation_v1_options } from "../../validation/v1/options_pb.js";

/**
 * Describes the file auth/v1/auth.proto.
 */
export const file_auth_v1_auth = /*@__PURE__*/
  fileDesc("ChJhdXRoL3YxL2F1dGgucHJvdG8SD2FjeWNsaWMuYXV0aC52MSJZChJBZG1pdHRlZENhcGFiaWxpdHkSEAoId29ya2xvYWQYASABKAwSDwoHcmVjZWlwdBgCIAEoDBIgChhhZG1pdHRlZF9hdF91bml4X3NlY29uZHMYAyABKAQiaAoJQWxsb3dhbmNlEhIKCmFjY291bnRfaWQYASABKAkSFAoMc3BlbmRfbWljcm9zGAIgASgEEh8KF2V4cGlyZXNfYXRfdW5peF9zZWNvbmRzGAMgASgEEhAKCHByb2R1Y3RzGAQgAygJIigKCUF0dHJpYnV0ZRIMCgRuYW1lGAEgASgJEg0KBXZhbHVlGAIgASgMImMKDkF0dHJpYnV0ZU1hdGNoEgwKBG5hbWUYASABKAkSDgoGYW55X29mGAIgAygMEjMKCHN1YmplY3RzGAMgAygLMiEuYWN5Y2xpYy5hdXRoLnYxLlN1YmplY3RSZWZlcmVuY2UimQIKC0F1ZGl0UmVjb3JkEhcKD2F0X3VuaXhfc2Vjb25kcxgBIAEoBBIRCgljaGFpbl9pZHMYAiADKAwSKwoHc3ViamVjdBgDIAEoCzIaLmFjeWNsaWMuYXV0aC52MS5QcmluY2lwYWwSDwoHbGluZWFnZRgEIAMoCRInCgZob2xkZXIYBSABKAsyFy5hY3ljbGljLmF1dGgudjEuSG9sZGVyEg4KBmFjdGlvbhgGIAEoCRIQCghyZXNvdXJjZRgHIAEoCRITCgtmaWx0ZXJfaGFzaBgIIAEoDBIwCghkZWNpc2lvbhgJIAEoDjIeLmFjeWNsaWMuYXV0aC52MS5BdWRpdERlY2lzaW9uEg4KBnJlYXNvbhgKIAEoCSJpCgxCaWxsaW5nR3JhbnQSLQoFZ3JhbnQYASABKAsyHi5hY3ljbGljLmF1dGgudjEuUmVzb3VyY2VHcmFudBIZCgxzcGVuZF9taWNyb3MYAiABKARIAIgBAUIPCg1fc3BlbmRfbWljcm9zImwKBUJpcnRoEhIKCmFjY291bnRfaWQYASABKAkSEwoLZW52aXJvbm1lbnQYAiABKAkSGgoSZ2VuZXNpc19wdWJsaWNfa2V5GAMgASgMEh4KFmlzc3VlZF9hdF91bml4X3NlY29uZHMYBCABKAQiTAoQQmlydGhDZXJ0aWZpY2F0ZRIlCgViaXJ0aBgBIAEoCzIWLmFjeWNsaWMuYXV0aC52MS5CaXJ0aBIRCglzaWduYXR1cmUYAiABKAwigwEKBUJsb2NrEjcKDGNlcnRpZmljYXRlcxgBIAEoCzIhLmFjeWNsaWMuYXV0aC52MS5DZXJ0aWZpY2F0ZUNoYWluEigKB2NhdmVhdHMYAiADKAsyFy5hY3ljbGljLmF1dGgudjEuQ2F2ZWF0EhcKD25leHRfcHVibGljX2tleRgDIAEoDCKdAQoPQ2FsbGVyQXV0aG9yaXR5EjIKCXByaW5jaXBhbBgBIAEoCzIfLmFjeWNsaWMuYXV0aC52MS5DYWxsZXJJZGVudGl0eRIfChdleHBpcmVzX2F0X3VuaXhfc2Vjb25kcxgCIAEoBBI1CgpyZXZvY2F0aW9uGAMgASgLMiEuYWN5Y2xpYy5hdXRoLnYxLkNhbGxlclJldm9jYXRpb24i9wEKDUNhbGxlckNvbnRleHQSDwoHdmVyc2lvbhgBIAEoDRIwCgZjYWxsZXIYAiABKAsyIC5hY3ljbGljLmF1dGgudjEuQ2FsbGVyQXV0aG9yaXR5EjEKCGF1ZGllbmNlGAMgASgLMh8uYWN5Y2xpYy5hdXRoLnYxLkNhbGxlcklkZW50aXR5EjQKCmRlbGVnYXRpb24YBCADKAsyIC5hY3ljbGljLmF1dGgudjEuQ2FsbGVyQXV0aG9yaXR5Eh8KF2V4cGlyZXNfYXRfdW5peF9zZWNvbmRzGAUgASgEEhkKEWNhcGFiaWxpdHlfaGFuZGxlGAYgASgMIjkKDkNhbGxlcklkZW50aXR5Eg8KB2FjY291bnQYASABKAwSCgoCbnMYAiABKAwSCgoCaWQYAyABKAwiOAoTQ2FsbGVyS2V5UmV2b2NhdGlvbhIOCgZrZXlfaWQYASABKAkSEQoJa2V5X2Vwb2NoGAIgASgEIlYKEENhbGxlclJldm9jYXRpb24SMgoEa2V5cxgBIAMoCzIkLmFjeWNsaWMuYXV0aC52MS5DYWxsZXJLZXlSZXZvY2F0aW9uEg4KBmJsb2NrcxgCIAMoDCJ4CgpDYXBhYmlsaXR5EiwKBmJsb2NrcxgBIAMoCzIcLmFjeWNsaWMuYXV0aC52MS5TaWduZWRCbG9jaxIQCgZzZWNyZXQYAiABKAxIABIQCgZzZWFsZWQYAyABKAxIABIPCgd2ZXJzaW9uGAQgASgNQgcKBXByb29mIvYDCgZDYXZlYXQSKwoGZ3JhbnRzGAEgASgLMhkuYWN5Y2xpYy5hdXRoLnYxLkdyYW50U2V0SAASIQoXZXhwaXJlc19hdF91bml4X3NlY29uZHMYAiABKARIABIhChdub3RfYmVmb3JlX3VuaXhfc2Vjb25kcxgDIAEoBEgAEjQKCGF1ZGllbmNlGAQgASgLMiAuYWN5Y2xpYy5hdXRoLnYxLlJlc291cmNlUGF0dGVybkgAEikKBmhvbGRlchgFIAEoCzIXLmFjeWNsaWMuYXV0aC52MS5Ib2xkZXJIABI4Cg5saW5lYWdlX2FwcGVuZBgGIAEoCzIeLmFjeWNsaWMuYXV0aC52MS5MaW5lYWdlQXBwZW5kSAASKQoGZmlsdGVyGAcgASgLMhcuYWN5Y2xpYy5hdXRoLnYxLkZpbHRlckgAEiMKA3BpbhgIIAEoCzIULmFjeWNsaWMuYXV0aC52MS5QaW5IABIpCgZsaW1pdHMYCSABKAsyFy5hY3ljbGljLmF1dGgudjEuTGltaXRzSAASEwoJbWF4X2RlcHRoGAogASgNSAASDwoFbGFiZWwYCyABKAlIABI1CglhbGxvd2FuY2UYDCABKAsyIC5hY3ljbGljLmF1dGgudjEuU2lnbmVkQWxsb3dhbmNlSABCBgoEa2luZCJzChBDZXJ0aWZpY2F0ZUNoYWluEjAKBWJpcnRoGAEgASgLMiEuYWN5Y2xpYy5hdXRoLnYxLkJpcnRoQ2VydGlmaWNhdGUSLQoEa2V5cxgCIAMoCzIfLmFjeWNsaWMuYXV0aC52MS5LZXlDZXJ0aWZpY2F0ZSKtAgoMQ2VydGlmaWVkS2V5EhIKCmFjY291bnRfaWQYASABKAkSDgoGa2V5X2lkGAIgASgJEhIKCnB1YmxpY19rZXkYAyABKAwSEQoJa2V5X2Vwb2NoGAQgASgEEisKCHZhbGlkaXR5GAUgASgLMhkuYWN5Y2xpYy5hdXRoLnYxLlZhbGlkaXR5EikKBmdyYW50cxgGIAEoCzIZLmFjeWNsaWMuYXV0aC52MS5HcmFudFNldBItCglwcmluY2lwYWwYByABKAsyGi5hY3ljbGljLmF1dGgudjEuUHJpbmNpcGFsEhoKDXBhcmVudF9rZXlfaWQYCCABKAlIAIgBARIdChVwYXJlbnRfcmV2b2NhdGlvbl9pZHMYCSADKAxCEAoOX3BhcmVudF9rZXlfaWQiggEKEkVudmlyb25tZW50S2V5cmluZxITCgtlbnZpcm9ubWVudBgBIAEoCRIQCghzZXF1ZW5jZRgCIAEoBBIXCg9wcmV2aW91c19kaWdlc3QYAyABKAwSDAoEa2V5cxgEIAMoDBIeChZpc3N1ZWRfYXRfdW5peF9zZWNvbmRzGAUgASgEIucCCg5FeGVjdXRpb25HcmFudBItCgVncmFudBgBIAEoCzIeLmFjeWNsaWMuYXV0aC52MS5SZXNvdXJjZUdyYW50EhcKCmNwdV9taWxsaXMYAiABKARIAIgBARIZCgxtZW1vcnlfYnl0ZXMYAyABKARIAYgBARIeChFwb2xpY3lfZ2VuZXJhdGlvbhgEIAEoBEgCiAEBEhgKC3VzYWdlX293bmVyGAUgASgJSAOIAQESHAoPbWF4X2NvbmN1cnJlbmN5GAYgASgESASIAQESJAoXcmVzZXJ2ZWRfY2FwYWNpdHlfdW5pdHMYByABKARIBYgBAUINCgtfY3B1X21pbGxpc0IPCg1fbWVtb3J5X2J5dGVzQhQKEl9wb2xpY3lfZ2VuZXJhdGlvbkIOCgxfdXNhZ2Vfb3duZXJCEgoQX21heF9jb25jdXJyZW5jeUIaChhfcmVzZXJ2ZWRfY2FwYWNpdHlfdW5pdHMijAEKD0ZpbGVzeXN0ZW1HcmFudBItCgVncmFudBgBIAEoCzIeLmFjeWNsaWMuYXV0aC52MS5SZXNvdXJjZUdyYW50EhYKCXBvc2l4X3VpZBgCIAEoDUgAiAEBEhYKCXBvc2l4X2dpZBgDIAEoDUgBiAEBQgwKCl9wb3NpeF91aWRCDAoKX3Bvc2l4X2dpZCJmCgZGaWx0ZXISJwoEa2V5cxgBIAMoCzIZLmFjeWNsaWMuYXV0aC52MS5LZXlSYW5nZRIzCgphdHRyaWJ1dGVzGAIgAygLMh8uYWN5Y2xpYy5hdXRoLnYxLkF0dHJpYnV0ZU1hdGNoIqgCCgVHcmFudBIyCghyZXNvdXJjZRgBIAEoCzIeLmFjeWNsaWMuYXV0aC52MS5SZXNvdXJjZUdyYW50SAASNAoJZXhlY3V0aW9uGAIgASgLMh8uYWN5Y2xpYy5hdXRoLnYxLkV4ZWN1dGlvbkdyYW50SAASMAoHYmlsbGluZxgDIAEoCzIdLmFjeWNsaWMuYXV0aC52MS5CaWxsaW5nR3JhbnRIABJDChF3ZWJzb2NrZXRfY29ubmVjdBgEIAEoCzImLmFjeWNsaWMuYXV0aC52MS5XZWJzb2NrZXRDb25uZWN0R3JhbnRIABI2CgpmaWxlc3lzdGVtGAUgASgLMiAuYWN5Y2xpYy5hdXRoLnYxLkZpbGVzeXN0ZW1HcmFudEgAQgYKBGtpbmQiMgoIR3JhbnRTZXQSJgoGZ3JhbnRzGAEgAygLMhYuYWN5Y2xpYy5hdXRoLnYxLkdyYW50IioKBkhvbGRlchIUCgdhY2NvdW50GAEgASgJSACIAQFCCgoIX2FjY291bnQiRAoQSXNzdWFuY2VSZWplY3RlZBIwCgZyZWFzb24YASABKA4yIC5hY3ljbGljLmF1dGgudjEuSXNzdWFuY2VSZWZ1c2FsIuYBCg1Jc3N1YW5jZVJlcGx5Eg8KB3ZlcnNpb24YASABKA0SFgoOcmVxdWVzdF9kaWdlc3QYAiABKAwSNQoIYWNjZXB0ZWQYAyABKAsyIS5hY3ljbGljLmF1dGgudjEuQ2VydGlmaWNhdGVDaGFpbkgAEjQKB3JlZnVzZWQYBCABKAsyIS5hY3ljbGljLmF1dGgudjEuSXNzdWFuY2VSZWplY3RlZEgAEjMKB3Vua25vd24YBSABKAsyIC5hY3ljbGljLmF1dGgudjEuSXNzdWFuY2VVbmtub3duSABCCgoIZGVjaXNpb24ijQEKD0lzc3VhbmNlUmVxdWVzdBIPCgd2ZXJzaW9uGAEgASgNEhUKDW9wZXJhdGlvbl9rZXkYAiABKAwSDgoGaXNzdWVyGAMgASgMEhMKC2Vudmlyb25tZW50GAQgASgJEi0KBmNsYWltcxgFIAEoCzIdLmFjeWNsaWMuYXV0aC52MS5DZXJ0aWZpZWRLZXkiEQoPSXNzdWFuY2VVbmtub3duIk8KDktleUNlcnRpZmljYXRlEioKA2tleRgBIAEoCzIdLmFjeWNsaWMuYXV0aC52MS5DZXJ0aWZpZWRLZXkSEQoJc2lnbmF0dXJlGAIgASgMIiYKCEtleVJhbmdlEg0KBXN0YXJ0GAEgASgMEgsKA2VuZBgCIAEoDCJACg1LZXlSZXZvY2F0aW9uEg4KBmtleV9pZBgBIAEoCRIfChdleHBpcmVzX2F0X3VuaXhfc2Vjb25kcxgCIAEoBCLvAQoPS2V5cmluZ1NuYXBzaG90EhIKCmFjY291bnRfaWQYASABKAkSEAoIc2VxdWVuY2UYAiABKAQSFwoPcHJldmlvdXNfZGlnZXN0GAMgASgMEhkKEW1pbmltdW1fa2V5X2Vwb2NoGAQgASgEEhcKD3Jldm9rZWRfa2V5X2lkcxgFIAMoCRIwCgtyZXZvY2F0aW9ucxgGIAMoCzIbLmFjeWNsaWMuYXV0aC52MS5SZXZvY2F0aW9uEhcKD3Jvb3RfcHVibGljX2tleRgHIAEoDBIeChZpc3N1ZWRfYXRfdW5peF9zZWNvbmRzGAggASgEIqICCgZMaW1pdHMSEAoDcnBzGAEgASgESACIAQESEgoFYnVyc3QYAiABKARIAYgBARIYCgtieXRlc19wZXJfcxgDIAEoBEgCiAEBEhUKCGNoYW5uZWxzGAQgASgESAOIAQESGAoLY29uY3VycmVuY3kYBSABKARIBIgBARISCgVieXRlcxgGIAEoBEgFiAEBEhkKDHNwZW5kX21pY3JvcxgHIAEoBEgGiAEBEhMKBnRva2VucxgIIAEoBEgHiAEBQgYKBF9ycHNCCAoGX2J1cnN0Qg4KDF9ieXRlc19wZXJfc0ILCglfY2hhbm5lbHNCDgoMX2NvbmN1cnJlbmN5QggKBl9ieXRlc0IPCg1fc3BlbmRfbWljcm9zQgkKB190b2tlbnMiHAoNTGluZWFnZUFwcGVuZBILCgNpZHMYASADKAkiYQoDUGluEgwKBG5hbWUYASABKAkSEQoHbGl0ZXJhbBgCIAEoDEgAEjAKCnN1YmplY3RfaWQYAyABKAsyGi5hY3ljbGljLmF1dGgudjEuU3ViamVjdElkSABCBwoFdmFsdWUiQwoTUGxhaW5TZWNyZXRWYXJpYWJsZRIMCgRuYW1lGAEgASgJEg8KB3ZlcnNpb24YAiABKAQSDQoFdmFsdWUYAyABKAkipQEKCVByaW5jaXBhbBIsCgRraW5kGAEgASgOMh4uYWN5Y2xpYy5hdXRoLnYxLlByaW5jaXBhbEtpbmQSCgoCaWQYAiABKAkSDwoHbGluZWFnZRgDIAMoCRIOCgZncm91cHMYBCADKAkSDQoFcm9sZXMYBSADKAkSLgoKYXR0cmlidXRlcxgGIAMoCzIaLmFjeWNsaWMuYXV0aC52MS5BdHRyaWJ1dGUiOAoXUHJvdGVjdGVkU2VjcmV0VmFyaWFibGUSDAoEbmFtZRgBIAEoCRIPCgd2ZXJzaW9uGAIgASgEIkIKEFJlY29yZEF0dHJpYnV0ZXMSLgoKYXR0cmlidXRlcxgBIAMoCzIaLmFjeWNsaWMuYXV0aC52MS5BdHRyaWJ1dGUiVAoNUmVzb3VyY2VHcmFudBIyCghyZXNvdXJjZRgBIAEoCzIgLmFjeWNsaWMuYXV0aC52MS5SZXNvdXJjZVBhdHRlcm4SDwoHYWN0aW9ucxgCIAMoCSIvCg9SZXNvdXJjZVBhdHRlcm4SDAoEbmFtZRgBIAEoCRIOCgZwcmVmaXgYAiABKAgiZwoKUmV2b2NhdGlvbhIKCgJpZBgBIAEoDBIfChdleHBpcmVzX2F0X3VuaXhfc2Vjb25kcxgCIAEoBBIOCgZyZWFzb24YAyABKAkSEgoFbGFiZWwYBCABKAlIAIgBAUIICgZfbGFiZWwi9wEKFFJldm9jYXRpb25DaGVja3BvaW50EhIKCmFjY291bnRfaWQYASABKAkSEAoIc2VxdWVuY2UYAiABKAQSIwobcmV0aXJlZF9iZWZvcmVfdW5peF9zZWNvbmRzGAMgASgEEhkKEW1pbmltdW1fa2V5X2Vwb2NoGAQgASgEEiwKBGtleXMYBSADKAsyHi5hY3ljbGljLmF1dGgudjEuS2V5UmV2b2NhdGlvbhIrCgZibG9ja3MYBiADKAsyGy5hY3ljbGljLmF1dGgudjEuUmV2b2NhdGlvbhIeChZpc3N1ZWRfYXRfdW5peF9zZWNvbmRzGAcgASgEIo0CCg9SZXZvY2F0aW9uRGVsdGESEgoKYWNjb3VudF9pZBgBIAEoCRIQCghzZXF1ZW5jZRgCIAEoBBIXCg9wcmV2aW91c19kaWdlc3QYAyABKAwSHgoWaXNzdWVkX2F0X3VuaXhfc2Vjb25kcxgEIAEoBBIsCgRrZXlzGAUgAygLMh4uYWN5Y2xpYy5hdXRoLnYxLktleVJldm9jYXRpb24SKwoGYmxvY2tzGAYgAygLMhsuYWN5Y2xpYy5hdXRoLnYxLlJldm9jYXRpb24SJAoXcmV2b2tlX2FsbF9iZWZvcmVfZXBvY2gYByABKARIAIgBAUIaChhfcmV2b2tlX2FsbF9iZWZvcmVfZXBvY2giTwoTU2VjcmV0RGVsZXRlUmVxdWVzdBINCgVzY29wZRgBIAEoCRIMCgRuYW1lGAIgASgJEhsKE2V4cGVjdGVkX2dlbmVyYXRpb24YAyABKAQiNQoQU2VjcmV0TGlzdEN1cnNvchISCgpnZW5lcmF0aW9uGAEgASgEEg0KBWFmdGVyGAIgASgJIpEBCg9TZWNyZXRMaXN0UmVwbHkSEgoKZ2VuZXJhdGlvbhgBIAEoBBIyCgl2YXJpYWJsZXMYAiADKAsyHy5hY3ljbGljLmF1dGgudjEuU2VjcmV0VmFyaWFibGUSNgoLbmV4dF9jdXJzb3IYAyABKAsyIS5hY3ljbGljLmF1dGgudjEuU2VjcmV0TGlzdEN1cnNvciJoChFTZWNyZXRMaXN0UmVxdWVzdBINCgVzY29wZRgBIAEoCRIxCgZjdXJzb3IYAiABKAsyIS5hY3ljbGljLmF1dGgudjEuU2VjcmV0TGlzdEN1cnNvchIRCglwYWdlX3NpemUYAyABKA0icwoTU2VjcmV0TXV0YXRpb25SZXBseRISCgpnZW5lcmF0aW9uGAEgASgEEgwKBG5hbWUYAiABKAkSDwoHdmVyc2lvbhgDIAEoBBIpCgRraW5kGAQgASgOMhsuYWN5Y2xpYy5hdXRoLnYxLlNlY3JldEtpbmQihgEKEFNlY3JldFB1dFJlcXVlc3QSDQoFc2NvcGUYASABKAkSDAoEbmFtZRgCIAEoCRIpCgRraW5kGAMgASgOMhsuYWN5Y2xpYy5hdXRoLnYxLlNlY3JldEtpbmQSDQoFdmFsdWUYBCABKAkSGwoTZXhwZWN0ZWRfZ2VuZXJhdGlvbhgFIAEoBCKOAQoOU2VjcmV0VmFyaWFibGUSNQoFcGxhaW4YASABKAsyJC5hY3ljbGljLmF1dGgudjEuUGxhaW5TZWNyZXRWYXJpYWJsZUgAEj0KCXByb3RlY3RlZBgCIAEoCzIoLmFjeWNsaWMuYXV0aC52MS5Qcm90ZWN0ZWRTZWNyZXRWYXJpYWJsZUgAQgYKBGtpbmQiUwoPU2lnbmVkQWxsb3dhbmNlEi0KCWFsbG93YW5jZRgBIAEoCzIaLmFjeWNsaWMuYXV0aC52MS5BbGxvd2FuY2USEQoJc2lnbmF0dXJlGAIgASgMIjEKC1NpZ25lZEJsb2NrEg8KB3BheWxvYWQYASABKAwSEQoJc2lnbmF0dXJlGAIgASgMImMKGFNpZ25lZEVudmlyb25tZW50S2V5cmluZxI0CgdrZXlyaW5nGAEgASgLMiMuYWN5Y2xpYy5hdXRoLnYxLkVudmlyb25tZW50S2V5cmluZxIRCglzaWduYXR1cmUYAiABKAwiXgoVU2lnbmVkS2V5cmluZ1NuYXBzaG90EjIKCHNuYXBzaG90GAEgASgLMiAuYWN5Y2xpYy5hdXRoLnYxLktleXJpbmdTbmFwc2hvdBIRCglzaWduYXR1cmUYAiABKAwipAEKEFNpZ25lZFJldm9jYXRpb24SMQoFZGVsdGEYASABKAsyIC5hY3ljbGljLmF1dGgudjEuUmV2b2NhdGlvbkRlbHRhSAASOwoKY2hlY2twb2ludBgCIAEoCzIlLmFjeWNsaWMuYXV0aC52MS5SZXZvY2F0aW9uQ2hlY2twb2ludEgAEhEKCXNpZ25hdHVyZRgDIAEoDEINCgtwdWJsaWNhdGlvbiJPCg5Tb2NrZXRIb3N0UnVsZRIMCgRob3N0GAEgASgJEi8KBXBvcnRzGAIgAygLMiAuYWN5Y2xpYy5hdXRoLnYxLlNvY2tldFBvcnRSYW5nZSIuCg9Tb2NrZXRQb3J0UmFuZ2USDQoFZmlyc3QYASABKA0SDAoEbGFzdBgCIAEoDSILCglTdWJqZWN0SWQiEAoOU3ViamVjdExpbmVhZ2UieAoQU3ViamVjdFJlZmVyZW5jZRIoCgJpZBgBIAEoCzIaLmFjeWNsaWMuYXV0aC52MS5TdWJqZWN0SWRIABIyCgdsaW5lYWdlGAIgASgLMh8uYWN5Y2xpYy5hdXRoLnYxLlN1YmplY3RMaW5lYWdlSABCBgoEa2luZCLNAQoLVHJ1c3RSZWNvcmQSQAoLZW52aXJvbm1lbnQYASABKAsyKS5hY3ljbGljLmF1dGgudjEuU2lnbmVkRW52aXJvbm1lbnRLZXlyaW5nSAASOQoHa2V5cmluZxgCIAEoCzImLmFjeWNsaWMuYXV0aC52MS5TaWduZWRLZXlyaW5nU25hcHNob3RIABI3CgpyZXZvY2F0aW9uGAMgASgLMiEuYWN5Y2xpYy5hdXRoLnYxLlNpZ25lZFJldm9jYXRpb25IAEIICgZyZWNvcmQiTAoIVmFsaWRpdHkSHwoXbm90X2JlZm9yZV91bml4X3NlY29uZHMYASABKAQSHwoXZXhwaXJlc19hdF91bml4X3NlY29uZHMYAiABKAQiRwoVV2Vic29ja2V0Q29ubmVjdEdyYW50Ei4KBXJ1bGVzGAEgAygLMh8uYWN5Y2xpYy5hdXRoLnYxLlNvY2tldEhvc3RSdWxlKrcBCg1BdWRpdERlY2lzaW9uEh4KGkFVRElUX0RFQ0lTSU9OX1VOU1BFQ0lGSUVEEAASGAoUQVVESVRfREVDSVNJT05fQUxMT1cQARIXChNBVURJVF9ERUNJU0lPTl9ERU5ZEAISFwoTQVVESVRfREVDSVNJT05fTUlOVBADEhkKFUFVRElUX0RFQ0lTSU9OX1JFVk9LRRAEEh8KG0FVRElUX0RFQ0lTSU9OX0NIQU5ORUxfT1BFThAFKpQBCg9Jc3N1YW5jZVJlZnVzYWwSIAocSVNTVUFOQ0VfUkVGVVNBTF9VTlNQRUNJRklFRBAAEhsKF0lTU1VBTkNFX1JFRlVTQUxfREVOSUVEEAESHQoZSVNTVUFOQ0VfUkVGVVNBTF9DT05GTElDVBACEiMKH0lTU1VBTkNFX1JFRlVTQUxfSVNTVUVSX0NIQU5HRUQQAyq2AQoNUHJpbmNpcGFsS2luZBIeChpQUklOQ0lQQUxfS0lORF9VTlNQRUNJRklFRBAAEhcKE1BSSU5DSVBBTF9LSU5EX1VTRVIQARIYChRQUklOQ0lQQUxfS0lORF9BR0VOVBACEhoKFlBSSU5DSVBBTF9LSU5EX1NFUlZJQ0UQAxIYChRQUklOQ0lQQUxfS0lORF9HUk9VUBAEEhwKGFBSSU5DSVBBTF9LSU5EX0FOT05ZTU9VUxAFKnQKClNlY3JldEtpbmQSGwoXU0VDUkVUX0tJTkRfVU5TUEVDSUZJRUQQABIVChFTRUNSRVRfS0lORF9QTEFJThABEhkKFVNFQ1JFVF9LSU5EX1BST1RFQ1RFRBACEhcKE1NFQ1JFVF9LSU5EX0RFTEVURUQQA0KYAloxZ2l0aHViLmNvbS9hY3ljbGljLWxhYnMvc2RrL2dvL2dlbi9hdXRoL3YxO2F1dGh2McL0GEgKBFBPU1QSGC92MS9pZGVudGl0eS9zZWNyZXRzL3B1dBomCgtzZWNyZXRzL3B1dBIXc2VjcmV0czove3Njb3BlfS97bmFtZX3C9BhDCgRQT1NUEhkvdjEvaWRlbnRpdHkvc2VjcmV0cy9saXN0GiAKDHNlY3JldHMvbGlzdBIQc2VjcmV0czove3Njb3BlfcL0GE4KBFBPU1QSGy92MS9pZGVudGl0eS9zZWNyZXRzL2RlbGV0ZRopCg5zZWNyZXRzL2RlbGV0ZRIXc2VjcmV0czove3Njb3BlfS97bmFtZX1iBnByb3RvMw", [file_validation_v1_options]);

/**
 * Describes the message acyclic.auth.v1.AdmittedCapability.
 * Use `create(AdmittedCapabilitySchema)` to create a new message.
 */
export const AdmittedCapabilitySchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 0);

/**
 * Describes the message acyclic.auth.v1.Allowance.
 * Use `create(AllowanceSchema)` to create a new message.
 */
export const AllowanceSchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 1);

/**
 * Describes the message acyclic.auth.v1.Attribute.
 * Use `create(AttributeSchema)` to create a new message.
 */
export const AttributeSchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 2);

/**
 * Describes the message acyclic.auth.v1.AttributeMatch.
 * Use `create(AttributeMatchSchema)` to create a new message.
 */
export const AttributeMatchSchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 3);

/**
 * Describes the message acyclic.auth.v1.AuditRecord.
 * Use `create(AuditRecordSchema)` to create a new message.
 */
export const AuditRecordSchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 4);

/**
 * Describes the message acyclic.auth.v1.BillingGrant.
 * Use `create(BillingGrantSchema)` to create a new message.
 */
export const BillingGrantSchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 5);

/**
 * Describes the message acyclic.auth.v1.Birth.
 * Use `create(BirthSchema)` to create a new message.
 */
export const BirthSchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 6);

/**
 * Describes the message acyclic.auth.v1.BirthCertificate.
 * Use `create(BirthCertificateSchema)` to create a new message.
 */
export const BirthCertificateSchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 7);

/**
 * Describes the message acyclic.auth.v1.Block.
 * Use `create(BlockSchema)` to create a new message.
 */
export const BlockSchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 8);

/**
 * Describes the message acyclic.auth.v1.CallerAuthority.
 * Use `create(CallerAuthoritySchema)` to create a new message.
 */
export const CallerAuthoritySchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 9);

/**
 * Describes the message acyclic.auth.v1.CallerContext.
 * Use `create(CallerContextSchema)` to create a new message.
 */
export const CallerContextSchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 10);

/**
 * Describes the message acyclic.auth.v1.CallerIdentity.
 * Use `create(CallerIdentitySchema)` to create a new message.
 */
export const CallerIdentitySchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 11);

/**
 * Describes the message acyclic.auth.v1.CallerKeyRevocation.
 * Use `create(CallerKeyRevocationSchema)` to create a new message.
 */
export const CallerKeyRevocationSchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 12);

/**
 * Describes the message acyclic.auth.v1.CallerRevocation.
 * Use `create(CallerRevocationSchema)` to create a new message.
 */
export const CallerRevocationSchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 13);

/**
 * Describes the message acyclic.auth.v1.Capability.
 * Use `create(CapabilitySchema)` to create a new message.
 */
export const CapabilitySchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 14);

/**
 * Describes the message acyclic.auth.v1.Caveat.
 * Use `create(CaveatSchema)` to create a new message.
 */
export const CaveatSchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 15);

/**
 * Describes the message acyclic.auth.v1.CertificateChain.
 * Use `create(CertificateChainSchema)` to create a new message.
 */
export const CertificateChainSchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 16);

/**
 * Describes the message acyclic.auth.v1.CertifiedKey.
 * Use `create(CertifiedKeySchema)` to create a new message.
 */
export const CertifiedKeySchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 17);

/**
 * Describes the message acyclic.auth.v1.EnvironmentKeyring.
 * Use `create(EnvironmentKeyringSchema)` to create a new message.
 */
export const EnvironmentKeyringSchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 18);

/**
 * Describes the message acyclic.auth.v1.ExecutionGrant.
 * Use `create(ExecutionGrantSchema)` to create a new message.
 */
export const ExecutionGrantSchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 19);

/**
 * Describes the message acyclic.auth.v1.FilesystemGrant.
 * Use `create(FilesystemGrantSchema)` to create a new message.
 */
export const FilesystemGrantSchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 20);

/**
 * Describes the message acyclic.auth.v1.Filter.
 * Use `create(FilterSchema)` to create a new message.
 */
export const FilterSchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 21);

/**
 * Describes the message acyclic.auth.v1.Grant.
 * Use `create(GrantSchema)` to create a new message.
 */
export const GrantSchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 22);

/**
 * Describes the message acyclic.auth.v1.GrantSet.
 * Use `create(GrantSetSchema)` to create a new message.
 */
export const GrantSetSchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 23);

/**
 * Describes the message acyclic.auth.v1.Holder.
 * Use `create(HolderSchema)` to create a new message.
 */
export const HolderSchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 24);

/**
 * Describes the message acyclic.auth.v1.IssuanceRejected.
 * Use `create(IssuanceRejectedSchema)` to create a new message.
 */
export const IssuanceRejectedSchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 25);

/**
 * Describes the message acyclic.auth.v1.IssuanceReply.
 * Use `create(IssuanceReplySchema)` to create a new message.
 */
export const IssuanceReplySchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 26);

/**
 * Describes the message acyclic.auth.v1.IssuanceRequest.
 * Use `create(IssuanceRequestSchema)` to create a new message.
 */
export const IssuanceRequestSchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 27);

/**
 * Describes the message acyclic.auth.v1.IssuanceUnknown.
 * Use `create(IssuanceUnknownSchema)` to create a new message.
 */
export const IssuanceUnknownSchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 28);

/**
 * Describes the message acyclic.auth.v1.KeyCertificate.
 * Use `create(KeyCertificateSchema)` to create a new message.
 */
export const KeyCertificateSchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 29);

/**
 * Describes the message acyclic.auth.v1.KeyRange.
 * Use `create(KeyRangeSchema)` to create a new message.
 */
export const KeyRangeSchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 30);

/**
 * Describes the message acyclic.auth.v1.KeyRevocation.
 * Use `create(KeyRevocationSchema)` to create a new message.
 */
export const KeyRevocationSchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 31);

/**
 * Describes the message acyclic.auth.v1.KeyringSnapshot.
 * Use `create(KeyringSnapshotSchema)` to create a new message.
 */
export const KeyringSnapshotSchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 32);

/**
 * Describes the message acyclic.auth.v1.Limits.
 * Use `create(LimitsSchema)` to create a new message.
 */
export const LimitsSchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 33);

/**
 * Describes the message acyclic.auth.v1.LineageAppend.
 * Use `create(LineageAppendSchema)` to create a new message.
 */
export const LineageAppendSchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 34);

/**
 * Describes the message acyclic.auth.v1.Pin.
 * Use `create(PinSchema)` to create a new message.
 */
export const PinSchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 35);

/**
 * Describes the message acyclic.auth.v1.PlainSecretVariable.
 * Use `create(PlainSecretVariableSchema)` to create a new message.
 */
export const PlainSecretVariableSchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 36);

/**
 * Describes the message acyclic.auth.v1.Principal.
 * Use `create(PrincipalSchema)` to create a new message.
 */
export const PrincipalSchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 37);

/**
 * Describes the message acyclic.auth.v1.ProtectedSecretVariable.
 * Use `create(ProtectedSecretVariableSchema)` to create a new message.
 */
export const ProtectedSecretVariableSchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 38);

/**
 * Describes the message acyclic.auth.v1.RecordAttributes.
 * Use `create(RecordAttributesSchema)` to create a new message.
 */
export const RecordAttributesSchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 39);

/**
 * Describes the message acyclic.auth.v1.ResourceGrant.
 * Use `create(ResourceGrantSchema)` to create a new message.
 */
export const ResourceGrantSchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 40);

/**
 * Describes the message acyclic.auth.v1.ResourcePattern.
 * Use `create(ResourcePatternSchema)` to create a new message.
 */
export const ResourcePatternSchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 41);

/**
 * Describes the message acyclic.auth.v1.Revocation.
 * Use `create(RevocationSchema)` to create a new message.
 */
export const RevocationSchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 42);

/**
 * Describes the message acyclic.auth.v1.RevocationCheckpoint.
 * Use `create(RevocationCheckpointSchema)` to create a new message.
 */
export const RevocationCheckpointSchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 43);

/**
 * Describes the message acyclic.auth.v1.RevocationDelta.
 * Use `create(RevocationDeltaSchema)` to create a new message.
 */
export const RevocationDeltaSchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 44);

/**
 * Describes the message acyclic.auth.v1.SecretDeleteRequest.
 * Use `create(SecretDeleteRequestSchema)` to create a new message.
 */
export const SecretDeleteRequestSchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 45);

/**
 * Describes the message acyclic.auth.v1.SecretListCursor.
 * Use `create(SecretListCursorSchema)` to create a new message.
 */
export const SecretListCursorSchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 46);

/**
 * Describes the message acyclic.auth.v1.SecretListReply.
 * Use `create(SecretListReplySchema)` to create a new message.
 */
export const SecretListReplySchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 47);

/**
 * Describes the message acyclic.auth.v1.SecretListRequest.
 * Use `create(SecretListRequestSchema)` to create a new message.
 */
export const SecretListRequestSchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 48);

/**
 * Describes the message acyclic.auth.v1.SecretMutationReply.
 * Use `create(SecretMutationReplySchema)` to create a new message.
 */
export const SecretMutationReplySchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 49);

/**
 * Describes the message acyclic.auth.v1.SecretPutRequest.
 * Use `create(SecretPutRequestSchema)` to create a new message.
 */
export const SecretPutRequestSchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 50);

/**
 * Describes the message acyclic.auth.v1.SecretVariable.
 * Use `create(SecretVariableSchema)` to create a new message.
 */
export const SecretVariableSchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 51);

/**
 * Describes the message acyclic.auth.v1.SignedAllowance.
 * Use `create(SignedAllowanceSchema)` to create a new message.
 */
export const SignedAllowanceSchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 52);

/**
 * Describes the message acyclic.auth.v1.SignedBlock.
 * Use `create(SignedBlockSchema)` to create a new message.
 */
export const SignedBlockSchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 53);

/**
 * Describes the message acyclic.auth.v1.SignedEnvironmentKeyring.
 * Use `create(SignedEnvironmentKeyringSchema)` to create a new message.
 */
export const SignedEnvironmentKeyringSchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 54);

/**
 * Describes the message acyclic.auth.v1.SignedKeyringSnapshot.
 * Use `create(SignedKeyringSnapshotSchema)` to create a new message.
 */
export const SignedKeyringSnapshotSchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 55);

/**
 * Describes the message acyclic.auth.v1.SignedRevocation.
 * Use `create(SignedRevocationSchema)` to create a new message.
 */
export const SignedRevocationSchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 56);

/**
 * Describes the message acyclic.auth.v1.SocketHostRule.
 * Use `create(SocketHostRuleSchema)` to create a new message.
 */
export const SocketHostRuleSchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 57);

/**
 * Describes the message acyclic.auth.v1.SocketPortRange.
 * Use `create(SocketPortRangeSchema)` to create a new message.
 */
export const SocketPortRangeSchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 58);

/**
 * Describes the message acyclic.auth.v1.SubjectId.
 * Use `create(SubjectIdSchema)` to create a new message.
 */
export const SubjectIdSchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 59);

/**
 * Describes the message acyclic.auth.v1.SubjectLineage.
 * Use `create(SubjectLineageSchema)` to create a new message.
 */
export const SubjectLineageSchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 60);

/**
 * Describes the message acyclic.auth.v1.SubjectReference.
 * Use `create(SubjectReferenceSchema)` to create a new message.
 */
export const SubjectReferenceSchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 61);

/**
 * Describes the message acyclic.auth.v1.TrustRecord.
 * Use `create(TrustRecordSchema)` to create a new message.
 */
export const TrustRecordSchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 62);

/**
 * Describes the message acyclic.auth.v1.Validity.
 * Use `create(ValiditySchema)` to create a new message.
 */
export const ValiditySchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 63);

/**
 * Describes the message acyclic.auth.v1.WebsocketConnectGrant.
 * Use `create(WebsocketConnectGrantSchema)` to create a new message.
 */
export const WebsocketConnectGrantSchema = /*@__PURE__*/
  messageDesc(file_auth_v1_auth, 64);

/**
 * Describes the enum acyclic.auth.v1.AuditDecision.
 */
export const AuditDecisionSchema = /*@__PURE__*/
  enumDesc(file_auth_v1_auth, 0);

/**
 * @generated from enum acyclic.auth.v1.AuditDecision
 */
export const AuditDecision = /*@__PURE__*/
  tsEnum(AuditDecisionSchema);

/**
 * Describes the enum acyclic.auth.v1.IssuanceRefusal.
 */
export const IssuanceRefusalSchema = /*@__PURE__*/
  enumDesc(file_auth_v1_auth, 1);

/**
 * @generated from enum acyclic.auth.v1.IssuanceRefusal
 */
export const IssuanceRefusal = /*@__PURE__*/
  tsEnum(IssuanceRefusalSchema);

/**
 * Describes the enum acyclic.auth.v1.PrincipalKind.
 */
export const PrincipalKindSchema = /*@__PURE__*/
  enumDesc(file_auth_v1_auth, 2);

/**
 * @generated from enum acyclic.auth.v1.PrincipalKind
 */
export const PrincipalKind = /*@__PURE__*/
  tsEnum(PrincipalKindSchema);

/**
 * Describes the enum acyclic.auth.v1.SecretKind.
 */
export const SecretKindSchema = /*@__PURE__*/
  enumDesc(file_auth_v1_auth, 3);

/**
 * @generated from enum acyclic.auth.v1.SecretKind
 */
export const SecretKind = /*@__PURE__*/
  tsEnum(SecretKindSchema);

