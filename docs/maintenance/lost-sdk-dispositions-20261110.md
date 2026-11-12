> Historical rescue inventory from PR #3328 at `58aa711768ca68b3555df94235318d248eebae95`, recorded on 2026-11-10. Counts and dispositions below belong to that snapshot. Operator-local paths are omitted; the original source remains available at [the recorded commit](https://github.com/acyclic-labs/sdk/blob/58aa711768ca68b3555df94235318d248eebae95/docs/maintenance/lost-sdk-dispositions-20261110.md). Query `foreman lost --repo sdk` for current status.

# SDK lost-work dispositions — 2026-11-10

Initial authoritative sweep: 42 lost PR entries and 40 at-risk worktrees. After exact-head rescues: 42 lost PR entries and **zero at-risk worktrees**, verified by a fresh foreman lost --repo sdk --json invocation. This is preservation evidence, not validation or a claim of semantic equivalence with main.

## Foreman mechanism and remaining owner work

The historical audit used the public Foreman lost-work report. Closed PR survivor notes match “Superseded by #N” / “Contained in #N” and are re-proven hunk by hunk. CONTAINED requires a full commit on main and every hunk there; RECOVERED requires a merged PR on main; RETIRED requires a main SHA and a substantive retirement reason. OWNED applies to worktrees, not lost PR hunks. There is no supported PR disposition for an exact head preserved on a pushed branch. No Foreman code or claims state was edited; no work was falsely retired or discarded.

Coordinator / Foreman owner: consume these exact-head rescue dispositions through a branch-aware, re-proven preservation mechanism. The requested zero-unexplained result remains unachieved in Foreman: its current output still reports 42 lost entries. Preserve those warnings until this evidence can be represented; do not blanket-retire recoverable work. The 38 PR entries outside the already assigned work below are fully rescued, including every missing hunk, because origin holds the original unchanged commit and all reachable objects.

The rescue tables below retain the exact original commits and branch inventory. PR #3328 at the recorded commit retains the complete historical report and its evidence-file names.

## Historical PR rescue dispositions

| PR | Exact original commit preserved on origin | Rescue branch |
| --- | --- | --- |
| #2005 | ece7679967c809f2652c2f44247ec3c0e80bd7cb | wip/rescue-ece7679967c8-20261110 |
| #2267 | 0367d2131c05d88e68e29f4ac8f1c05ccecf1548 | wip/rescue-0367d2131c05-20261110 |
| #2322 | d935c966b39424e6c9b7e4abf609c95000b3e907 | wip/rescue-d935c966b394-20261110 |
| #2539 | 7cf023a101d62918f5ebc83b6024faafcac12a0e | wip/rescue-7cf023a101d6-20261110 |
| #2552 | 8e4c70a1d1f77bf82a98f24b2e1cc9e0e9f3feb6 | wip/rescue-8e4c70a1d1f7-20261110 |
| #2562 | 01508569a4c9bd6f6b339c9b380eec0c5ab641c6 | wip/rescue-01508569a4c9-20261110 |
| #2570 | 4aad1164e6df78eb450667d7951b2f3c59eec1a6 | wip/rescue-4aad1164e6df-20261110 |
| #2575 | 2b03eb373af172864c9e6f8f7d3a258303fdc73d | wip/rescue-2b03eb373af1-20261110 |
| #2649 | 71d120b70b2ac9724a3ecba3c7c7fa99bda78c01 | wip/rescue-71d120b70b2a-20261110 |
| #2657 | 7682ef92b3e3697d73b9bd850b6c79e00865b37b | wip/rescue-7682ef92b3e3-20261110 |
| #2659 | a37e8a32d3349e0dc2f264787c3d81eac3032054 | wip/rescue-a37e8a32d334-20261110 |
| #2684 | 4eed34d1fbaf1d65daaa2930f2f4faf04359ac13 | wip/rescue-4eed34d1fbaf-20261110 |
| #2716 | d3802b637f0203ef85d6254d2f11a19597a97aa8 | wip/rescue-d3802b637f02-20261110 |
| #2723 | 034046cb85bd878787d28c31e3bfc97764353a56 | wip/rescue-034046cb85bd-20261110 |
| #2742 | ab1c8f7169e642a3f318f2442d3567e41e27dc1d | wip/rescue-ab1c8f7169e6-20261110 |
| #2745 | ab042b05659d4a3d42f34c2898fc2e72811c2702 | wip/rescue-ab042b05659d-20261110 |
| #2761 | b33c12ad95e0736ba96e35eb7ce264d7e6a2eb0b | wip/rescue-b33c12ad95e0-20261110 |
| #2771 | 6357488933a4eab01e051f7000faf796ee38fe87 | wip/rescue-6357488933a4-20261110 |
| #2782 | 4939f29fb6a7dd7cc71975e814aa7166a97de51c | wip/rescue-4939f29fb6a7-20261110 |
| #2844 | 64ecf170b8a3a779be3b58e61b04e83b722ad3fa | wip/rescue-64ecf170b8a3-20261110 |
| #2892 | 6423d26095bfeee02848e5d17463973692e79b0c | wip/rescue-6423d26095bf-20261110 |
| #2894 | a65fe02e25152cd7e526f96069300edafc1322e9 | wip/rescue-a65fe02e2515-20261110 |
| #2898 | 5f15223621ef8bd0a5ce995da75dab1b0e661cb5 | wip/rescue-5f15223621ef-20261110 |
| #2899 | 563acf144a0d87f28b3b950b474859ca8bf4d66d | wip/rescue-563acf144a0d-20261110 |
| #2939 | e2b87ae262fb3a2270b31b2df160f7c02bb6cbee | wip/rescue-e2b87ae262fb-20261110 |
| #2946 | 567433e3338b1f9e2053794a31e12d21ea4b77ab | wip/rescue-567433e3338b-20261110 |
| #2953 | 61031d5eaccd39632d77b77e39507e4678b03e1d | wip/rescue-61031d5eaccd-20261110 |
| #2954 | b01080eba4ac112a595ff98ad8818b4c1cca93ff | wip/rescue-b01080eba4ac-20261110 |
| #2977 | 308fa4e466823248fa61d0b49319109547dc0e70 | wip/rescue-308fa4e46682-20261110 |
| #2980 | 90de9a85d7c74c8581be593996f4bfd330b0d788 | wip/rescue-90de9a85d7c7-20261110 |
| #2990 | fb311c63700b7fe19b6adc40f9c6e12dea5fdc5e | wip/rescue-fb311c63700b-20261110 |
| #2992 | 27bba75caf2eebd37ed510a70d104a7fc5ce7f1b | wip/rescue-27bba75caf2e-20261110 |
| #2993 | 9a5d57541b32d6a4f1ddeb8ce0729ca33759d08a | wip/rescue-9a5d57541b32-20261110 |
| #3009 | 2db27dc21a8dc38bb975579af8e75103bed99450 | wip/rescue-2db27dc21a8d-20261110 |
| #3147 | 507747da65470b632cb32ff1f7940d3fff118367 | wip/rescue-507747da6547-20261110 |
| #3170 | 478c800304e995b12222c004ea649e87636af9f4 | wip/rescue-478c800304e9-20261110 |
| #3252 | 563acf144a0d87f28b3b950b474859ca8bf4d66d | wip/rescue-563acf144a0d-20261110 |
| #3277 | 59a094c5c89d1201d67970d095f7608255e90bd8 | wip/rescue-59a094c5c89d-20261110 |

## Worktree rescue dispositions

HEAD was read directly from each named worktree; no working files, indexes or branches were changed. Each origin rescue ref was verified by ls-remote after pushing. Rescued heads preserve all unpushed ancestors.

| Worktree | Previously unpushed | Exact preserved HEAD | Origin rescue branch |
| --- | --- | --- | --- |
| del-core-contracts-current-next-20261108-wt | 34 | 56cf4717f05297763c413cfdeda25a966ef625f2 | wip/rescue-56cf4717f052-20261110 |
| del-core-contracts-next-20261108-wt | 31 | 9448d0814c3f76276f9c4e51d5336102126b3963 | wip/rescue-9448d0814c3f-20261110 |
| del-core-fold-contracts-20261108-wt | 29 | 6633d28b2756577dd064676f9ee453b7c40b7db2 | wip/rescue-6633d28b2756-20261110 |
| del-core-fold-filesystem-20261108-wt | 23 | 1bfa8df599e97288ddd6e6d8ce9875219e27d025 | wip/rescue-1bfa8df599e9-20261110 |
| del-core-rename-next-20261108-wt | 25 | 38641b7541b412986d07cf7c7d5f0a787a4a4318 | wip/rescue-38641b7541b4-20261110 |
| del-core-rename-next-current-20261108-wt | 27 | a8daf401e7f20e9f989533884b75483e02e078f3 | wip/rescue-a8daf401e7f2-20261110 |
| del-core-rename-review-20261108-wt | 25 | 6500ec864faa47f9b1ca4f49bf99f6741c587fc7 | wip/rescue-6500ec864faa-20261110 |
| hakari-wt | 1 | f361a44bb8f08c2aa0330a300cc5bd07a16824b0 | wip/rescue-f361a44bb8f0-20261110 |
| c3125 | 70 | ddd092b738c795e296db03895efb6d97112b4e44 | wip/rescue-ddd092b738c7-20261110 |
| cloud-pin-gaps | 1 | 75523dca295db809450ca2310089efbcab55c25a | wip/rescue-75523dca295d-20261110 |
| feature-unification | 1 | 648bde9188ac407a71f3428203b69d6dfaff4066 | wip/rescue-648bde9188ac-20261110 |
| fix-harness-ts-paths-20261108 | 2 | 408c137bf0686f053506021848438a97439d3053 | wip/rescue-408c137bf068-20261110 |
| floor-webdocs-20261109 | 2 | 6f701f2208cb12a22451f49075f6161ddbfa3978 | wip/rescue-6f701f2208cb-20261110 |
| fuse-owner-death | 1 | e0b490e1d42770fef968f067e2d1d827f4ba0f64 | wip/rescue-e0b490e1d427-20261110 |
| gc-doctor-20261109 | 3 | 072f1813d72623d151d677129822c6baafbe10e7 | wip/rescue-072f1813d726-20261110 |
| green-pr-owners-20261109 | 1 | b8c210911ea02dd305547d0f8d97b8c71f803d4c | wip/rescue-b8c210911ea0-20261110 |
| js-account-leaf | 4 | 6a08f6bdbb3e6e16c95e853cf2bea5aaf90e3bd2 | wip/rescue-6a08f6bdbb3e-20261110 |
| land-3049 | 1 | b8c210911ea02dd305547d0f8d97b8c71f803d4c | wip/rescue-b8c210911ea0-20261110 |
| land-3137 | 1 | e89b5aa437f0a55079bf2522ba2842fe459dec8f | wip/rescue-e89b5aa437f0-20261110 |
| land-3189 | 1 | 1e686c21a0a4823c24a0e827cac29e18df3df5c0 | wip/rescue-1e686c21a0a4-20261110 |
| live-inputs-2 | 8 | e9e6fc8464d8a981bbb7a74ed3192f5efdb4911e | wip/rescue-e9e6fc8464d8-20261110 |
| machines-daytona | 5 | 4abd3f2791c67a2d68566a561a24317ba50bae94 | wip/rescue-4abd3f2791c6-20261110 |
| machines-fork-results | 1 | 612e34ec15f514928d0e4b3dbcb535e719092ba4 | wip/rescue-612e34ec15f5-20261110 |
| mainred-disabled-compaction | 3 | dfb62814a8791263b0b21e7caadf749abe252981 | wip/rescue-dfb62814a879-20261110 |
| mainred-windows-fs | 3 | bc31bf6e5913c5d5e3a4d4a7741a5daa76aaa547 | wip/rescue-bc31bf6e5913-20261110 |
| metadata-generated-copy | 1 | 36381b6f8fbcafd6465ff28a1981743277d865e4 | wip/rescue-36381b6f8fbc-20261110 |
| parked-graphcoder-retained | 1 | f5c58143cc77999abbe953dd25087a445f68d710 | wip/rescue-f5c58143cc77-20261110 |
| pr-2977-20261109 | 118 | bf9f86e91147856a7f505b1bccb3403be933a937 | wip/rescue-bf9f86e91147-20261110 |
| pr-3004-20261109 | 19 | b6ff257c2caa545695d3cd3e469df6f7613988bd | wip/rescue-b6ff257c2caa-20261110 |
| pr-3103-20261109 | 7 | 4b59313fcfdfb54e33ae9cb1621ae30d73138166 | wip/rescue-4b59313fcfdf-20261110 |
| revive-core-misc-1-20261109 | 6 | 2133959d9d3ed29c64b0582291106fdb3e4bfe01 | wip/rescue-2133959d9d3e-20261110 |
| revive-fs-1-20261109 | 7 | b2da61a5148efb93567dbadbfff93e99f08c3629 | wip/rescue-b2da61a5148e-20261110 |
| revive-fs-2-20261109 | 6 | 8654aa1c2b2fb4cfe90f51472cb9d538c93a06a7 | wip/rescue-8654aa1c2b2f-20261110 |
| revive-fs-4-20261109 | 1 | a04964af01d499ff772a80aba307f2b7c06a5239 | wip/rescue-a04964af01d4-20261110 |
| revive-sessions-3-20261109 | 6 | b461460df06d5285155e16fd3a7c8714db2c4a9e | wip/rescue-b461460df06d-20261110 |
| revive-sessions-4-20261109 | 3 | 6156b9fc5479f7c42d9389243db9744da9958257 | wip/rescue-6156b9fc5479-20261110 |
| stream-status-export | 1 | be1c85c0e10e43da666b384c9f2784e02805202f | wip/rescue-be1c85c0e10e-20261110 |
| winred-fs | 2 | d1ac035ef42709f05279c05ca380c09853465dda | wip/rescue-d1ac035ef427-20261110 |
| winred-harness | 1 | f9e96364cde4131571317b9522c16531a899ae63 | wip/rescue-f9e96364cde4-20261110 |
| winred-proto | 1 | 6b72990e22fc05af23286bc8ebc29d0d25b220a5 | wip/rescue-6b72990e22fc-20261110 |

## Already assigned items: verify only

No comments, branches, claims or PR bodies changed for these four items. These are named successors and design evidence, not hunk-equivalence certifications.

- #3227 → [#3324](https://github.com/acyclic-labs/sdk/pull/3324), open at e77d29e2bcc28390f699f399f136bd431df4dc23, origin/fix/live-context-epoch-delta-20261110 verified. Closed original names “Superseded by #3324” in issuecomment-6761947473. Foreman still reports five hunks absent across main and #3324; owner must reconcile equivalent rewrites or remaining delta.
- #3260 → [#3326](https://github.com/acyclic-labs/sdk/pull/3326), open at f5c58143cc77999abbe953dd25087a445f68d710, origin/fix/graphcoder-retained-behaviors-20261110 verified. Closed original names #3326 in issuecomment-6762028650 and explicitly retires plugin relocation in favor of the separate preset/binary required by AGENTS.md. The two retained behaviors are in #3326; Windows entry guarantee is cited at main c302d2e. This design retirement is prose, not a supported RETIRED claim; only the owning task should formalize it.
- #3306 → [#3319](https://github.com/acyclic-labs/sdk/pull/3319), open at 963ee70f4c1f4494f99b013d941ca809a20a3dba, origin/c2/w-nofuse verified. Successor #3319 body explicitly names #3306 and original unlanded delta through 0380acdaaa67. Original closed PR last notes only say follow-up required; **no survivor named on original**. c2w-nofuse owner must name #3319 there.
- #3307 → [#3322](https://github.com/acyclic-labs/sdk/pull/3322), open at 3e0cbe85b4a411363748662ea8472f338a87725e, origin/c2/w-sandbox verified. Successor #3322 body explicitly names #3307 and distinguishes reverted initial integration from subsequent fixes. Original closed PR last notes only say follow-up required; **no survivor named on original**. c2w-sandbox owner must name #3322 there.

## Scope and validation

No Rust or product files changed, so no compile/test run was needed. Validation was exact origin-ref equality for every rescue and named successor, plus the fresh Foreman sweep. Neither primary SDK nor Cloud checkout was accessed as a working directory; no worktree or branch was deleted; original commits and signatures were not rewritten. WORK.md has no lost-sweep-sdk row, so no unrelated row was edited.
