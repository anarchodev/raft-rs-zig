# BUG: `e2c4aea` segfaults the threaded host in `confchange::restore` at group creation

**Status:** RESOLVED (2026-06-06) — root cause was a **gitignored `Cargo.lock` re-resolving `raft`'s transitive `hashbrown` to a bad `0.16.1`**, not the source change. Fixed by committing the lockfile (`7ea746d`). See **Resolution** at the bottom.
**Introduced by:** dependency drift, *not* `e2c4aea`'s source (the 23-line block was bisected out and the crash persisted — see Resolution)
**Last-good:** `239bafb` ("WAL segmentation + segment GC")
**Severity:** crashes any threaded host on the first `createGroup` — fully broken, not intermittent.

## Symptom

A threaded host (rove's `rewind` single-node worker) boots fine, then **segfaults
(SIGSEGV / general protection) on the first raft group creation**, the moment the
first write arrives. The host dies; every subsequent request fails.

Backtrace (rove `rewind`, debug build, x86-64):

```
General protection exception (no address available)
  _mm_movemask_epi8                              core_arch sse2.rs:1495
  hashbrown::control::group::sse2  (Group::load) sse2.rs:111
  hashbrown raw::mod.rs:2209  iter<u64,()>
  hashbrown map.rs:651        union<u64,...>
  raft-0.7.0  src/util.rs:142          Union::iter
  raft-0.7.0  src/confchange/changer.rs:292   check_invariants
  raft-0.7.0  src/confchange/changer.rs:277   check_and_copy
  raft-0.7.0  src/confchange/changer.rs:141   simple
  raft-0.7.0  src/confchange/restore.rs:95     restore
  raft-0.7.0  src/raft.rs:374                  Raft::new   (confchange::restore(...))
  raft-0.7.0  src/raw_node.rs:304              RawNode::new
  raft-sys    src/lib.rs:531                   raft_manager_create_group  (RawNode::new)
  src/manager.zig:136                          createGroupEpoch
  <host>  createGroupCore → ensureGroup → pump thread
```

The fault is the **load that feeds `_mm_movemask_epi8`** — hashbrown reading the
control bytes of a `HashSet<u64>` at a bad/garbage pointer while
`check_invariants` iterates the joint voter config built by `confchange::restore`.

## Proven causation (A/B + clean rebuild)

Bisected against the consuming repo (rove) by swapping only the
`build.zig.zon` pin between the two commits and rebuilding:

| raft-rs-zig pin | first write |
|---|---|
| `239bafb` (last-good) | **HTTP 204**, host alive, write durable across restart |
| `e2c4aea` (this commit) | **SIGSEGV** at `confchange::restore`, host dead |

- Reproduces on a **clean from-scratch cargo rebuild** (`rm -rf raft-sys/target`
  then full rebuild) — it is **not** a stale-artifact issue.
- Single-threaded `cargo test` / the host's single-threaded test step **pass**.
  Only the threaded binary crashes. The two regression tests added in `e2c4aea`
  pass.

## Why the obvious explanation is wrong

The commit's own intent (persist `LightReady.commit_index()` so a
compacted+recovered group re-opens) is sound. But note **where** it crashes:

1. **The crash is at group _creation_ (`RawNode::new`), before this group ever
   commits.** The new `process_ready` block (lib.rs:1005–1027) only runs at
   commit time — it has **not executed for this group** when the crash fires.
2. **The WAL is empty at crash time.** On the consuming side the data dir is
   fresh; after boot, before any write, `raft-wal` is **0 bytes**. So the
   crashing `createGroup` is the *first* group, recovering from an *empty* WAL.
   The `conf_state` handed to `RawNode::new` is built from the default voter
   set, by code that is **byte-identical** between `239bafb` and `e2c4aea`.
3. **Source delta is tiny and elsewhere.** `git diff 239bafb..e2c4aea` touches
   only:
   - `raft-sys/src/lib.rs` — the `process_ready` `set_hard_state`-on-commit block
   - `src/manager.zig` — two new tests only
   `raft_manager_create_group`, `grouped_file_storage.zig`, `storage.zig`,
   `file_storage.zig`, the vendored `raft-0.7.0`, and `hashbrown` are all
   **unchanged**. The only transitive lockfile move is `bitflags` 2.12.1 → 2.13.0.

So the new logic is not corrupting any data the crashing path reads — the
crashing path runs first, over an empty WAL and identical conf_state code.

## Conclusion / leading hypothesis

The added Rust code in `lib.rs` (`process_ready`) — and/or the `bitflags`
2.12.1→2.13.0 re-resolve — **shifts the compiled layout of the `raft-0.7.0`
closure** (it all builds into the same `raft.*-cgu.N` codegen units), and that
surfaces a **latent UB / codegen miscompilation in the SSE2 hashbrown path used
by `confchange::restore`**. The garbage control-byte pointer in `Group::load`
is the signature of memory corruption / a bad map header, not a logic error.

This is consistent with every observation: deterministic, threaded-only,
present on a clean rebuild, in a function the new code never calls, over an
empty WAL.

Allocator note worth ruling out: this static lib links into a Zig host. If
Rust (system `malloc`) and the host disagree about the allocator backing the
memory hashbrown reads, layout shifts can flip a latent corruption from benign
to fatal. (Was latent under `239bafb`.)

## Reproduction

In the rove checkout (consumer), branch `v2`:

```bash
# point build.zig.zon raft_rs_zig pin at e2c4aea (the bug) — see hashes below
set -a; . ./.env; set +a            # S3 env; rewind has no fs blob backend
zig build rewind
rm -rf /tmp/rw && ./zig-out/bin/rewind /tmp/rw 18100 &
curl -s -o /dev/null -w '%{http_code}\n' --http2-prior-knowledge -X POST \
  http://127.0.0.1:18100/_system/admin-kv \
  -H 'Host: admin.localhost' \
  -H 'Authorization: Bearer rewindtestroottokenpadding0123456789abcd' \
  -H 'Content-Type: application/json' \
  --data '{"pairs":[{"key":"k","value":"v"}]}'
# e2c4aea: process aborts (core dumped), curl prints 000
# 239bafb: 204
```

Pins (`build.zig.zon`):
- bug:  `git+https://github.com/anarchodev/raft-rs-zig#e2c4aea274fa41d203e11d7a8d0b2f56cc36305e`
- good: `git+https://github.com/anarchodev/raft-rs-zig#239bafb0bfa0f18419ac4df86bfda2324fd27c8e`

## Suggested next diagnostics (for the fix)

The goal is to keep the (correct) commit-index persistence while making the
threaded host stop crashing. Narrow which of the two deltas is load-bearing:

1. **Isolate the lockfile move.** Pin `bitflags = "=2.12.1"` (keep the lib.rs
   change), rebuild, repro. If the crash disappears, it's purely a codegen/dep
   layout effect and the fix is a lockfile pin + an upstream report — the new
   logic is exonerated.
2. **Isolate the code add.** Revert *only* the `lib.rs` `process_ready` block
   (keep `bitflags` 2.13.0), rebuild, repro. Crash gone ⇒ the added code is the
   layout trigger (still a codegen/UB bug, but tells you where).
3. **Sanitizers.** Build `raft-sys` deps with ASan or run the threaded host
   under valgrind to catch the corrupting write upstream of the hashbrown read.
   `confchange::restore` reading a bad `HashSet` ctrl pointer is the
   read-side symptom; the corruption likely happens earlier.
4. **Allocator unification.** Confirm Rust and the Zig host share one allocator
   (e.g. force Rust onto the host's allocator, or both on system malloc) and
   that nothing in the new path frees host-owned or cross-FFI memory.
5. **Reduce.** A minimal threaded harness in this repo that creates a group on
   an empty WAL on a non-main thread (mirroring the host's pump thread) would
   pull the repro in-tree, where the single-threaded tests currently miss it.

## Investigation update (2026-06-06)

An in-tree investigation against this commit confirms the report's
*reasoning* but invalidates its *evidence*, and fails to reproduce the
crash. Net: the attribution to `e2c4aea` is **not yet established**.

### Confirmed

- **The new code cannot be a direct logic cause.** The 23-line addition
  lives entirely inside `raft_manager_process_ready`
  (`raft-sys/src/lib.rs:1004–1026`), guarded by `light_rd.commit_index()`.
  It runs only at commit time; the crash is at `RawNode::new` during
  `createGroup`, which runs first. For the first group the block has
  provably not executed. (§"Why the obvious explanation is wrong" holds.)
- **The source delta is only process_ready + Zig tests.** `createGroup`
  and the `conf_state` path are byte-identical between the two commits.
- **Single-threaded tests pass** at `e2c4aea` on a clean build.

### The original A/B was confounded (now fixed)

The report's A/B swapped only the consumer's `build.zig.zon` pin and
rebuilt. But `raft-sys/Cargo.lock` was **gitignored** and
`raft-sys/Cargo.toml` is **unchanged** between the commits — so each
rebuild re-resolved the *entire* Rust dependency tree against the
registry-of-the-moment. The "bitflags 2.12.1→2.13.0" delta the report
cites was therefore a **build-time drift artifact, not a commit effect**;
a build today resolves bitflags `2.11.1` (neither version named). The
A/B never isolated the 23-line change from incidental dep drift.

Fix: `raft-sys/Cargo.lock` is now committed (`7ea746d`). With it,
`239bafb` and `e2c4aea` resolve **byte-identical dependency trees**
(verified via `cargo tree` in a worktree — only the crate's own path
differs). Any A/B run with this lockfile now isolates exactly the
source delta.

### The in-tree repro (diagnostic #5) does NOT reproduce

A permanent threaded test was added (`src/manager.zig`: "threaded
createGroup over fresh WAL on a spawned thread does not crash") that runs
the full `createGroup → campaign → propose → pump` lifecycle on a
non-main `std.Thread` over a fresh empty WAL — exactly the shape the
report blames. It **passes**. Pushed harder during investigation
(not committed): 16 threads × 8 rounds = 128 overlapping
`RawNode::new`/`confchange::restore`, across **debug, ReleaseFast, and
ReleaseSafe** — all pass. The fault does not reproduce in this repo.

This pushes the cause toward something **specific to the rove host
process** (its allocator, its other live threads, S3/HTTP2 memory
pressure) rather than the raft-rs-zig threaded lifecycle or release-mode
codegen layout. The garbage `HashSet` ctrl pointer is the read-side
symptom of corruption originating elsewhere in the host — consistent
with the allocator note, not with a logic bug on the crashing path.

### The now-valid A/B procedure (run in the rove consumer)

The original repro under §Reproduction is still the right test, but must
now be run with a **pinned lockfile on both sides** so the only variable
is the source delta:

1. For each pin (`239bafb`, `e2c4aea`), check it out and drop the
   committed `raft-sys/Cargo.lock` into place before building, so both
   builds use the identical dependency tree.
2. Run the §Reproduction HTTP write against each.
3. Interpret:
   - **Still 204 vs SIGSEGV** with identical deps ⇒ the 23-line change is
     genuinely implicated (codegen/allocator-interaction, not logic).
     Next: ASan / valgrind on the rove binary to catch the corrupting
     write upstream of the hashbrown read.
   - **Both 204** (crash vanishes once the lockfile is shared) ⇒ the
     original failure was dependency drift; `e2c4aea` is exonerated.

Until step 3 is run, treat the `e2c4aea` attribution as unproven.

## Impact on the consumer

For rove specifically, the bare `e2c4aea` commit (gitignored lock) was
**net-negative**: its purpose is to unblock WAL compaction (`compact_wal`),
which rove keeps **off** for unrelated threaded-integration reasons, *and* its
unpinned lockfile let the build drift onto the crashing `hashbrown 0.16.1`. The
fixed commit `7ea746d` (committed lock) is safe to adopt — see Resolution.

## Resolution (2026-06-06)

The "Investigation update" A/B procedure was run, plus a tighter bisect that
holds the dep tree constant. Conclusion: **`e2c4aea`'s source is exonerated; the
crash was `hashbrown 0.16.1` pulled in by an unpinned lockfile.**

### Bisect: the 23-line block is not the cause

Held *everything* constant (same fetched package, same cached dep tree, same
build env) and removed **only** the `process_ready` commit-index block from the
cached `raft-sys/src/lib.rs`, then rebuilt the rove `rewind` binary and ran the
§Reproduction write. **It still SIGSEGV'd** at the identical
`confchange::restore` site. So the added code is not the trigger — it merely
shipped alongside a dependency re-resolution.

### Root cause: unpinned `Cargo.lock` → `hashbrown 0.16.1`

`raft-sys/Cargo.lock` was gitignored, so every build re-resolved the whole Rust
tree against the registry-of-the-moment. Around the time of `e2c4aea` the
resolver picked **`hashbrown 0.16.1`** for `raft 0.7.0`'s transitive use. The
crashing rove binaries all link `hashbrown-0.16.1`; the GPF is in its SSE2
`Group::load` while `confchange::restore` iterates the voter `HashSet`. Whether
0.16.1 is itself buggy or merely exposes a latent UB/codegen/allocator
interaction under the rove host, **that version is the differentiator** — not
the raft-rs-zig source. (The in-tree threaded test never reproduced because it
resolved a different hashbrown.)

### Fix + verification

`7ea746d` commits `raft-sys/Cargo.lock`, pinning to **`hashbrown 0.15.5` /
`0.17.0`** (zero references to `0.16.1`). Verified end-to-end in the rove
consumer (branch `v2`) by bumping the pin to `7ea746d` and rebuilding clean:

- `scripts/rewind_smoke.py` → **PASS** (writes 204 + restart/durability leg).
- `zig build v2-test` → **green**.
- Direct repro: 3 writes all 204, host stays alive.

(NB: the linked binary still contains a stale `hashbrown-0.16.1` *string* in
debug metadata, but the compiled `raft` now uses the pinned `0.17.0` and the
crash is gone.)

### Follow-ups

- Keep `Cargo.lock` committed (done in `7ea746d`) — the real durable fix; an
  unpinned lock can drift back onto `0.16.1` at any time.
- Optional: file/track an upstream `hashbrown 0.16.1` issue, or add an explicit
  `hashbrown` floor/ceiling in `raft-sys/Cargo.toml` to document the exclusion.
- The `e2c4aea` commit-index-persistence logic is fine and stays.
