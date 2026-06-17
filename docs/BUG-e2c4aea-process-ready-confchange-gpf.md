# BUG: `e2c4aea` segfaults the threaded host in `confchange::restore` at group creation

**Status:** ROOT-CAUSED (2026-06-06, via gdb + objdump) — it is a **debug-build misaligned-`movaps`** on an under-aligned 16-byte SIMD constant in the Rust object, NOT a dependency at all. The committed-`Cargo.lock` "fix" (`7ea746d`) only masks it by relaying the constant onto a 16-byte boundary. The robust fix is to build the Rust staticlib optimized. See **PROVEN root cause (gdb)** at the very bottom; the dependency-drift / `hashbrown` / "unproven crate" sections above are superseded and kept only for history.
**Introduced by:** nothing in raft-rs-zig source — a rustc/LLVM `-O0` codegen + link-layout interaction (see PROVEN root cause). Dependency-version changes only shift binary layout, flipping the constant between a 16-aligned (benign) and 8-aligned (#GP) address.
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
crash in-tree. Combined with the rove-side bisect (see Resolution), this
**exonerates the `e2c4aea` source** and points at unpinned-dependency
drift. It also **rules out the two mechanisms that were proposed** (raft's
runtime hashbrown, and cbindgen/ABI header drift) — see Resolution.

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

It does **not** reproduce because the in-tree build resolves a different
dependency tree than the crashing rove builds did — which is the whole
point: the crash tracks the dependency tree, not the raft-rs-zig source.
The garbage `HashSet` ctrl pointer is the read-side symptom of memory
corruption originating in a drifted dependency, surfacing in raft's
`std::collections::HashSet`.

## Impact on the consumer

For rove specifically, the bare `e2c4aea` commit (gitignored lock) was
**net-negative**: its purpose is to unblock WAL compaction (`compact_wal`),
which rove keeps **off** for unrelated threaded-integration reasons, *and* its
unpinned lockfile let the build drift onto a crashing dependency tree. The
fixed commit `7ea746d` (committed lock) is safe to adopt — see Resolution.

## Resolution (2026-06-06)

The "Investigation update" A/B procedure was run, plus a tighter bisect that
holds the dep tree constant. Conclusion: **`e2c4aea`'s source is exonerated; the
crash was caused by an unpinned lockfile drifting raft's dependency
tree.** The crate originally blamed (`hashbrown 0.16.1`) is *not* the
culprit — see "Root cause" below for the dependency-graph proof.

### Bisect: the 23-line block is not the cause

Held *everything* constant (same fetched package, same cached dep tree, same
build env) and removed **only** the `process_ready` commit-index block from the
cached `raft-sys/src/lib.rs`, then rebuilt the rove `rewind` binary and ran the
§Reproduction write. **It still SIGSEGV'd** at the identical
`confchange::restore` site. So the added code is not the trigger — it merely
shipped alongside a dependency re-resolution.

### Root cause: unpinned `Cargo.lock` (the crate is *not* `hashbrown`)

`raft-sys/Cargo.lock` was gitignored, so every build re-resolved the whole Rust
tree against the registry-of-the-moment. The crash tracks that drift: pin the
tree and it disappears (verification below); leave it unpinned and it can come
back. That part of the original Resolution is correct.

**But `hashbrown` is not the differentiator, and cannot be.** A dependency-graph
audit (`cargo tree`, raft source) rules it out conclusively:

1. **raft 0.7.0 has no external `hashbrown` dependency.** Its deps are
   `bytes, fxhash, getset, protobuf, raft-proto, rand, slog*, thiserror`. The
   crashing `HashSet<u64>::union` (`raft-0.7.0/src/util.rs:142`) uses
   `crate::HashSet`, which raft defines at `lib.rs:602–604` as
   `std::collections::HashSet<K, BuildHasherDefault<fxhash::FxHasher>>`. That is
   **std's** HashSet — the `hashbrown` in the backtrace is the copy **vendored
   into the rustc toolchain**, whose version is fixed by the compiler, *not* by
   `Cargo.lock`. No `Cargo.lock` entry can change it.
2. **The external `hashbrown` crate is build-tool-only.** It is reachable only
   through `cbindgen → indexmap (0.17) / wasmparser (0.15)`.
   `cargo tree -e normal -i hashbrown` prints "nothing to print" for every
   version, and the runtime (normal-edge) tree contains **zero** `hashbrown`
   nodes. cbindgen runs in `build.rs` to regenerate `raft_sys.h`; its code is
   never linked into `libraft_sys.a` or the rove binary. The "stale
   `hashbrown-0.16.1` string in debug metadata" the original Resolution noticed
   is exactly the footprint of a *build-time* tool, not of linked runtime code —
   it corroborates this, rather than the runtime-link claim.
3. **It is not cbindgen/ABI header drift either.** Regenerating `raft_sys.h`
   under the committed lock vs a fresh-resolved tree (cbindgen pinned at 0.27.0
   in both) yields a **byte-identical** 304-line header. So the FFI struct
   layout the Zig `@cImport` sees does not change with the dep tree; an
   ABI-mismatch corruption channel is excluded.

What an unpinned lock *does* drift, in raft's **runtime** closure, includes
`log`, `chrono`, `memchr`, and `zerocopy`/`ppv-lite86` (the last via `rand`,
which raft uses for randomized election timeouts). The true differentiator is
one of these (or an allocator/UB interaction it exposes) — **unproven without
the crashing-era lockfile**. `hashbrown 0.16.1` was at most a marker of the
drifted tree, mistaken for its cause.

### Fix + verification

`7ea746d` commits `raft-sys/Cargo.lock`, freezing the entire dependency tree.
Verified two ways:

- **Header determinism (in-tree).** `raft_sys.h` is byte-identical across the
  committed lock and a fresh resolve — the FFI surface is stable.
- **End-to-end (rove consumer, branch `v2`).** Bumping the pin to `7ea746d` and
  rebuilding clean: `scripts/rewind_smoke.py` → **PASS** (204 + restart/
  durability leg), `zig build v2-test` → **green**, direct repro 3× 204 with the
  host alive.

The fix works because it pins the *whole* tree; it does not depend on
identifying the specific drifted crate.

### Follow-ups

- Keep `Cargo.lock` committed (done in `7ea746d`) — the durable fix. Because it
  is listed in `build.zig.zon`'s `.paths`, consumers fetching this package now
  get the pinned tree.
- **Do not** file an upstream `hashbrown` issue or add a `hashbrown` bound in
  `raft-sys/Cargo.toml` — that would chase a crate that is never linked into the
  consensus code.
- To actually pinpoint the culprit (optional, only if it recurs): recover the
  crashing-era `Cargo.lock` from a rove build that still reproduces, diff it
  against `7ea746d`'s lock, and bisect the differing **runtime** crates
  (`log`/`chrono`/`memchr`/`zerocopy`), e.g. under ASan/valgrind.
- The `e2c4aea` commit-index-persistence logic is fine and stays.

## PROVEN root cause (gdb + objdump, 2026-06-06)

A gdb session on a crashing rove `rewind` (debug build, BAD dep tree) caught the
fault. It is **not** an allocator bug, **not** a specific dependency package,
**not** corrupted/garbage data, and **not** a CPU-flag (MXCSR/AC) issue. It is a
**misaligned aligned-SSE load of an under-aligned static SIMD constant**, only in
the `-O0` (debug) build, with placement decided by binary layout.

### The fault

```
Thread 2 received signal SIGSEGV
#0  _mm_movemask_epi8        sse2.rs:1495
...  hashbrown Group::match_full / RawIterRange::new / RawTable::iter
#11 raft::util::Union::iter  src/util.rs:142   (HashSet::union over the voter set)
#12 confchange::changer::check_invariants
#15 confchange::restore::restore
#16 Raft::new  #17 RawNode::new  #18 raft_manager_create_group
#19 createGroupEpoch  #20 createGroupCore  #21 ensureGroup  #22/23 pump

=> 0x1d7d354 <_mm_movemask_epi8+4>:  movaps -0xcbc0e3(%rip),%xmm0   # 0x10c1278
```

- The faulting instruction is **`movaps`** (16-byte-aligned load *required*) of a
  RIP-relative **static constant** at `0x10c1278`.
- `0x10c1278 & 0xf == 0x8` → the constant is **8-byte aligned, not 16** → `#GP`.
- Registers: **MXCSR = 0x1f80** (default, all exceptions masked) and **EFLAGS =
  0x10206** (AC/alignment-check bit `0x40000` is **clear**). So it is neither an
  FP-exception nor an alignment-check-flag effect — it is a real
  movaps-on-misaligned-16 general-protection fault.
- The operand is a fixed `.rodata` constant (`.Lanon...`), not a heap/FFI pointer
  → not corrupted memory, not garbage conf_state, not the allocator.

### Why it is layout-sensitive (the dep-version "trigger")

`objdump -h` on the **debug** `libraft_sys.a` shows `.rodata.cst16` sections (16-byte
SIMD constants) emitted with **mixed, insufficient alignment**:

```
102 × .rodata.cst16  align 2**4   (16, correct)
 91 × .rodata.cst16  align 2**3   (8  — UNDER-ALIGNED)
  7 × .rodata.cst16  align 2**0   (1!)
  1 × .rodata.cst16  align 2**2
```

The crashing constant (`.Lanon.ffb9a202c598fcdeb2beae8f09e3bf82.4`) is a 16-byte
entry in `.rodata.cst16`. rustc's `-O0` codegen marks many of these sections only
8-byte-aligned while still generating `movaps` against them. The linker honours
the (too-small) 8-byte alignment and packs the section wherever it fits;
**whether it lands on a 16- or merely-8-aligned address is pure binary layout.**
Unrelated dep bumps (chrono/log code-size deltas) shift downstream addresses and
flip that constant across a 16-byte boundary — hence "chrono+log BAD together
crashes, either alone is fine," and hence it only triggers in the large rove
binary, never in the tiny in-tree test (same `libraft_sys.a`, different layout).

### Confirmation

- **Release build does not crash.** Building rove (and thus raft-sys) `--release`
  against the exact BAD tree → 204, host alive, 3/3. In release the intrinsic is a
  single `pmovmskb` with no constant load, so the misaligned `movaps` never exists.
- **The in-tree threaded test never crashes** even with c_allocator + initRecover +
  the BAD tree — because its small binary happens to 16-align the constant.

### The real fix (recommended)

Build the Rust staticlib **optimized regardless of the Zig optimize mode** — e.g.
in raft-rs-zig's `build.zig`, always pass `--release` (or at least
`-C opt-level=1`) to the `cargo build` step, decoupled from the consumer's debug/
release setting. That removes the misaligned-`movaps` codegen permanently and is
layout-independent.

Pinning `Cargo.lock` (`7ea746d`) is **not** a real fix — it only relays the
constant onto a 16-aligned address by luck and can regress on any future code or
dependency change. Keep the committed lock for reproducibility, but the
opt-level fix is what actually closes the bug.

(Upstream-flavoured note: rustc `-O0` emitting `.rodata.cst16` with 8-byte
alignment while issuing `movaps` against it is the underlying toolchain issue;
worth a minimal repro + report if it recurs outside this workaround.)
