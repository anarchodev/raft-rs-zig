# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Zig bindings over TiKV's `raft-rs 0.7`. The consensus engine itself is Rust; this
repo is a thin FFI layer plus Zig-side storage backends. It's consumed as a Zig
package (notably by `rove`, the kvexp/raft data plane — see references to "rewind2"
and "rove" in source comments). Single binary, no network transport built in:
the host drives message routing through `takeMessages`/`step`.

## Build & test

```sh
zig build              # builds raft-sys (cargo) + Zig lib + raft-spike exe
zig build run          # run the demo/smoke-test exe (src/main.zig)
zig build test         # run all Zig unit tests
```

There is no "run a single test" flag — `zig build test` builds one test binary
over the `raft_rs_zig` module (`refAllDecls` in `src/root.zig` pulls in every
file's tests). To narrow, temporarily gate tests with `if (true) return;` or
comment out `refAllDecls`. Tests live inline at the bottom of each `src/*.zig`.

Requires: Zig 0.15+ (`build.zig.zon` sets `minimum_zig_version = "0.15.0"`; uses
the 0.15 `std.ArrayList` unmanaged API — `list.append(allocator, x)`), and a
Rust/cargo toolchain on PATH.

Debug vs release: `zig build` (debug) links `raft-sys/target/debug/`, any
`-Doptimize=Release*` links `target/release/`. `build.zig` picks the cargo
profile dir from the optimize mode — they must agree, and they do automatically.

## Two-piece build pipeline

The single most important thing to understand before touching the build:

1. `build.zig` runs `cargo build` on `raft-sys/`. That cargo build has a
   **build-script dependency on cbindgen** (`raft-sys/build.rs`) which regenerates
   `raft-sys/include/raft_sys.h` from the Rust `#[repr(C)]` / `#[no_mangle]`
   surface on every cargo run.
2. The Zig side `@cImport`s that generated header and links
   `libraft_sys.a`. The static-lib artifact `dependOn`s the cargo step, so a
   consumer's `linkLibrary(raft_dep.artifact("raft_rs_zig"))` transitively
   triggers the whole cargo build.

Consequence: **after editing `raft-sys/src/lib.rs`, the FFI types/functions in
`raft_sys.h` regenerate automatically on the next `zig build`** — you don't hand-edit
the header (it's gitignored, along with `raft-sys/target/` and `Cargo.lock`). If a
Zig `@cImport` symbol is missing, you likely changed a Rust signature and need a
rebuild, or you added a function without `#[no_mangle] pub extern "C"`.

On Linux the Rust runtime forces linking `pthread`/`dl`/`m`/`gcc_s`
(`_Unwind_*`); these are declared on the module in `build.zig` so they propagate
to consumers. `panic = "abort"` in both cargo profiles — a Rust panic aborts the
process rather than unwinding across the FFI boundary.

## Architecture

### The lifecycle contract (do not abstract this away)

raft-rs is purely the consensus state machine — it has **no scheduler and no
"this group has work" signal**. The host drives it on a pump loop, and the Zig
wrapper (`Manager` in `src/manager.zig`) deliberately keeps these steps explicit:

```
tickAll / tickGroups   →  drive election/heartbeat timers
pollReady(buf)         →  get group ids with pending work
processReady(g, cb)    →  persist + apply committed entries via callback
takeMessages(g, cb)    →  drain outbound messages, route to peers' step()
release(g)             →  return slot to IDLE  ← MANDATORY, see below
```

`processReady` fires the apply callback **once per committed entry** on the hot
path — apply callbacks are raw `callconv(.c)` (no closure wrapper) by design.

### Ready-channel / mailbox (the K=10k scaling fix)

`pollReady` does **not** scan all groups. Every op that produces work (`propose`,
`step`, `campaign`, a `tick` that fires something) calls `notify_locked`, which
CAS-flips the group's slot IDLE→NOTIFIED and pushes its id onto a manager FIFO.
`pollReady` drains that FIFO — cost is O(ready count), independent of total group
count. This is modeled on TiKV's batch-system and exists specifically to handle
thousands of mostly-idle groups.

**The `release` invariant is load-bearing:** every group id returned by
`pollReady` must be `release`d before `pollReady` can surface it again. `release`
flips NOTIFIED→IDLE *and* re-checks `has_ready`/outbox, re-notifying if work
landed mid-cycle — this catches the race where a concurrent `notify` saw NOTIFIED
and skipped the push. The pump shape (drain → process each → release each)
satisfies it. Forgetting `release` silently wedges a group out of the channel.

### Migration epoch / fence

Each group carries an `epoch` (incarnation counter, owned/assigned by the control
plane). No-downtime tenant migration re-creates a group id on a new node with a
**fresh raft incarnation** (term/commit reset) — which makes stale in-flight
messages dangerous because their term can exceed the fresh group's. raft's
term-based fencing works *within* an incarnation but not *across* one. `stepFenced`
/ `stepBatch` drop any message stamped with an epoch strictly older than the local
group's. Epoch 0 = never-migrated; `0 < 0` is false so the fence is a free no-op.
`setEpoch` is monotonic (lowering is rejected — it would re-admit fenced traffic).
Birth epoch goes through `createGroupEpoch`, not a post-create `setEpoch`, to close
the create→set window.

### Storage backends (Zig-owned, passed via vtable)

raft-rs's `Storage` trait is implemented in Rust as `FfiStorage`, which trampolines
every call through a `RaftStorageVTable` of C function pointers whose `userdata` is
a Zig storage object. Three implementations, all in `src/`:

- **`MemStorage`** (`storage.zig`) — in-memory log + hard state. The base; the
  other two own a `*MemStorage` for raft-rs's read view and bookkeeping. Entry 0
  is a data-less sentinel at the snapshot index; real entries follow.
- **`FileStorage`** (`file_storage.zig`) — `MemStorage` + an append-only WAL
  sidecar, one fsync per ready cycle via explicit `flush()`. **v1 is
  benchmark-correct only: `init` always truncates, there is no replay.**
- **`GroupedFileStorage` + `SharedWal`** (`grouped_file_storage.zig`) — the real
  multi-tenant backend. **N groups share ONE WAL file with one fsync per pump
  cycle** (fsync is the scarce resource; per-group files would force K fsyncs and
  crater throughput past K>4). Records are interleaved and tagged with `group_id`.
  Supports crash recovery (`SharedWal.open` validates CRCs and truncates at the
  first torn tail; `initRecover` replays each group's records in file order, which
  makes leader-change tail-rewrites resolve to last-authoritative-entry),
  per-group compaction, and WAL segmentation + GC (sealed segments deleted once no
  group has live entries in them; each new segment re-emits cached per-group hard
  state + compaction markers so GC never drops them).

Storage error codes matter to raft-rs: a callback returning **-2 = Compacted**
(asked below `first_index`, fall back to snapshot) vs **-1 = Unavailable** (asked
past the tail, not yet present) — these drive completely different raft behavior;
don't conflate them.

### FFI boundary conventions

- All vtable callbacks and FFI fns return `0` = success, non-zero = error;
  `Manager` maps these to the `Error` enum in `manager.zig`.
- Pointer+len pairs cross the boundary; bytes handed to callbacks (apply data,
  outbound message bytes) are **valid only for the callback's duration** — Rust
  frees/reuses after. Copy in the callback if you need to retain.
- Outbound/inbound raft messages are rust-protobuf-serialized `eraftpb::Message`
  bytes — opaque to Zig. The host ferries `(to, bytes)` from `takeMessages` to the
  recipient's `step`.
- Unknown-group ids are tolerated as no-ops on `processReady`/`takeMessages`/
  `release` — the ready channel may surface ids for groups destroyed since notify.
  Destroyed group ids are **tombstoned** to prevent accidental reuse; intentional
  migration reuse must `clearTombstone` first.

## Known bug (read before touching compaction or `process_ready`)

`BUG-hardstate-commit-not-persisted.md` documents a live durability defect:
`raft_manager_process_ready` persists `ready.hs()` but **drops
`LightReady::commit_index()`**, so a leader's advancing commit index is never
written to the durable hard state. Masked without compaction (recovers commit=0,
re-election papers over it) but a **hard panic** once the log is compacted
(`RawNode::new` asserts `first_index-1 <= hs.commit`). The file contains root
cause, a runnable repro, and the fix. The `rove` consumer gates WAL compaction off
(`Node.compact_wal = false`) until this is fixed.
