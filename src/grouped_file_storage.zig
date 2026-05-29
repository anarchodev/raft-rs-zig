//! Multi-group file-backed Storage — N raft groups share ONE WAL file
//! per node, with one fsync per pump cycle amortizing every group's
//! writes. Built as `SharedWal` (the shared file handle) + per-group
//! `GroupedFileStorage` (the instance raft-rs sees through its
//! storage vtable).
//!
//! Why interleaved: fsync is the scarce resource. A per-group
//! FileStorage design forces K fsyncs per pump cycle (each
//! processReady ends with that group's flush). On btrfs / nvme that's
//! ms-scale work per fsync; K>4 craters throughput. Interleaving lets
//! one fsync amortize every group's writes — the only shape that
//! scales multi-tenant raft.
//!
//! Record format extends `FileStorage`'s with a `group_id` field:
//!
//!     record  = [tag:u8][group_id:u64 LE][payload_len:u32 LE][payload][crc32:u32 LE]
//!     tag = 1 → entry
//!           payload = [entry_type:u32][term:u64][index:u64]
//!                     [data_len:u32][data]
//!                     [context_len:u32][context]
//!                     [sync_log:u8]
//!     tag = 2 → hardstate
//!           payload = [term:u64][vote:u64][commit:u64]   (24 bytes)
//!     tag = 3 → confstate
//!           payload = [voters_len:u32][voters: u64*N]
//!                     [learners_len:u32][learners: u64*M]
//!
//! CRC32 is `std.hash.Crc32` (IEEE 802.3) over `tag || group_id ||
//! payload_len || payload`.
//!
//! `flush()` lives on `SharedWal`, not `GroupedFileStorage` — the
//! dispatcher calls it once per pump cycle after every ready group has
//! run processReady. No per-group flush is exposed by the vtable.
//!
//! ## Recovery (replay-from-existing)
//!
//! `SharedWal.open` is the crash-recovery constructor (vs `init`, which
//! truncates fresh). It walks the existing file once, CRC-validates each
//! record, and **stops at the first torn or corrupt record** — a
//! half-written tail from a crash mid-append. Everything up to that
//! point is the durable prefix; the torn tail is physically truncated
//! and the write head positioned at the prefix end so the next append
//! continues cleanly.
//!
//! While scanning it buckets the recovered records by `group_id`, in
//! file order. `GroupedFileStorage.initRecover` then drains its group's
//! bucket and replays the records through the same append path a live
//! group uses. That is what makes **tail-truncate** correct on replay:
//! a leader-change rewrite of an uncommitted suffix was *appended* to
//! the shared file (it could not physically rewind — other groups had
//! appended past the rewritten region, so a `setEndPos` would orphan
//! their entries). Replaying records in file order feeds the rewrite
//! through `MemStorage.appendOne`'s "index <= last ⇒ truncate then
//! append" rule, so the later record supersedes the earlier one at the
//! same index — last-authoritative-entry per group falls out for free,
//! and the rebuilt `entry_offsets` point at the surviving records.
//!
//! Out of scope for this slice (separate work, noted so nobody assumes
//! it's done): **segment GC** and a **per-group compaction watermark**.
//! Those need the log to be *segmented* (it is one file today) and to
//! persist snapshots (snapshot bytes are not WAL-backed yet), so they
//! are a distinct piece, not part of recovery. Until then the full log
//! is always present, so replay reconstructs the complete entry stream
//! plus the latest hard state — which is exactly correct for a log that
//! never compacts.

const std = @import("std");
const c = @cImport({
    @cInclude("raft_sys.h");
});
const mem_storage = @import("storage.zig");

pub const MemStorage = mem_storage.MemStorage;

pub const Tag = enum(u8) {
    entry = 1,
    hardstate = 2,
    confstate = 3,
};

/// Length of the per-record header: tag + group_id + payload_len.
const HEADER_LEN: usize = 1 + 8 + 4;
/// Length of the per-record trailer: crc32.
const TRAILER_LEN: usize = 4;

/// Upper bound on a single record's payload during recovery. A length
/// field larger than this is treated as corruption (torn tail), not a
/// real record — it bounds recovery memory against a garbage length
/// read from a half-written header. Generous vs any real raft entry.
const MAX_REPLAY_PAYLOAD: u32 = 64 * 1024 * 1024;

/// One record recovered from the WAL during `SharedWal.open`, awaiting
/// replay into its group's storage. Owns its payload copy (the scan
/// buffer is reused); freed when the group drains its bucket in
/// `initRecover`, or by `SharedWal.deinit` for any group that never
/// re-created (an orphaned bucket).
pub const RecoveredRecord = struct {
    tag: Tag,
    /// Byte offset of this record's start in the file — becomes the
    /// group's `entry_offsets` slot for entry records.
    offset: u64,
    /// Owned copy of the record payload (body only, no header/trailer).
    payload: []u8,
};

/// One shared WAL file across all groups on a single cluster-node.
/// Construction + flush are the dispatcher's responsibility; group
/// storages borrow `*SharedWal` via `GroupedFileStorage.init` and
/// write through `appendRecord`.
///
/// Single-threaded by contract: the dispatcher pumps one ready group
/// at a time per node, so the file handle + offset + scratch buffer
/// are accessed serially. If the dispatcher ever parallelizes per-
/// group processReady, this struct needs a mutex on `wal_offset` +
/// `file.writevAll` (and per-group scratch — already on
/// `GroupedFileStorage`, not here).
pub const SharedWal = struct {
    allocator: std.mem.Allocator,
    file: std.fs.File,
    wal_path: []u8,
    /// Monotonic write head — used to record per-group entry offsets
    /// before each append so the per-group map stays in lockstep.
    wal_offset: u64,
    /// Records recovered by `open`, bucketed by `group_id` in file
    /// order, awaiting replay. Empty after `init` (fresh start). Each
    /// group drains its bucket in `GroupedFileStorage.initRecover`;
    /// `deinit` frees any bucket that was never drained.
    recovered: std.AutoHashMap(u64, std.ArrayList(RecoveredRecord)),

    /// Fresh start: truncate any existing file to empty. Use `open`
    /// instead to recover an existing WAL across a restart. (Tests,
    /// benchmarks, and any caller that genuinely wants a clean slate
    /// use this; production boot uses `open`.)
    pub fn init(allocator: std.mem.Allocator, wal_path: []const u8) !*SharedWal {
        const self = try allocator.create(SharedWal);
        errdefer allocator.destroy(self);

        const path_dup = try allocator.dupe(u8, wal_path);
        errdefer allocator.free(path_dup);

        const file = try std.fs.cwd().createFile(wal_path, .{
            .truncate = true,
            .read = false,
        });
        errdefer file.close();

        self.* = .{
            .allocator = allocator,
            .file = file,
            .wal_path = path_dup,
            .wal_offset = 0,
            .recovered = std.AutoHashMap(u64, std.ArrayList(RecoveredRecord)).init(allocator),
        };
        return self;
    }

    /// Crash-recovery start: open an existing WAL (or create it if
    /// absent), replay-scan it, truncate any torn tail, and position
    /// the write head at the durable prefix end. Recovered records are
    /// bucketed by group in `self.recovered`; callers re-create each
    /// group via `GroupedFileStorage.initRecover`, which drains the
    /// matching bucket. See the module doc comment for the recovery
    /// model and what it deliberately does not yet cover.
    pub fn open(allocator: std.mem.Allocator, wal_path: []const u8) !*SharedWal {
        const self = try allocator.create(SharedWal);
        errdefer allocator.destroy(self);

        const path_dup = try allocator.dupe(u8, wal_path);
        errdefer allocator.free(path_dup);

        // truncate=false → preserve existing bytes; read=true → allow
        // the positional reads the scan needs. Creates an empty file on
        // first boot.
        const file = try std.fs.cwd().createFile(wal_path, .{
            .truncate = false,
            .read = true,
        });
        errdefer file.close();

        var recovered = std.AutoHashMap(u64, std.ArrayList(RecoveredRecord)).init(allocator);
        errdefer freeRecovered(allocator, &recovered);

        const valid_end = try scanForReplay(allocator, file, &recovered);

        // Drop the torn tail (if any) and position the write head at the
        // durable prefix end so the next `appendRecord` continues cleanly.
        try file.setEndPos(valid_end);
        try file.seekTo(valid_end);

        self.* = .{
            .allocator = allocator,
            .file = file,
            .wal_path = path_dup,
            .wal_offset = valid_end,
            .recovered = recovered,
        };
        return self;
    }

    pub fn deinit(self: *SharedWal) void {
        self.file.close();
        const a = self.allocator;
        freeRecovered(a, &self.recovered);
        a.free(self.wal_path);
        a.destroy(self);
    }

    /// Hand a group its recovered records (ownership transfers to the
    /// caller, which must free each `payload` and `deinit` the list).
    /// Removes the bucket from `recovered` so `deinit` won't double-free
    /// it. Returns null for a group with nothing to replay.
    pub fn takeRecovered(self: *SharedWal, group_id: u64) ?std.ArrayList(RecoveredRecord) {
        if (self.recovered.fetchRemove(group_id)) |kv| return kv.value;
        return null;
    }

    /// fsync the WAL. The dispatcher calls this ONCE per pump cycle
    /// after every ready group has run processReady (and every
    /// vtable write callback has returned). This is the load-bearing
    /// "one fsync amortizes K groups" point — the entire reason
    /// SharedWal exists.
    pub fn flush(self: *SharedWal) !void {
        try self.file.sync();
    }

    /// Append one record to the shared file. Returns the file offset
    /// the record was written at — the caller's per-group
    /// `entry_offsets` records this so a later tail-truncate can map
    /// "entry index N" back to its byte range.
    ///
    /// `payload` is the body bytes only; this function frames the
    /// header (tag + group_id + payload_len) and trailer (crc32).
    pub fn appendRecord(
        self: *SharedWal,
        group_id: u64,
        tag: Tag,
        payload: []const u8,
    ) !u64 {
        var header: [HEADER_LEN]u8 = undefined;
        header[0] = @intFromEnum(tag);
        std.mem.writeInt(u64, header[1..9], group_id, .little);
        std.mem.writeInt(u32, header[9..13], @intCast(payload.len), .little);

        var crc = std.hash.Crc32.init();
        crc.update(header[0..]);
        crc.update(payload);
        var trailer: [TRAILER_LEN]u8 = undefined;
        std.mem.writeInt(u32, &trailer, crc.final(), .little);

        const offset_before = self.wal_offset;
        var iov = [_]std.posix.iovec_const{
            .{ .base = &header, .len = header.len },
            .{ .base = if (payload.len == 0) undefined else payload.ptr, .len = payload.len },
            .{ .base = &trailer, .len = trailer.len },
        };
        try self.file.writevAll(iov[0..]);
        self.wal_offset += HEADER_LEN + payload.len + TRAILER_LEN;
        return offset_before;
    }
};

/// Free every recovered bucket and the map itself. Used both by
/// `SharedWal.deinit` (for buckets no group claimed) and by `open`'s
/// errdefer if recovery fails partway.
fn freeRecovered(
    allocator: std.mem.Allocator,
    recovered: *std.AutoHashMap(u64, std.ArrayList(RecoveredRecord)),
) void {
    var it = recovered.iterator();
    while (it.next()) |entry| {
        for (entry.value_ptr.items) |r| allocator.free(r.payload);
        entry.value_ptr.deinit(allocator);
    }
    recovered.deinit();
}

/// pread `buf.len` bytes at `offset`, looping over short reads. Returns
/// the count actually read — less than `buf.len` only at end-of-file
/// (a torn tail), which the caller reads as the recovery boundary.
fn readFull(file: std.fs.File, buf: []u8, offset: u64) !usize {
    var total: usize = 0;
    while (total < buf.len) {
        const n = try file.pread(buf[total..], offset + @as(u64, total));
        if (n == 0) break;
        total += n;
    }
    return total;
}

/// Walk the WAL from offset 0, CRC-validating each record and bucketing
/// the valid ones by `group_id` (in file order) into `recovered`.
/// Stops at the first record that is short-read or fails its CRC — that
/// is the crash boundary; everything before it is the durable prefix.
/// Returns the byte offset of that boundary (the durable prefix length).
fn scanForReplay(
    allocator: std.mem.Allocator,
    file: std.fs.File,
    recovered: *std.AutoHashMap(u64, std.ArrayList(RecoveredRecord)),
) !u64 {
    var off: u64 = 0;
    var header: [HEADER_LEN]u8 = undefined;
    var trailer: [TRAILER_LEN]u8 = undefined;
    while (true) {
        const hn = try readFull(file, &header, off);
        if (hn < HEADER_LEN) break; // clean EOF (hn==0) or torn header

        const tag: Tag = switch (header[0]) {
            1 => .entry,
            2 => .hardstate,
            3 => .confstate,
            else => break, // unknown tag — garbage from a torn write
        };
        const group_id = std.mem.readInt(u64, header[1..9], .little);
        const payload_len = std.mem.readInt(u32, header[9..13], .little);
        if (payload_len > MAX_REPLAY_PAYLOAD) break; // garbage length → torn

        const payload = try allocator.alloc(u8, payload_len);
        var keep_payload = false;
        defer if (!keep_payload) allocator.free(payload);

        const pn = try readFull(file, payload, off + HEADER_LEN);
        if (pn < payload_len) break; // torn payload
        const tn = try readFull(file, &trailer, off + HEADER_LEN + payload_len);
        if (tn < TRAILER_LEN) break; // torn trailer

        var crc = std.hash.Crc32.init();
        crc.update(header[0..]);
        crc.update(payload);
        if (crc.final() != std.mem.readInt(u32, trailer[0..], .little)) break; // corrupt

        const gop = try recovered.getOrPut(group_id);
        if (!gop.found_existing) gop.value_ptr.* = .empty;
        try gop.value_ptr.append(allocator, .{ .tag = tag, .offset = off, .payload = payload });
        keep_payload = true; // ownership moved into the bucket

        off += HEADER_LEN + payload_len + TRAILER_LEN;
    }
    return off;
}

/// Decode an `entry`-record payload (written by `writeEntryRecord`)
/// back into a `RaftEntryFfi`. The returned `data`/`context` pointers
/// borrow `payload` — valid only while `payload` lives (the caller's
/// `MemStorage` append copies them). Bounds are checked at every step;
/// a malformed payload (shouldn't happen post-CRC, but defensive)
/// returns an error rather than reading out of bounds.
fn parseEntryPayload(payload: []const u8) !c.RaftEntryFfi {
    if (payload.len < 4 + 8 + 8 + 4) return error.TruncatedEntryRecord;
    const entry_type = std.mem.readInt(u32, payload[0..4], .little);
    const term = std.mem.readInt(u64, payload[4..12], .little);
    const index = std.mem.readInt(u64, payload[12..20], .little);
    const data_len: usize = std.mem.readInt(u32, payload[20..24], .little);

    var p: usize = 24;
    if (p + data_len > payload.len) return error.TruncatedEntryRecord;
    const data = payload[p .. p + data_len];
    p += data_len;

    if (p + 4 > payload.len) return error.TruncatedEntryRecord;
    const context_len: usize = std.mem.readInt(u32, payload[p..][0..4], .little);
    p += 4;
    if (p + context_len > payload.len) return error.TruncatedEntryRecord;
    const context = payload[p .. p + context_len];
    p += context_len;

    if (p + 1 > payload.len) return error.TruncatedEntryRecord;
    const sync_log = payload[p] != 0;

    return .{
        .entry_type = entry_type,
        .term = term,
        .index = index,
        .data = if (data_len > 0) data.ptr else null,
        .data_len = data_len,
        .context = if (context_len > 0) context.ptr else null,
        .context_len = context_len,
        .sync_log = sync_log,
    };
}

/// Decode a `hardstate`-record payload (24 bytes: term, vote, commit).
fn parseHardStatePayload(payload: []const u8) !c.RaftHardStateFfi {
    if (payload.len < 24) return error.TruncatedHardStateRecord;
    return .{
        .term = std.mem.readInt(u64, payload[0..8], .little),
        .vote = std.mem.readInt(u64, payload[8..16], .little),
        .commit = std.mem.readInt(u64, payload[16..24], .little),
    };
}

/// Per-group storage on top of `SharedWal`. raft-rs sees this through
/// the `vtable` below; `*GroupedFileStorage` is the userdata pointer
/// passed to `Manager.createGroup`. Each group has its own
/// `MemStorage` (its read view + entry buffer) + its own
/// `entry_offsets` map; both are independent across groups. The shared
/// piece is just the file handle on `wal`.
///
/// **Lifetime**: `wal` is **borrowed** (not owned). The dispatcher
/// owns `*SharedWal` and tears it down after every `GroupedFileStorage`
/// has been destroyed (via `destroyGroup`'s vtable callback). Destroying
/// a group does NOT close the shared file.
pub const GroupedFileStorage = struct {
    allocator: std.mem.Allocator,
    /// Per-group read view + entry buffer — same role MemStorage plays
    /// inside `FileStorage`.
    mem: *MemStorage,
    /// Borrowed shared-WAL handle. Other groups on this node share it.
    wal: *SharedWal,
    /// The group id all records appended by this storage carry. Set at
    /// init; never changes.
    group_id: u64,
    /// Per-group offset map — tracks `mem.entries` 1:1. Slot `i` is the
    /// file offset where the record for `mem.entries.items[i]` starts.
    /// Slot 0 is the MemStorage sentinel and is never read.
    entry_offsets: std.ArrayList(u64),
    /// Per-group scratch buffer for assembling entry payloads. Lives
    /// here (not on `SharedWal`) so that if the dispatcher ever runs
    /// per-group processReady in parallel, the scratch buffers don't
    /// alias. The `SharedWal.appendRecord` call itself is still
    /// serialized by the dispatcher today.
    scratch: std.ArrayList(u8),

    /// Recovery counterpart to `init`: build the group, then replay the
    /// records `SharedWal.open` bucketed for it (drained from the WAL,
    /// applied in file order). After this returns, `mem` and
    /// `entry_offsets` reflect the durable on-disk state — same as if
    /// the group had been live when those records were first written.
    /// A group with nothing recovered comes back empty, exactly like
    /// `init`. The WAL must have been created with `open`, not `init`.
    pub fn initRecover(
        allocator: std.mem.Allocator,
        voters: []const u64,
        wal: *SharedWal,
        group_id: u64,
    ) !*GroupedFileStorage {
        const self = try GroupedFileStorage.init(allocator, voters, wal, group_id);
        errdefer self.deinit();

        if (wal.takeRecovered(group_id)) |taken| {
            var bucket = taken;
            defer {
                for (bucket.items) |r| allocator.free(r.payload);
                bucket.deinit(allocator);
            }
            for (bucket.items) |r| switch (r.tag) {
                .entry => try self.replayEntry(try parseEntryPayload(r.payload), r.offset),
                .hardstate => self.replayHardState(try parseHardStatePayload(r.payload)),
                .confstate => {}, // not produced today — skip, bytes already accounted
            };
        }
        return self;
    }

    /// Replay one recovered entry: mirror `appendEntriesCb`'s
    /// tail-truncate-then-append on `entry_offsets` (using the entry's
    /// real on-disk `file_offset`, not a fresh write), then feed it
    /// through `MemStorage`'s append — whose own "index <= last ⇒
    /// truncate" rule makes a later-in-file rewrite supersede an earlier
    /// record at the same index. No bytes are written to the WAL.
    fn replayEntry(self: *GroupedFileStorage, e: c.RaftEntryFfi, file_offset: u64) !void {
        const last_idx = self.mem.lastIndex();
        if (e.index <= last_idx) {
            const sentinel_idx = self.mem.entries.items[0].index;
            if (e.index <= sentinel_idx) return error.ReplayIndexBeforeSnapshot;
            const truncate_at: usize = @intCast(e.index - sentinel_idx);
            self.entry_offsets.shrinkRetainingCapacity(truncate_at);
        }
        try self.entry_offsets.append(self.allocator, file_offset);
        var arr = [_]c.RaftEntryFfi{e};
        // @ptrCast: the MemStorage vtable's RaftEntryFfi comes from
        // storage.zig's own @cImport, a distinct type from ours (same
        // dance as `appendEntriesCb`).
        if (mem_storage.vtable.append_entries.?(self.mem, @ptrCast(&arr), 1) != 0)
            return error.ReplayAppendFailed;
    }

    /// Replay one recovered hard state into `mem` (no WAL write). Later
    /// records overwrite earlier ones, so file order yields the last
    /// durable hard state.
    fn replayHardState(self: *GroupedFileStorage, hs: c.RaftHardStateFfi) void {
        _ = mem_storage.vtable.set_hard_state.?(self.mem, @ptrCast(&hs));
    }

    pub fn init(
        allocator: std.mem.Allocator,
        voters: []const u64,
        wal: *SharedWal,
        group_id: u64,
    ) !*GroupedFileStorage {
        const self = try allocator.create(GroupedFileStorage);
        errdefer allocator.destroy(self);

        const mem = try MemStorage.init(allocator, voters);
        errdefer mem.deinit();

        var offsets: std.ArrayList(u64) = .empty;
        errdefer offsets.deinit(allocator);
        // Sentinel slot mirrors mem.entries[0]; never read.
        try offsets.append(allocator, 0);

        self.* = .{
            .allocator = allocator,
            .mem = mem,
            .wal = wal,
            .group_id = group_id,
            .entry_offsets = offsets,
            .scratch = .empty,
        };
        return self;
    }

    pub fn deinit(self: *GroupedFileStorage) void {
        // NB: we do NOT close `self.wal` — it's borrowed; the
        // dispatcher owns it and tears it down after all groups
        // sharing it have been destroyed.
        self.mem.deinit();
        self.entry_offsets.deinit(self.allocator);
        self.scratch.deinit(self.allocator);
        self.allocator.destroy(self);
    }

    fn writeEntryRecord(self: *GroupedFileStorage, e: c.RaftEntryFfi) !u64 {
        self.scratch.clearRetainingCapacity();
        const w = self.scratch.writer(self.allocator);
        try w.writeInt(u32, e.entry_type, .little);
        try w.writeInt(u64, e.term, .little);
        try w.writeInt(u64, e.index, .little);
        try w.writeInt(u32, @intCast(e.data_len), .little);
        if (e.data_len > 0) try w.writeAll(e.data[0..e.data_len]);
        try w.writeInt(u32, @intCast(e.context_len), .little);
        if (e.context_len > 0) try w.writeAll(e.context[0..e.context_len]);
        try w.writeByte(if (e.sync_log) 1 else 0);
        return self.wal.appendRecord(self.group_id, .entry, self.scratch.items);
    }

    fn writeHardStateRecord(self: *GroupedFileStorage, hs: c.RaftHardStateFfi) !void {
        var payload: [24]u8 = undefined;
        std.mem.writeInt(u64, payload[0..8], hs.term, .little);
        std.mem.writeInt(u64, payload[8..16], hs.vote, .little);
        std.mem.writeInt(u64, payload[16..24], hs.commit, .little);
        _ = try self.wal.appendRecord(self.group_id, .hardstate, &payload);
    }
};

// ── Vtable callbacks ─────────────────────────────────────────────────────────
//
// Read callbacks delegate straight to MemStorage's vtable. Write
// callbacks build the payload, append through the shared WAL, then
// delegate. fsync is NOT here — the dispatcher calls
// `SharedWal.flush()` after every ready group has run processReady.

fn destroyCb(ud: ?*anyopaque) callconv(.c) void {
    const self: *GroupedFileStorage = @ptrCast(@alignCast(ud.?));
    self.deinit();
}

fn initialStateCb(
    ud: ?*anyopaque,
    out_hs: [*c]c.RaftHardStateFfi,
    out_cs: [*c]c.RaftConfStateFfi,
) callconv(.c) i32 {
    const self: *GroupedFileStorage = @ptrCast(@alignCast(ud.?));
    return mem_storage.vtable.initial_state.?(self.mem, @ptrCast(out_hs), @ptrCast(out_cs));
}

fn entriesCb(
    ud: ?*anyopaque,
    low: u64,
    high: u64,
    max_size: u64,
    out: [*c]c.RaftEntriesOut,
) callconv(.c) i32 {
    const self: *GroupedFileStorage = @ptrCast(@alignCast(ud.?));
    return mem_storage.vtable.entries.?(self.mem, low, high, max_size, @ptrCast(out));
}

fn termCb(ud: ?*anyopaque, idx: u64, out: [*c]u64) callconv(.c) i32 {
    const self: *GroupedFileStorage = @ptrCast(@alignCast(ud.?));
    return mem_storage.vtable.term.?(self.mem, idx, out);
}

fn firstIndexCb(ud: ?*anyopaque, out: [*c]u64) callconv(.c) i32 {
    const self: *GroupedFileStorage = @ptrCast(@alignCast(ud.?));
    return mem_storage.vtable.first_index.?(self.mem, out);
}

fn lastIndexCb(ud: ?*anyopaque, out: [*c]u64) callconv(.c) i32 {
    const self: *GroupedFileStorage = @ptrCast(@alignCast(ud.?));
    return mem_storage.vtable.last_index.?(self.mem, out);
}

fn snapshotCb(
    ud: ?*anyopaque,
    request_index: u64,
    out_data: [*c][*c]const u8,
    out_data_len: [*c]usize,
    out_meta_index: [*c]u64,
    out_meta_term: [*c]u64,
) callconv(.c) i32 {
    const self: *GroupedFileStorage = @ptrCast(@alignCast(ud.?));
    return mem_storage.vtable.snapshot.?(
        self.mem,
        request_index,
        @ptrCast(out_data),
        out_data_len,
        out_meta_index,
        out_meta_term,
    );
}

fn appendEntriesCb(
    ud: ?*anyopaque,
    entries: [*c]const c.RaftEntryFfi,
    n: usize,
) callconv(.c) i32 {
    const self: *GroupedFileStorage = @ptrCast(@alignCast(ud.?));
    if (n == 0) return 0;

    // Tail-truncate handling: raft-rs may rewrite this group's
    // uncommitted suffix on a leader-change. The per-group offset map
    // and MemStorage view both follow the truncation, but the shared
    // file does NOT physically rewind — other groups have appended
    // past the truncated entry. v1 leaves orphan bytes in the file;
    // see the module doc comment.
    const last_idx = self.mem.lastIndex();
    const first_new_idx = entries[0].index;
    if (first_new_idx <= last_idx) {
        const sentinel_idx = self.mem.entries.items[0].index;
        if (first_new_idx <= sentinel_idx) return -1; // before the sentinel — bug
        const truncate_at: usize = @intCast(first_new_idx - sentinel_idx);
        self.entry_offsets.shrinkRetainingCapacity(truncate_at);
    }

    // Append: each new entry's offset is recorded before the write so
    // a later tail-truncate can locate the boundary in the offset map.
    for (entries[0..n]) |e| {
        const offset = self.writeEntryRecord(e) catch return -1;
        self.entry_offsets.append(self.allocator, offset) catch return -1;
    }

    return mem_storage.vtable.append_entries.?(self.mem, @ptrCast(entries), n);
}

fn setHardStateCb(ud: ?*anyopaque, hs: [*c]const c.RaftHardStateFfi) callconv(.c) i32 {
    const self: *GroupedFileStorage = @ptrCast(@alignCast(ud.?));
    self.writeHardStateRecord(hs.*) catch return -1;
    return mem_storage.vtable.set_hard_state.?(self.mem, @ptrCast(hs));
}

fn applySnapshotCb(
    ud: ?*anyopaque,
    data: [*c]const u8,
    data_len: usize,
    meta_index: u64,
    meta_term: u64,
) callconv(.c) i32 {
    const self: *GroupedFileStorage = @ptrCast(@alignCast(ud.?));
    return mem_storage.vtable.apply_snapshot.?(self.mem, data, data_len, meta_index, meta_term);
}

pub const vtable: c.RaftStorageVTable = .{
    .initial_state = initialStateCb,
    .entries = entriesCb,
    .term = termCb,
    .first_index = firstIndexCb,
    .last_index = lastIndexCb,
    .snapshot = snapshotCb,
    .append_entries = appendEntriesCb,
    .set_hard_state = setHardStateCb,
    .apply_snapshot = applySnapshotCb,
    .destroy = destroyCb,
};

// ── Tests ──────────────────────────────────────────────────────────────────

const testing = std.testing;

const Harness = struct {
    tmp: std.testing.TmpDir,
    path: [:0]u8,

    fn init() !Harness {
        var tmp = testing.tmpDir(.{});
        errdefer tmp.cleanup();
        var dir_buf: [std.fs.max_path_bytes]u8 = undefined;
        const dir_path = try tmp.dir.realpath(".", &dir_buf);
        const joined = try std.fmt.allocPrint(
            testing.allocator,
            "{s}/grouped_file_storage.wal",
            .{dir_path},
        );
        defer testing.allocator.free(joined);
        const path = try testing.allocator.dupeZ(u8, joined);
        return .{ .tmp = tmp, .path = path };
    }

    fn deinit(self: *Harness) void {
        testing.allocator.free(self.path);
        self.tmp.cleanup();
    }
};

fn fakeEntry(term: u64, index: u64, data: []const u8) c.RaftEntryFfi {
    return .{
        .entry_type = 0,
        .term = term,
        .index = index,
        .data = if (data.len == 0) null else data.ptr,
        .data_len = data.len,
        .context = null,
        .context_len = 0,
        .sync_log = false,
    };
}

test "SharedWal: init creates empty file; deinit closes cleanly" {
    var h = try Harness.init();
    defer h.deinit();

    const wal = try SharedWal.init(testing.allocator, h.path);
    defer wal.deinit();
    try testing.expectEqual(@as(u64, 0), wal.wal_offset);
}

test "SharedWal: one record written by group 1 lands at offset 0 with right framing" {
    var h = try Harness.init();
    defer h.deinit();

    const wal = try SharedWal.init(testing.allocator, h.path);
    defer wal.deinit();

    const payload: [3]u8 = .{ 'x', 'y', 'z' };
    const offset = try wal.appendRecord(1, .entry, &payload);
    try testing.expectEqual(@as(u64, 0), offset);
    // 13 header + 3 payload + 4 trailer = 20
    try testing.expectEqual(@as(u64, 20), wal.wal_offset);

    // Read back via the OS and check the header framing.
    const f = try std.fs.cwd().openFile(h.path, .{});
    defer f.close();
    var buf: [20]u8 = undefined;
    _ = try f.readAll(&buf);
    try testing.expectEqual(@as(u8, @intFromEnum(Tag.entry)), buf[0]);
    try testing.expectEqual(@as(u64, 1), std.mem.readInt(u64, buf[1..9], .little));
    try testing.expectEqual(@as(u32, 3), std.mem.readInt(u32, buf[9..13], .little));
    try testing.expectEqualSlices(u8, "xyz", buf[13..16]);
}

test "SharedWal: K groups interleave into one file; offsets are returned per write" {
    var h = try Harness.init();
    defer h.deinit();

    const wal = try SharedWal.init(testing.allocator, h.path);
    defer wal.deinit();

    // Three groups, one entry each.
    const off1 = try wal.appendRecord(1, .entry, "aaa");
    const off2 = try wal.appendRecord(2, .entry, "bb");
    const off3 = try wal.appendRecord(1, .entry, "cccc");

    try testing.expectEqual(@as(u64, 0), off1);
    try testing.expectEqual(@as(u64, 13 + 3 + 4), off2);
    try testing.expectEqual(@as(u64, 13 + 3 + 4 + 13 + 2 + 4), off3);
    try testing.expectEqual(@as(u64, 13 + 3 + 4 + 13 + 2 + 4 + 13 + 4 + 4), wal.wal_offset);
}

test "GroupedFileStorage: init wires through SharedWal; deinit doesn't close the shared file" {
    var h = try Harness.init();
    defer h.deinit();

    const wal = try SharedWal.init(testing.allocator, h.path);
    defer wal.deinit();

    const g1 = try GroupedFileStorage.init(testing.allocator, &.{1}, wal, 1);
    defer g1.deinit();
    const g2 = try GroupedFileStorage.init(testing.allocator, &.{1}, wal, 2);
    defer g2.deinit();

    try testing.expectEqual(@as(u64, 1), g1.group_id);
    try testing.expectEqual(@as(u64, 2), g2.group_id);
    try testing.expectEqual(@as(usize, 1), g1.entry_offsets.items.len);
    try testing.expectEqual(@as(usize, 1), g2.entry_offsets.items.len);
}

test "GroupedFileStorage: each group's appends land in the shared file; per-group offsets are independent" {
    var h = try Harness.init();
    defer h.deinit();

    const wal = try SharedWal.init(testing.allocator, h.path);
    defer wal.deinit();

    const g1 = try GroupedFileStorage.init(testing.allocator, &.{1}, wal, 1);
    defer g1.deinit();
    const g2 = try GroupedFileStorage.init(testing.allocator, &.{1}, wal, 2);
    defer g2.deinit();

    // Group 1 appends an entry, then group 2 appends an entry —
    // verify both land in the same file at correct offsets.
    const e1 = fakeEntry(1, 1, "x");
    var arr1 = [_]c.RaftEntryFfi{e1};
    try testing.expectEqual(@as(i32, 0), appendEntriesCb(g1, &arr1, 1));

    const e2 = fakeEntry(1, 1, "yyyy");
    var arr2 = [_]c.RaftEntryFfi{e2};
    try testing.expectEqual(@as(i32, 0), appendEntriesCb(g2, &arr2, 1));

    // Per-group entry offsets are independent.
    try testing.expectEqual(@as(usize, 2), g1.entry_offsets.items.len);
    try testing.expectEqual(@as(usize, 2), g2.entry_offsets.items.len);
    // g1's entry starts at 0; g2's starts after g1's full record.
    try testing.expectEqual(@as(u64, 0), g1.entry_offsets.items[1]);
    //   g1 record: 13 header + 30 payload + 4 trailer = 47
    //   (30 = 4 entry_type + 8 term + 8 index + 4 data_len + 1 data
    //         + 4 context_len + 0 + 1 sync_log)
    try testing.expectEqual(@as(u64, 47), g2.entry_offsets.items[1]);

    // mem views also independent.
    try testing.expectEqual(@as(u64, 1), g1.mem.lastIndex());
    try testing.expectEqual(@as(u64, 1), g2.mem.lastIndex());
}

test "GroupedFileStorage: tail-truncate updates mem + offsets but does NOT physically rewind the file" {
    var h = try Harness.init();
    defer h.deinit();

    const wal = try SharedWal.init(testing.allocator, h.path);
    defer wal.deinit();

    const g1 = try GroupedFileStorage.init(testing.allocator, &.{1}, wal, 1);
    defer g1.deinit();
    const g2 = try GroupedFileStorage.init(testing.allocator, &.{1}, wal, 2);
    defer g2.deinit();

    // g1: append 3 entries.
    var batch = [_]c.RaftEntryFfi{
        fakeEntry(1, 1, "a"),
        fakeEntry(1, 2, "b"),
        fakeEntry(1, 3, "c"),
    };
    try testing.expectEqual(@as(i32, 0), appendEntriesCb(g1, &batch, batch.len));
    try testing.expectEqual(@as(u64, 3), g1.mem.lastIndex());

    // g2: append after g1, then g1 tail-truncates from index 2 — the
    // shared file must NOT physically rewind, or g2's entry would be
    // orphaned.
    var g2_batch = [_]c.RaftEntryFfi{fakeEntry(1, 1, "z")};
    try testing.expectEqual(@as(i32, 0), appendEntriesCb(g2, &g2_batch, 1));
    const wal_offset_before_trunc = wal.wal_offset;
    const g2_last_offset = g2.entry_offsets.items[1];

    // g1's leader-change overwrites index 2 onward.
    var rewrite = [_]c.RaftEntryFfi{fakeEntry(2, 2, "B'")};
    try testing.expectEqual(@as(i32, 0), appendEntriesCb(g1, &rewrite, 1));

    // g1's mem view is rewritten — last index = 2.
    try testing.expectEqual(@as(u64, 2), g1.mem.lastIndex());
    // g1's entry_offsets shrank then re-appended one slot.
    try testing.expectEqual(@as(usize, 3), g1.entry_offsets.items.len); // sentinel + idx 1 + idx 2
    // g2 still has its entry's offset — wasn't disturbed.
    try testing.expectEqual(@as(u64, g2_last_offset), g2.entry_offsets.items[1]);
    // wal_offset advanced (didn't rewind) — the rewrite is APPENDED.
    try testing.expect(wal.wal_offset > wal_offset_before_trunc);
}

test "GroupedFileStorage: setHardState writes a per-group hardstate record" {
    var h = try Harness.init();
    defer h.deinit();

    const wal = try SharedWal.init(testing.allocator, h.path);
    defer wal.deinit();

    const g1 = try GroupedFileStorage.init(testing.allocator, &.{1}, wal, 7);
    defer g1.deinit();

    const hs: c.RaftHardStateFfi = .{ .term = 5, .vote = 3, .commit = 2 };
    try testing.expectEqual(@as(i32, 0), setHardStateCb(g1, &hs));

    // Header (13) + 24 byte hs payload + 4 trailer = 41
    try testing.expectEqual(@as(u64, 41), wal.wal_offset);

    // Read back, confirm tag + group_id.
    const f = try std.fs.cwd().openFile(h.path, .{});
    defer f.close();
    var buf: [41]u8 = undefined;
    _ = try f.readAll(&buf);
    try testing.expectEqual(@as(u8, @intFromEnum(Tag.hardstate)), buf[0]);
    try testing.expectEqual(@as(u64, 7), std.mem.readInt(u64, buf[1..9], .little));
    try testing.expectEqual(@as(u32, 24), std.mem.readInt(u32, buf[9..13], .little));
    try testing.expectEqual(@as(u64, 5), std.mem.readInt(u64, buf[13..21], .little));
}

test "SharedWal.flush: a single flush amortizes K groups' writes" {
    // The structural guarantee — the entire reason this module exists.
    // We verify by writing through K groups, calling flush ONCE, and
    // confirming all records are on disk.
    var h = try Harness.init();
    defer h.deinit();

    const wal = try SharedWal.init(testing.allocator, h.path);
    defer wal.deinit();

    const K: usize = 8;
    var groups: [K]*GroupedFileStorage = undefined;
    for (&groups, 0..) |*g, i| {
        g.* = try GroupedFileStorage.init(testing.allocator, &.{1}, wal, @intCast(i + 1));
    }
    defer for (groups) |g| g.deinit();

    for (groups) |g| {
        var arr = [_]c.RaftEntryFfi{fakeEntry(1, 1, "data")};
        try testing.expectEqual(@as(i32, 0), appendEntriesCb(g, &arr, 1));
    }

    try wal.flush(); // ONE fsync amortizes K groups' writes.

    // All K records on disk. Each entry record: 13 header + 33 payload
    // + 4 trailer = 50 bytes (33 = 4 entry_type + 8 term + 8 index + 4
    // data_len + 4 data + 4 context_len + 1 sync_log).
    try testing.expectEqual(@as(u64, K * 50), wal.wal_offset);
}

test "recovery: open on a fresh path yields an empty WAL" {
    var h = try Harness.init();
    defer h.deinit();

    const wal = try SharedWal.open(testing.allocator, h.path);
    defer wal.deinit();
    try testing.expectEqual(@as(u64, 0), wal.wal_offset);
    try testing.expectEqual(@as(usize, 0), wal.recovered.count());

    // initRecover on a group with nothing on disk is just an empty group.
    const g1 = try GroupedFileStorage.initRecover(testing.allocator, &.{1}, wal, 1);
    defer g1.deinit();
    try testing.expectEqual(@as(u64, 0), g1.mem.lastIndex());
}

test "recovery: open rebuilds per-group log + hardstate from a written WAL" {
    var h = try Harness.init();
    defer h.deinit();

    // Write phase: two groups interleaved, entries + a hardstate, flush.
    {
        const wal = try SharedWal.init(testing.allocator, h.path);
        defer wal.deinit();
        const g1 = try GroupedFileStorage.init(testing.allocator, &.{1}, wal, 1);
        defer g1.deinit();
        const g2 = try GroupedFileStorage.init(testing.allocator, &.{1}, wal, 2);
        defer g2.deinit();

        var b1 = [_]c.RaftEntryFfi{ fakeEntry(1, 1, "a"), fakeEntry(1, 2, "bb") };
        try testing.expectEqual(@as(i32, 0), appendEntriesCb(g1, &b1, b1.len));
        const hs1: c.RaftHardStateFfi = .{ .term = 1, .vote = 1, .commit = 2 };
        try testing.expectEqual(@as(i32, 0), setHardStateCb(g1, &hs1));

        var b2 = [_]c.RaftEntryFfi{fakeEntry(1, 1, "z")};
        try testing.expectEqual(@as(i32, 0), appendEntriesCb(g2, &b2, 1));

        try wal.flush();
    }

    // Recover phase: a fresh process re-opens the same file.
    const wal = try SharedWal.open(testing.allocator, h.path);
    defer wal.deinit();
    const g1 = try GroupedFileStorage.initRecover(testing.allocator, &.{1}, wal, 1);
    defer g1.deinit();
    const g2 = try GroupedFileStorage.initRecover(testing.allocator, &.{1}, wal, 2);
    defer g2.deinit();

    // g1: both entries + the hardstate came back.
    try testing.expectEqual(@as(u64, 2), g1.mem.lastIndex());
    try testing.expectEqual(@as(u64, 1), g1.mem.hs_term);
    try testing.expectEqual(@as(u64, 2), g1.mem.hs_commit);
    // g2: its single independent entry.
    try testing.expectEqual(@as(u64, 1), g2.mem.lastIndex());

    // entry_offsets rebuilt (sentinel + N), and g1's first entry is the
    // first record in the file.
    try testing.expectEqual(@as(usize, 3), g1.entry_offsets.items.len);
    try testing.expectEqual(@as(usize, 2), g2.entry_offsets.items.len);
    try testing.expectEqual(@as(u64, 0), g1.entry_offsets.items[1]);

    // Write head sits at the durable prefix end (== file size), so the
    // next append continues the file rather than overwriting it.
    try testing.expectEqual((try wal.file.stat()).size, wal.wal_offset);
    var b3 = [_]c.RaftEntryFfi{fakeEntry(1, 3, "c")};
    try testing.expectEqual(@as(i32, 0), appendEntriesCb(g1, &b3, 1));
    try testing.expectEqual(@as(u64, 3), g1.mem.lastIndex());
}

test "recovery: a torn (CRC-failed) tail record is dropped and truncated" {
    var h = try Harness.init();
    defer h.deinit();

    var valid_size: u64 = 0;
    {
        const wal = try SharedWal.init(testing.allocator, h.path);
        defer wal.deinit();
        const g1 = try GroupedFileStorage.init(testing.allocator, &.{1}, wal, 1);
        defer g1.deinit();
        var b = [_]c.RaftEntryFfi{ fakeEntry(1, 1, "a"), fakeEntry(1, 2, "b") };
        try testing.expectEqual(@as(i32, 0), appendEntriesCb(g1, &b, b.len));
        try wal.flush();
        valid_size = wal.wal_offset;
    }

    // Simulate a crash mid-append: a structurally complete record with a
    // wrong CRC tacked onto the end (exercises header+payload+trailer
    // read, then the CRC-mismatch boundary).
    {
        var rec: [HEADER_LEN + 3 + TRAILER_LEN]u8 = undefined;
        rec[0] = @intFromEnum(Tag.entry);
        std.mem.writeInt(u64, rec[1..9], 1, .little);
        std.mem.writeInt(u32, rec[9..13], 3, .little);
        @memcpy(rec[13..16], "xxx");
        std.mem.writeInt(u32, rec[16..20], 0xDEADBEEF, .little); // not the real CRC
        const f = try std.fs.cwd().openFile(h.path, .{ .mode = .write_only });
        defer f.close();
        try f.seekTo(valid_size);
        try f.writeAll(&rec);
    }

    const wal = try SharedWal.open(testing.allocator, h.path);
    defer wal.deinit();
    const g1 = try GroupedFileStorage.initRecover(testing.allocator, &.{1}, wal, 1);
    defer g1.deinit();

    // Only the durable prefix recovered; the torn record is gone.
    try testing.expectEqual(@as(u64, 2), g1.mem.lastIndex());
    // Torn tail physically truncated.
    try testing.expectEqual(valid_size, wal.wal_offset);
    try testing.expectEqual(valid_size, (try wal.file.stat()).size);
}

test "recovery: replay reproduces last-authoritative entry after a tail rewrite" {
    var h = try Harness.init();
    defer h.deinit();

    {
        const wal = try SharedWal.init(testing.allocator, h.path);
        defer wal.deinit();
        const g1 = try GroupedFileStorage.init(testing.allocator, &.{1}, wal, 1);
        defer g1.deinit();

        // idx 1,2,3 at term 1.
        var b = [_]c.RaftEntryFfi{ fakeEntry(1, 1, "a"), fakeEntry(1, 2, "b"), fakeEntry(1, 3, "c") };
        try testing.expectEqual(@as(i32, 0), appendEntriesCb(g1, &b, b.len));
        // Leader change rewrites idx 2 at term 2 — appended to the file
        // (can't physically rewind a shared WAL), truncating 2,3 in the
        // live view.
        var rw = [_]c.RaftEntryFfi{fakeEntry(2, 2, "B")};
        try testing.expectEqual(@as(i32, 0), appendEntriesCb(g1, &rw, 1));
        try testing.expectEqual(@as(u64, 2), g1.mem.lastIndex());
        try wal.flush();
    }

    const wal = try SharedWal.open(testing.allocator, h.path);
    defer wal.deinit();
    const g1 = try GroupedFileStorage.initRecover(testing.allocator, &.{1}, wal, 1);
    defer g1.deinit();

    // Replaying the four records in file order reproduces the live view:
    // the later idx-2 record supersedes the earlier one, and idx 3 is gone.
    try testing.expectEqual(@as(u64, 2), g1.mem.lastIndex());
    try testing.expectEqual(@as(?u64, 1), g1.mem.termAt(1));
    try testing.expectEqual(@as(?u64, 2), g1.mem.termAt(2));
    try testing.expectEqual(@as(?u64, null), g1.mem.termAt(3));

    // entry_offsets: sentinel + idx1 + idx2(rewrite). The idx-2 slot
    // points at the 4th (rewrite) record. Each single-byte-data entry
    // record is 13 + 30 + 4 = 47 bytes, so the rewrite sits at 3*47=141.
    try testing.expectEqual(@as(usize, 3), g1.entry_offsets.items.len);
    try testing.expectEqual(@as(u64, 141), g1.entry_offsets.items[2]);
}
