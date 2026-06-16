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
//! ## Segmentation + GC (bounding the WAL)
//!
//! Compaction (see `GroupedFileStorage.compact`) advances a group's
//! `first_index` and marks records dead, but the bytes stay on disk
//! until a whole segment can be reclaimed. The WAL is therefore chopped
//! into segments: the **active** segment is always the base path, and a
//! roll (when it passes `segment_target`) seals it by renaming to
//! `{base}.{seg_no}` and opens a fresh active at the base path again.
//! (Keeping the active at the base path means the never-rolled case is
//! byte-identical to a plain single-file WAL — every unsegmented test
//! exercises that path unchanged.)
//!
//! A sealed segment is deleted once every group that wrote entries into
//! it has a compaction watermark at or above the highest index it wrote
//! there — i.e. the segment holds no live entries. Hard state would
//! otherwise pin old segments for quiet groups, so each new segment
//! opens with a **header**: the WAL caches the latest hard state per
//! group (it sees every hard-state record go by) and re-emits all of
//! them into each fresh segment. That way the newest segment always
//! carries every live group's hard state, GC of older segments never
//! loses it, and recovery — replaying sealed segments oldest-first then
//! the active — sees the latest hard state win. Compaction watermarks
//! are not persisted: they rebuild as groups re-compact after a restart
//! (compaction is a space optimization; re-applying already-applied
//! entries is idempotent in the layer above).

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
    /// A compaction marker: "this group's log is compacted through
    /// {index, term}". Payload = [index:u64 LE][term:u64 LE]. Written by
    /// `GroupedFileStorage.compact`, re-baselined into every later
    /// segment header (like hard state), and consumed by recovery to
    /// anchor the log sentinel above 1 so a GC'd prefix is not a gap.
    compaction = 4,
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

/// Hard-state record payload length: term + vote + commit, all u64.
const HS_PAYLOAD_LEN: usize = 24;
/// Compaction record payload length: index + term, both u64.
const COMPACTION_PAYLOAD_LEN: usize = 16;

/// Default segment roll threshold. A deployment tunes this; it only
/// affects how finely the WAL is chopped for GC, never correctness.
/// Large enough that the single-file tests never roll (so they exercise
/// the unsegmented path unchanged).
pub const DEFAULT_SEGMENT_TARGET: u64 = 64 * 1024 * 1024;

/// A sealed (no-longer-active) WAL segment, tracked for GC. Its file is
/// `{base}.{seg_no:0>6}`. `group_max` is the highest entry index each
/// group wrote into it; once every such group's compaction watermark
/// reaches that index the segment holds no live entries and is deleted.
/// Hard state never blocks GC — it's re-baselined into every later
/// segment's header.
const SealedSegment = struct {
    seg_no: u64,
    group_max: std.AutoHashMap(u64, u64),
};

/// Format the on-disk path of a sealed segment into `buf`.
fn sealedSegmentPath(buf: []u8, base_path: []const u8, seg_no: u64) ![]u8 {
    return std.fmt.bufPrint(buf, "{s}.{d:0>6}", .{ base_path, seg_no });
}

/// One shared, segmented WAL across all groups on a single cluster-node.
/// Construction + flush are the dispatcher's responsibility; group
/// storages borrow `*SharedWal` via `GroupedFileStorage.init` and write
/// through `appendRecord`. See the module doc for the segmentation / GC
/// model.
///
/// Single-threaded by contract: the dispatcher pumps one ready group at
/// a time per node, so the file handle, offset, and caches are accessed
/// serially. If per-group processReady is ever parallelized, this needs
/// a mutex around the write head + the caches.
pub const SharedWal = struct {
    allocator: std.mem.Allocator,
    /// The ACTIVE segment — always at `wal_path` (no suffix). A roll
    /// seals it by renaming to `{wal_path}.{seg_no}` and opens a fresh
    /// active here. Named `file` (not `active`) so existing callers that
    /// reach `wal.file` keep working.
    file: std.fs.File,
    /// Path of the active segment (the base path).
    wal_path: []u8,
    /// Bytes written into the ACTIVE segment. Resets (to the header
    /// size) on each roll.
    wal_offset: u64,
    /// Roll threshold: when `wal_offset` reaches this, the next
    /// `appendRecord` seals the active segment and starts a new one.
    segment_target: u64,
    /// Next sealed-segment number to hand out (monotonic, starts at 1).
    next_seg_no: u64,
    /// Sealed segments, oldest first, tracked for GC.
    sealed: std.ArrayList(SealedSegment),
    /// Highest entry index each group has written into the ACTIVE
    /// segment. Moves into the new `SealedSegment` on roll.
    active_group_max: std.AutoHashMap(u64, u64),
    /// Latest hard-state payload seen per group, re-emitted as each new
    /// segment's header so GC of older segments never drops a quiet
    /// group's hard state. Updated on every hard-state append.
    hardstate_cache: std.AutoHashMap(u64, [HS_PAYLOAD_LEN]u8),
    /// Latest compaction-marker payload seen per group, re-emitted in
    /// each new segment's header alongside hard state — so recovery can
    /// always find a group's snapshot point even after the segment that
    /// first recorded it has been GC'd.
    compaction_cache: std.AutoHashMap(u64, [COMPACTION_PAYLOAD_LEN]u8),
    /// Per-group compaction watermark (mirrors each group's
    /// `compaction_index`), pushed in via `noteCompaction`; drives GC.
    /// Not persisted — rebuilt as groups re-compact after a restart.
    compaction: std.AutoHashMap(u64, u64),
    /// Records recovered by `open`, bucketed by `group_id` in file
    /// order, awaiting replay. Empty after `init`. Each group drains its
    /// bucket in `GroupedFileStorage.initRecover`; `deinit` frees any
    /// bucket that was never drained.
    recovered: std.AutoHashMap(u64, std.ArrayList(RecoveredRecord)),

    /// Fresh start with the default segment size.
    pub fn init(allocator: std.mem.Allocator, wal_path: []const u8) !*SharedWal {
        return initWithTarget(allocator, wal_path, DEFAULT_SEGMENT_TARGET);
    }

    /// Fresh start: truncate any existing active file to empty. Use
    /// `open` instead to recover an existing WAL. `segment_target` is
    /// the roll threshold (tests pass a small value to force rolls).
    pub fn initWithTarget(
        allocator: std.mem.Allocator,
        wal_path: []const u8,
        segment_target: u64,
    ) !*SharedWal {
        const self = try allocator.create(SharedWal);
        errdefer allocator.destroy(self);
        const path_dup = try allocator.dupe(u8, wal_path);
        errdefer allocator.free(path_dup);
        const file = try std.fs.cwd().createFile(wal_path, .{ .truncate = true, .read = false });
        errdefer file.close();

        self.* = .{
            .allocator = allocator,
            .file = file,
            .wal_path = path_dup,
            .wal_offset = 0,
            .segment_target = segment_target,
            .next_seg_no = 1,
            .sealed = .empty,
            .active_group_max = std.AutoHashMap(u64, u64).init(allocator),
            .hardstate_cache = std.AutoHashMap(u64, [HS_PAYLOAD_LEN]u8).init(allocator),
            .compaction_cache = std.AutoHashMap(u64, [COMPACTION_PAYLOAD_LEN]u8).init(allocator),
            .compaction = std.AutoHashMap(u64, u64).init(allocator),
            .recovered = std.AutoHashMap(u64, std.ArrayList(RecoveredRecord)).init(allocator),
        };
        return self;
    }

    /// Crash-recovery start with the default segment size.
    pub fn open(allocator: std.mem.Allocator, wal_path: []const u8) !*SharedWal {
        return openWithTarget(allocator, wal_path, DEFAULT_SEGMENT_TARGET);
    }

    /// Crash-recovery start: discover the sealed segments, replay-scan
    /// them oldest-first then the active segment (truncating the
    /// active's torn tail), rebuild the GC + hard-state caches, and
    /// position the write head to continue the active segment. Recovered
    /// records are bucketed by group in `self.recovered`; callers
    /// re-create each group via `GroupedFileStorage.initRecover`.
    pub fn openWithTarget(
        allocator: std.mem.Allocator,
        wal_path: []const u8,
        segment_target: u64,
    ) !*SharedWal {
        // Build the recovered state in locals so the final assembly into
        // `self.*` is infallible (no half-initialized struct to unwind).
        var recovered = std.AutoHashMap(u64, std.ArrayList(RecoveredRecord)).init(allocator);
        errdefer freeRecovered(allocator, &recovered);
        var sealed: std.ArrayList(SealedSegment) = .empty;
        errdefer {
            for (sealed.items) |*s| s.group_max.deinit();
            sealed.deinit(allocator);
        }
        var active_group_max = std.AutoHashMap(u64, u64).init(allocator);
        errdefer active_group_max.deinit();
        var hardstate_cache = std.AutoHashMap(u64, [HS_PAYLOAD_LEN]u8).init(allocator);
        errdefer hardstate_cache.deinit();
        var compaction_cache = std.AutoHashMap(u64, [COMPACTION_PAYLOAD_LEN]u8).init(allocator);
        errdefer compaction_cache.deinit();

        const seg_nos = try discoverSealedSegments(allocator, wal_path);
        defer allocator.free(seg_nos);

        // Sealed segments were fsynced before sealing, so they are
        // intact; scan each fully (oldest first) so cross-segment replay
        // order and last-hard-state-wins hold.
        var max_seg_no: u64 = 0;
        for (seg_nos) |sn| {
            var gm = std.AutoHashMap(u64, u64).init(allocator);
            errdefer gm.deinit();
            var pbuf: [std.fs.max_path_bytes]u8 = undefined;
            const p = try sealedSegmentPath(&pbuf, wal_path, sn);
            const f = try std.fs.cwd().openFile(p, .{});
            defer f.close();
            _ = try scanForReplay(allocator, f, &recovered, &gm, &hardstate_cache, &compaction_cache);
            try sealed.append(allocator, .{ .seg_no = sn, .group_max = gm });
            if (sn > max_seg_no) max_seg_no = sn;
        }

        // The active segment is the only one that can have a torn tail.
        const file = try std.fs.cwd().createFile(wal_path, .{ .truncate = false, .read = true });
        errdefer file.close();
        const valid_end = try scanForReplay(allocator, file, &recovered, &active_group_max, &hardstate_cache, &compaction_cache);
        try file.setEndPos(valid_end);
        try file.seekTo(valid_end);

        const self = try allocator.create(SharedWal);
        errdefer allocator.destroy(self);
        const path_dup = try allocator.dupe(u8, wal_path);
        errdefer allocator.free(path_dup);

        self.* = .{
            .allocator = allocator,
            .file = file,
            .wal_path = path_dup,
            .wal_offset = valid_end,
            .segment_target = segment_target,
            .next_seg_no = max_seg_no + 1,
            .sealed = sealed,
            .active_group_max = active_group_max,
            .hardstate_cache = hardstate_cache,
            .compaction_cache = compaction_cache,
            .compaction = std.AutoHashMap(u64, u64).init(allocator),
            .recovered = recovered,
        };
        return self;
    }

    pub fn deinit(self: *SharedWal) void {
        self.file.close();
        const a = self.allocator;
        freeRecovered(a, &self.recovered);
        for (self.sealed.items) |*s| s.group_max.deinit();
        self.sealed.deinit(a);
        self.active_group_max.deinit();
        self.hardstate_cache.deinit();
        self.compaction_cache.deinit();
        self.compaction.deinit();
        a.free(self.wal_path);
        a.destroy(self);
    }

    /// Hand a group its recovered records (ownership transfers to the
    /// caller, which must free each `payload` and `deinit` the list).
    /// Removes the bucket so `deinit` won't double-free it. Null when
    /// the group has nothing to replay.
    pub fn takeRecovered(self: *SharedWal, group_id: u64) ?std.ArrayList(RecoveredRecord) {
        if (self.recovered.fetchRemove(group_id)) |kv| return kv.value;
        return null;
    }

    /// fsync the active segment. The dispatcher calls this ONCE per pump
    /// cycle after every ready group has run processReady — the
    /// load-bearing "one fsync amortizes K groups" point. (Sealed
    /// segments were already fsynced at seal time.)
    pub fn flush(self: *SharedWal) !void {
        try self.file.sync();
    }

    /// Append one record to the WAL, rolling to a new segment first if
    /// the active one has reached `segment_target`. Returns the offset
    /// within the active segment where the record was written. `payload`
    /// is the body only; framing (header + crc trailer) is added here.
    pub fn appendRecord(
        self: *SharedWal,
        group_id: u64,
        tag: Tag,
        payload: []const u8,
    ) !u64 {
        if (self.wal_offset >= self.segment_target) try self.roll();

        // Maintain the GC + header caches from the append stream itself,
        // so neither needs back-pointers to the groups.
        switch (tag) {
            .hardstate => if (payload.len == HS_PAYLOAD_LEN) {
                var hs: [HS_PAYLOAD_LEN]u8 = undefined;
                @memcpy(&hs, payload[0..HS_PAYLOAD_LEN]);
                try self.hardstate_cache.put(group_id, hs);
            },
            .entry => if (payload.len >= 20) {
                // Entry payload: entry_type u32, term u64, index u64, ...
                const idx = std.mem.readInt(u64, payload[12..20], .little);
                const gop = try self.active_group_max.getOrPut(group_id);
                if (!gop.found_existing or idx > gop.value_ptr.*) gop.value_ptr.* = idx;
            },
            .compaction => if (payload.len == COMPACTION_PAYLOAD_LEN) {
                var cp: [COMPACTION_PAYLOAD_LEN]u8 = undefined;
                @memcpy(&cp, payload[0..COMPACTION_PAYLOAD_LEN]);
                try self.compaction_cache.put(group_id, cp);
            },
            .confstate => {},
        }

        return self.writeFramed(group_id, tag, payload);
    }

    /// Frame and write one record into the active segment, advancing the
    /// write head. The low-level write shared by `appendRecord` and the
    /// roll-time header re-emit (which must not re-trigger a roll or
    /// re-touch the caches).
    fn writeFramed(self: *SharedWal, group_id: u64, tag: Tag, payload: []const u8) !u64 {
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

    /// Seal the active segment (fsync, close, rename to
    /// `{base}.{seg_no}`) and open a fresh active at the base path,
    /// re-emitting every cached hard state as the new segment's header.
    fn roll(self: *SharedWal) !void {
        try self.file.sync();
        self.file.close();

        var pbuf: [std.fs.max_path_bytes]u8 = undefined;
        const sealed_path = try sealedSegmentPath(&pbuf, self.wal_path, self.next_seg_no);
        try std.fs.cwd().rename(self.wal_path, sealed_path);

        // The active segment's per-group entry maxima become the sealed
        // segment's GC key; the active starts fresh.
        try self.sealed.append(self.allocator, .{ .seg_no = self.next_seg_no, .group_max = self.active_group_max });
        self.next_seg_no += 1;
        self.active_group_max = std.AutoHashMap(u64, u64).init(self.allocator);

        self.file = try std.fs.cwd().createFile(self.wal_path, .{ .truncate = true, .read = false });
        self.wal_offset = 0;

        // Header: re-baseline every live group's hard state and
        // compaction marker, so older segments hold the only copy of
        // neither.
        var it = self.hardstate_cache.iterator();
        while (it.next()) |e| {
            _ = try self.writeFramed(e.key_ptr.*, .hardstate, e.value_ptr.*[0..]);
        }
        var cit = self.compaction_cache.iterator();
        while (cit.next()) |e| {
            _ = try self.writeFramed(e.key_ptr.*, .compaction, e.value_ptr.*[0..]);
        }
    }

    /// Record group `group_id`'s compaction watermark and reclaim any
    /// sealed segment that now holds no live entries. Called by
    /// `GroupedFileStorage.compact`.
    pub fn noteCompaction(self: *SharedWal, group_id: u64, watermark: u64) !void {
        try self.compaction.put(group_id, watermark);
        self.gcSealed();
    }

    /// Declare a group permanently gone (an *intentional* migration
    /// detach — NOT ordinary storage teardown, which must preserve
    /// records for restart recovery). Drops the group from every cache
    /// so it stops being re-emitted / pinning segments, then GCs
    /// anything it was holding open. The dispatcher calls this around
    /// `destroyGroup`, never from `GroupedFileStorage.deinit`.
    pub fn noteGroupDestroyed(self: *SharedWal, group_id: u64) void {
        _ = self.hardstate_cache.remove(group_id);
        _ = self.compaction_cache.remove(group_id);
        _ = self.compaction.remove(group_id);
        _ = self.active_group_max.remove(group_id);
        for (self.sealed.items) |*s| _ = s.group_max.remove(group_id);
        self.gcSealed();
    }

    /// Delete every sealed segment all of whose groups are compacted
    /// past their highest index in it. Best-effort: a failed unlink
    /// leaves a dead file that recovery just skips, so errors are
    /// swallowed rather than propagated up the write path.
    fn gcSealed(self: *SharedWal) void {
        var i: usize = 0;
        while (i < self.sealed.items.len) {
            if (self.segmentDead(&self.sealed.items[i])) {
                var seg = self.sealed.orderedRemove(i); // keep oldest-first order
                var pbuf: [std.fs.max_path_bytes]u8 = undefined;
                if (sealedSegmentPath(&pbuf, self.wal_path, seg.seg_no)) |p| {
                    std.fs.cwd().deleteFile(p) catch {};
                } else |_| {}
                seg.group_max.deinit();
            } else i += 1;
        }
    }

    fn segmentDead(self: *const SharedWal, seg: *const SealedSegment) bool {
        var it = seg.group_max.iterator();
        while (it.next()) |e| {
            const wm = self.compaction.get(e.key_ptr.*) orelse 0;
            if (e.value_ptr.* > wm) return false; // a live (un-compacted) entry remains
        }
        return true;
    }
};

/// Discover the sealed-segment numbers for `wal_path` (files named
/// `{basename}.{digits}` next to it), sorted ascending. Returns an empty
/// slice if the directory can't be opened (fresh boot, no segments).
fn discoverSealedSegments(allocator: std.mem.Allocator, wal_path: []const u8) ![]u64 {
    const dir_path = std.fs.path.dirname(wal_path) orelse ".";
    const base_name = std.fs.path.basename(wal_path);

    var dir = std.fs.cwd().openDir(dir_path, .{ .iterate = true }) catch return &[_]u64{};
    defer dir.close();

    var list: std.ArrayList(u64) = .empty;
    errdefer list.deinit(allocator);

    var it = dir.iterate();
    while (try it.next()) |entry| {
        if (entry.kind != .file) continue;
        if (!std.mem.startsWith(u8, entry.name, base_name)) continue;
        if (entry.name.len <= base_name.len + 1) continue; // the active segment itself, or no suffix
        if (entry.name[base_name.len] != '.') continue;
        const suffix = entry.name[base_name.len + 1 ..];
        const sn = std.fmt.parseInt(u64, suffix, 10) catch continue; // non-numeric suffix → not ours
        try list.append(allocator, sn);
    }
    std.mem.sort(u64, list.items, {}, std.sort.asc(u64));
    return list.toOwnedSlice(allocator);
}

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

/// Walk one segment from offset 0, CRC-validating each record and
/// bucketing the valid ones by `group_id` (in file order) into
/// `recovered`. Stops at the first short-read or CRC failure — the
/// crash boundary; everything before it is the durable prefix. Returns
/// that boundary offset.
///
/// Also rebuilds GC + header state: each entry record updates
/// `group_max[group_id]` to the max index seen (this segment's GC key),
/// and each hard-state record updates `hardstate_cache[group_id]` (so,
/// scanning segments oldest-first then the active, the cache ends at the
/// latest hard state per group).
fn scanForReplay(
    allocator: std.mem.Allocator,
    file: std.fs.File,
    recovered: *std.AutoHashMap(u64, std.ArrayList(RecoveredRecord)),
    group_max: *std.AutoHashMap(u64, u64),
    hardstate_cache: *std.AutoHashMap(u64, [HS_PAYLOAD_LEN]u8),
    compaction_cache: *std.AutoHashMap(u64, [COMPACTION_PAYLOAD_LEN]u8),
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
            4 => .compaction,
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

        // Rebuild GC / header state from the record.
        switch (tag) {
            .entry => if (payload_len >= 20) {
                const idx = std.mem.readInt(u64, payload[12..20], .little);
                const gop = try group_max.getOrPut(group_id);
                if (!gop.found_existing or idx > gop.value_ptr.*) gop.value_ptr.* = idx;
            },
            .hardstate => if (payload_len == HS_PAYLOAD_LEN) {
                var hs: [HS_PAYLOAD_LEN]u8 = undefined;
                @memcpy(&hs, payload[0..HS_PAYLOAD_LEN]);
                try hardstate_cache.put(group_id, hs);
            },
            .compaction => if (payload_len == COMPACTION_PAYLOAD_LEN) {
                var cp: [COMPACTION_PAYLOAD_LEN]u8 = undefined;
                @memcpy(&cp, payload[0..COMPACTION_PAYLOAD_LEN]);
                try compaction_cache.put(group_id, cp);
            },
            .confstate => {},
        }

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
/// Application snapshot-generate hook. Same ABI as the storage vtable's
/// `snapshot` callback, minus the leading userdata (the storage passes its own
/// `snapshot_ctx`) and plus `group_id`. Returns 0 + fills the out-params on
/// success, -1 for SnapshotTemporarilyUnavailable (the app is still preparing
/// the snapshot — raft-rs retries), other negatives on error. The storage
/// derives `out_meta_term` authoritatively from its own log, so the hook need
/// only fill `out_data` + `out_meta_index`.
pub const SnapshotProviderFn = *const fn (
    ctx: ?*anyopaque,
    group_id: u64,
    request_index: u64,
    out_data: [*c][*c]const u8,
    out_data_len: [*c]usize,
    out_meta_index: [*c]u64,
    out_meta_term: [*c]u64,
) callconv(.c) i32;

/// Application snapshot-apply hook: install an inbound snapshot's application
/// state (rove loads the tenant bundle the descriptor names into its store and
/// stamps the durable watermark = `meta_index`). Called from `apply_snapshot`
/// BEFORE the in-memory raft log is reset, so a failure (return != 0) aborts
/// the apply with no torn state. The app is expected to have the snapshot bytes
/// already staged locally (the pump defers the apply until then via
/// `raft_manager_pending_snapshot`), so this is fast + synchronous.
pub const ApplyHandlerFn = *const fn (
    ctx: ?*anyopaque,
    group_id: u64,
    data: [*c]const u8,
    data_len: usize,
    meta_index: u64,
    meta_term: u64,
) callconv(.c) i32;

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
    /// This group's compaction watermark: the highest index whose log
    /// entry has been dropped (== `mem`'s sentinel index). Records at or
    /// below it are dead on disk; segment GC uses this to decide when a
    /// whole segment can be reclaimed. 0 until the first `compact`.
    compaction_index: u64 = 0,

    /// Optional application snapshot hooks (the consumer — rove — sets these
    /// post-init via `setSnapshotHooks`). When `snapshot_provider` is set, the
    /// storage's `snapshot` callback delegates to it (the app produces a
    /// snapshot *descriptor* pointing at the materialized tenant state) instead
    /// of the MemStorage stub that always reports SnapshotTemporarilyUnavailable.
    /// `apply_handler` installs an inbound snapshot's application state (the app
    /// loads the bundle the descriptor names + advances its durable watermark);
    /// it runs from `apply_snapshot` BEFORE the in-memory log is reset. `ctx` is
    /// opaque (rove's per-group handle).
    snapshot_ctx: ?*anyopaque = null,
    snapshot_provider: ?SnapshotProviderFn = null,
    apply_handler: ?ApplyHandlerFn = null,

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

            // Pre-pass: the highest compaction marker is the snapshot
            // point. Entries at or below it were dropped by compaction —
            // their state lives in the application snapshot, and the
            // segment that held them may have been GC'd, so replaying
            // them would gap. Anchor the sentinel there and skip them.
            var snap_index: u64 = 0;
            var snap_term: u64 = 0;
            for (bucket.items) |r| {
                if (r.tag == .compaction and r.payload.len == COMPACTION_PAYLOAD_LEN) {
                    const idx = std.mem.readInt(u64, r.payload[0..8], .little);
                    if (idx >= snap_index) {
                        snap_index = idx;
                        snap_term = std.mem.readInt(u64, r.payload[8..16], .little);
                    }
                }
            }
            if (snap_index > 0) {
                try self.mem.resetToSnapshot(snap_index, snap_term);
                self.compaction_index = snap_index;
                self.entry_offsets.clearRetainingCapacity();
                try self.entry_offsets.append(self.allocator, 0); // sentinel slot
            }

            for (bucket.items) |r| switch (r.tag) {
                .entry => {
                    const e = try parseEntryPayload(r.payload);
                    if (e.index <= snap_index) continue; // covered by the snapshot
                    try self.replayEntry(e, r.offset);
                },
                .hardstate => self.replayHardState(try parseHardStatePayload(r.payload)),
                .compaction => {}, // consumed in the pre-pass
                .confstate => {}, // not produced today
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

    /// Wire the application's snapshot hooks (rove sets these right after
    /// `init`/`initRecover`, while it still holds the `*GroupedFileStorage`).
    /// Until set, `snapshot` reports SnapshotTemporarilyUnavailable (the
    /// historical single-node behaviour) and `apply_snapshot` only resets the
    /// in-memory log.
    pub fn setSnapshotHooks(
        self: *GroupedFileStorage,
        ctx: ?*anyopaque,
        provider: ?SnapshotProviderFn,
        apply: ?ApplyHandlerFn,
    ) void {
        self.snapshot_ctx = ctx;
        self.snapshot_provider = provider;
        self.apply_handler = apply;
    }

    pub fn deinit(self: *GroupedFileStorage) void {
        // NB: we do NOT close `self.wal` — it's borrowed; the dispatcher
        // owns it and tears it down after all groups sharing it have
        // been destroyed. We also do NOT call `wal.noteGroupDestroyed`
        // here: `deinit` runs on ordinary shutdown too, and a group's
        // WAL records must survive shutdown for restart recovery.
        // Reclaiming a group's segments is an *intentional* migration-
        // detach action — the dispatcher calls `wal.noteGroupDestroyed`
        // explicitly for that, separate from storage teardown.
        self.mem.deinit();
        self.entry_offsets.deinit(self.allocator);
        self.scratch.deinit(self.allocator);
        self.allocator.destroy(self);
    }

    /// Compact this group's log through `index`: advance the in-memory
    /// log (raising `first_index`) and drop the matching `entry_offsets`
    /// slots in lockstep, then record the watermark. The shared WAL file
    /// is NOT rewritten here — those on-disk records simply become dead;
    /// segment GC reclaims the bytes once an entire segment has fallen
    /// below every group's `compaction_index`. No-op below the current
    /// watermark. Caller contract (raft's): only compact up to the
    /// applied index.
    pub fn compact(self: *GroupedFileStorage, index: u64) !void {
        const dummy_idx = self.mem.entries.items[0].index;
        if (index <= dummy_idx) return; // already compacted past here
        // mem.compact validates the upper bound and advances the sentinel.
        try self.mem.compact(index);
        // entry_offsets tracks mem.entries 1:1; mirror the same drop so
        // a later tail-truncate still maps indices correctly.
        const offset: usize = @intCast(index - dummy_idx);
        std.mem.copyForwards(u64, self.entry_offsets.items[0..], self.entry_offsets.items[offset..]);
        self.entry_offsets.shrinkRetainingCapacity(self.entry_offsets.items.len - offset);
        self.entry_offsets.items[0] = 0; // sentinel slot carries no record
        self.compaction_index = index;

        // Persist a compaction marker so recovery can anchor the log
        // sentinel at {index, term} even after the segment that held the
        // dropped entries is GC'd. The term is the sentinel's term (the
        // term at the compact index), which mem.compact just preserved.
        var cp: [COMPACTION_PAYLOAD_LEN]u8 = undefined;
        std.mem.writeInt(u64, cp[0..8], index, .little);
        std.mem.writeInt(u64, cp[8..16], self.mem.entries.items[0].term, .little);
        _ = try self.wal.appendRecord(self.group_id, .compaction, &cp);

        // Tell the WAL so it can reclaim any segment now fully below
        // this (and every other group's) watermark.
        try self.wal.noteCompaction(self.group_id, index);
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
    if (self.snapshot_provider) |provider| {
        const rc = provider(
            self.snapshot_ctx,
            self.group_id,
            request_index,
            out_data,
            out_data_len,
            out_meta_index,
            out_meta_term,
        );
        if (rc != 0) return rc; // -1 = TemporarilyUnavailable (still preparing)
        const idx = out_meta_index.*;
        // raft-rs requires the snapshot to cover at least `request_index`; if
        // the app hasn't materialized that far yet, report Unavailable so raft
        // retries (a later durabilize advances the materialized point).
        if (idx < request_index) return -1;
        // The log term at the snapshot index is authoritative — derive it from
        // our own log rather than trusting the app. `idx` is the durabilized
        // index, which is >= the compaction sentinel, so `termAt` resolves.
        const t = self.mem.termAt(idx) orelse return -1;
        out_meta_term.* = t;
        return 0;
    }
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
    // Install the application state FIRST (rove loads the staged tenant bundle
    // into LMDB + stamps the durable watermark = meta_index). Only if that
    // succeeds do we reset the raft log + persist the compaction anchor — so a
    // crash before the state is durable leaves the node at its old position to
    // re-receive the snapshot, never a torn half-applied state.
    if (self.apply_handler) |apply| {
        const rc = apply(self.snapshot_ctx, self.group_id, data, data_len, meta_index, meta_term);
        if (rc != 0) return rc;
    }
    // Reset the in-memory log to the snapshot point + advance hardstate.
    const rc = mem_storage.vtable.apply_snapshot.?(self.mem, data, data_len, meta_index, meta_term);
    if (rc != 0) return rc;
    // The log now starts above meta_index: drop the offset map to a lone
    // sentinel slot and record the compaction watermark, mirroring `compact`.
    self.compaction_index = meta_index;
    self.entry_offsets.clearRetainingCapacity();
    self.entry_offsets.append(self.allocator, 0) catch return -1;
    // Persist a compaction marker so recovery anchors the sentinel at
    // {meta_index, meta_term} even after the pre-snapshot segments are GC'd.
    var cp: [COMPACTION_PAYLOAD_LEN]u8 = undefined;
    std.mem.writeInt(u64, cp[0..8], meta_index, .little);
    std.mem.writeInt(u64, cp[8..16], meta_term, .little);
    _ = self.wal.appendRecord(self.group_id, .compaction, &cp) catch return -1;
    self.wal.noteCompaction(self.group_id, meta_index) catch return -1;
    return 0;
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

test "GroupedFileStorage.compact: drops offset slots in lockstep with mem, advances watermark" {
    var h = try Harness.init();
    defer h.deinit();
    const wal = try SharedWal.init(testing.allocator, h.path);
    defer wal.deinit();
    const g = try GroupedFileStorage.init(testing.allocator, &.{1}, wal, 1);
    defer g.deinit();

    var b = [_]c.RaftEntryFfi{ fakeEntry(1, 1, "a"), fakeEntry(1, 2, "b"), fakeEntry(1, 3, "c") };
    try testing.expectEqual(@as(i32, 0), appendEntriesCb(g, &b, b.len));
    try testing.expectEqual(@as(usize, 4), g.entry_offsets.items.len); // sentinel + 3

    try g.compact(2);
    try testing.expectEqual(@as(u64, 2), g.compaction_index);
    try testing.expectEqual(@as(u64, 3), g.mem.firstIndex());
    // Offset map shrank in lockstep: sentinel + the one surviving entry,
    // whose slot still points at index 3's record (2 * 47 = 94).
    try testing.expectEqual(@as(usize, 2), g.entry_offsets.items.len);
    try testing.expectEqual(@as(u64, 0), g.entry_offsets.items[0]);
    try testing.expectEqual(@as(u64, 94), g.entry_offsets.items[1]);
}

test "segmentation: rolling seals segments; the group keeps its full log" {
    var h = try Harness.init();
    defer h.deinit();
    const wal = try SharedWal.initWithTarget(testing.allocator, h.path, 50);
    defer wal.deinit();
    const g = try GroupedFileStorage.init(testing.allocator, &.{1}, wal, 1);
    defer g.deinit();

    // 47-byte records vs a 50-byte target → rolls between most entries.
    var b = [_]c.RaftEntryFfi{
        fakeEntry(1, 1, "a"), fakeEntry(1, 2, "b"), fakeEntry(1, 3, "c"),
        fakeEntry(1, 4, "d"), fakeEntry(1, 5, "e"),
    };
    try testing.expectEqual(@as(i32, 0), appendEntriesCb(g, &b, b.len));

    try testing.expect(wal.sealed.items.len >= 1);
    var pbuf: [std.fs.max_path_bytes]u8 = undefined;
    const p = try sealedSegmentPath(&pbuf, h.path, 1);
    try std.fs.cwd().access(p, .{}); // the first sealed segment exists on disk
    // The active stays at the base path; the in-memory log is intact.
    try testing.expectEqual(@as(u64, 5), g.mem.lastIndex());
}

test "segment GC: sealed segments are deleted once the group compacts past them" {
    var h = try Harness.init();
    defer h.deinit();
    const wal = try SharedWal.initWithTarget(testing.allocator, h.path, 50);
    defer wal.deinit();
    const g = try GroupedFileStorage.init(testing.allocator, &.{1}, wal, 1);
    defer g.deinit();

    var b = [_]c.RaftEntryFfi{ fakeEntry(1, 1, "a"), fakeEntry(1, 2, "b"), fakeEntry(1, 3, "c"), fakeEntry(1, 4, "d") };
    try testing.expectEqual(@as(i32, 0), appendEntriesCb(g, &b, b.len));
    try testing.expect(wal.sealed.items.len >= 1);

    try g.compact(4); // drop everything ≤ 4 → every sealed segment is dead
    try testing.expectEqual(@as(usize, 0), wal.sealed.items.len);
    var pbuf: [std.fs.max_path_bytes]u8 = undefined;
    const p = try sealedSegmentPath(&pbuf, h.path, 1);
    try testing.expectError(error.FileNotFound, std.fs.cwd().access(p, .{}));
}

test "segment GC: a segment shared with a still-live group is not deleted" {
    var h = try Harness.init();
    defer h.deinit();
    const wal = try SharedWal.initWithTarget(testing.allocator, h.path, 60);
    defer wal.deinit();
    const g1 = try GroupedFileStorage.init(testing.allocator, &.{1}, wal, 1);
    defer g1.deinit();
    const g2 = try GroupedFileStorage.init(testing.allocator, &.{1}, wal, 2);
    defer g2.deinit();

    // g1@1 and g2@1 land in the first segment; the next append rolls it.
    var a1 = [_]c.RaftEntryFfi{fakeEntry(1, 1, "a")};
    try testing.expectEqual(@as(i32, 0), appendEntriesCb(g1, &a1, 1));
    var a2 = [_]c.RaftEntryFfi{fakeEntry(1, 1, "b")};
    try testing.expectEqual(@as(i32, 0), appendEntriesCb(g2, &a2, 1));
    var a3 = [_]c.RaftEntryFfi{fakeEntry(1, 2, "c")};
    try testing.expectEqual(@as(i32, 0), appendEntriesCb(g1, &a3, 1)); // rolls, sealing {g1:1, g2:1}
    try testing.expect(wal.sealed.items.len >= 1);

    var pbuf: [std.fs.max_path_bytes]u8 = undefined;
    const p = try sealedSegmentPath(&pbuf, h.path, 1);

    // Compacting only g1 leaves g2's entry live in segment 1 → kept.
    try g1.compact(1);
    try std.fs.cwd().access(p, .{});

    // Now g2 is compacted too → segment 1 has no live entries → deleted.
    try g2.compact(1);
    try testing.expectError(error.FileNotFound, std.fs.cwd().access(p, .{}));
}

test "segment recovery: full log + hard state survive across segments and GC" {
    var h = try Harness.init();
    defer h.deinit();

    {
        const wal = try SharedWal.initWithTarget(testing.allocator, h.path, 60);
        defer wal.deinit();
        const g = try GroupedFileStorage.init(testing.allocator, &.{1}, wal, 1);
        defer g.deinit();

        const hs: c.RaftHardStateFfi = .{ .term = 7, .vote = 1, .commit = 3 };
        try testing.expectEqual(@as(i32, 0), setHardStateCb(g, &hs));
        var b = [_]c.RaftEntryFfi{ fakeEntry(7, 1, "a"), fakeEntry(7, 2, "b"), fakeEntry(7, 3, "c"), fakeEntry(7, 4, "d") };
        try testing.expectEqual(@as(i32, 0), appendEntriesCb(g, &b, b.len));

        // Compact through 3. Every sealed segment holding only entries ≤ 3
        // is GC'd — including the one that first recorded the hard state.
        // It survives only because each new segment's header re-baselined
        // it (and the compaction marker).
        try g.compact(3);
        try wal.flush();
    }

    const wal = try SharedWal.open(testing.allocator, h.path);
    defer wal.deinit();
    const g = try GroupedFileStorage.initRecover(testing.allocator, &.{1}, wal, 1);
    defer g.deinit();

    // The compacted prefix is gone — recovery anchors the sentinel at the
    // compaction point (index 3, term 7) and replays only entry 4. No gap.
    try testing.expectEqual(@as(u64, 4), g.mem.firstIndex());
    try testing.expectEqual(@as(u64, 4), g.mem.lastIndex());
    try testing.expectEqual(@as(?u64, 7), g.mem.termAt(3)); // sentinel term
    try testing.expectEqual(@as(?u64, 7), g.mem.termAt(4));
    // Hard state recovered though its original segment was deleted.
    try testing.expectEqual(@as(u64, 7), g.mem.hs_term);
    try testing.expectEqual(@as(u64, 3), g.mem.hs_commit);
}

test "segment GC: noteGroupDestroyed reclaims a detached group's sealed segments" {
    var h = try Harness.init();
    defer h.deinit();
    const wal = try SharedWal.initWithTarget(testing.allocator, h.path, 50);
    defer wal.deinit();
    const g = try GroupedFileStorage.init(testing.allocator, &.{1}, wal, 1);
    defer g.deinit();

    var b = [_]c.RaftEntryFfi{ fakeEntry(1, 1, "a"), fakeEntry(1, 2, "b"), fakeEntry(1, 3, "c") };
    try testing.expectEqual(@as(i32, 0), appendEntriesCb(g, &b, b.len));
    try testing.expect(wal.sealed.items.len >= 1);
    var pbuf: [std.fs.max_path_bytes]u8 = undefined;
    const p = try sealedSegmentPath(&pbuf, h.path, 1);
    try std.fs.cwd().access(p, .{}); // sealed segment exists

    // An intentional migration detach: the group's records are dead at
    // once (no compaction needed), so its sealed segments are reclaimed.
    wal.noteGroupDestroyed(1);
    try testing.expectEqual(@as(usize, 0), wal.sealed.items.len);
    try testing.expectError(error.FileNotFound, std.fs.cwd().access(p, .{}));
}
