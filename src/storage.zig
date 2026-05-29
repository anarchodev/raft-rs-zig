const std = @import("std");
const c = @cImport({
    @cInclude("raft_sys.h");
});

/// In-memory log + state store, owned by Zig. One instance per raft group.
pub const MemStorage = struct {
    allocator: std.mem.Allocator,

    voters: std.ArrayList(u64),
    learners: std.ArrayList(u64),

    hs_term: u64 = 0,
    hs_vote: u64 = 0,
    hs_commit: u64 = 0,

    /// `entries[0]` is a sentinel at the snapshot index (initially {idx=0,term=0}).
    /// Real log entries follow at increasing indices.
    entries: std.ArrayList(StoredEntry),

    /// Reused buffer for the `entries` callback so we don't allocate on every call.
    /// Single-threaded use only.
    entries_buf: std.ArrayList(c.RaftEntryFfi),

    pub const StoredEntry = struct {
        entry_type: u32,
        term: u64,
        index: u64,
        data: []u8,
        context: []u8,
        sync_log: bool,
    };

    pub fn init(allocator: std.mem.Allocator, voters: []const u64) !*MemStorage {
        const self = try allocator.create(MemStorage);
        self.* = .{
            .allocator = allocator,
            .voters = .empty,
            .learners = .empty,
            .entries = .empty,
            .entries_buf = .empty,
        };
        try self.voters.appendSlice(allocator, voters);
        try self.entries.append(allocator, .{
            .entry_type = 0,
            .term = 0,
            .index = 0,
            .data = &.{},
            .context = &.{},
            .sync_log = false,
        });
        return self;
    }

    pub fn deinit(self: *MemStorage) void {
        for (self.entries.items) |e| {
            if (e.data.len > 0) self.allocator.free(e.data);
            if (e.context.len > 0) self.allocator.free(e.context);
        }
        self.entries.deinit(self.allocator);
        self.entries_buf.deinit(self.allocator);
        self.voters.deinit(self.allocator);
        self.learners.deinit(self.allocator);
        const a = self.allocator;
        a.destroy(self);
    }

    pub fn firstIndex(self: *const MemStorage) u64 {
        return self.entries.items[0].index + 1;
    }

    pub fn lastIndex(self: *const MemStorage) u64 {
        return self.entries.items[self.entries.items.len - 1].index;
    }

    pub fn termAt(self: *const MemStorage, idx: u64) ?u64 {
        const dummy_idx = self.entries.items[0].index;
        if (idx < dummy_idx) return null;
        const offset = idx - dummy_idx;
        if (offset >= self.entries.items.len) return null;
        return self.entries.items[@intCast(offset)].term;
    }

    /// Compact the log through `compact_index`: drop entries with index
    /// <= `compact_index` and advance the snapshot sentinel to that
    /// index (preserving its term). `first_index` becomes
    /// `compact_index + 1`. The application calls this after it has
    /// durably materialized state through `compact_index` (so those
    /// entries are no longer needed to recover) — i.e. only ever up to
    /// the applied index, which is raft's contract for compaction.
    ///
    /// No-op if already compacted at/after `compact_index`; errors if
    /// `compact_index` is past the last entry (can't compact a
    /// not-yet-present suffix).
    pub fn compact(self: *MemStorage, compact_index: u64) !void {
        const dummy_idx = self.entries.items[0].index;
        if (compact_index <= dummy_idx) return; // already compacted past here
        if (compact_index > self.lastIndex()) return error.CompactIndexOutOfBounds;

        const offset: usize = @intCast(compact_index - dummy_idx);
        // Free the data of everything dropped, including the entry that
        // is about to become the (data-less) sentinel. Kept entries
        // live at offset+1.. — their data is untouched.
        for (self.entries.items[0 .. offset + 1]) |e| {
            if (e.data.len > 0) self.allocator.free(e.data);
            if (e.context.len > 0) self.allocator.free(e.context);
        }
        const term_keep = self.entries.items[offset].term;
        self.entries.items[offset] = .{
            .entry_type = 0,
            .term = term_keep,
            .index = compact_index,
            .data = &.{},
            .context = &.{},
            .sync_log = false,
        };
        // Shift the new sentinel + kept tail down to the front. Stale
        // duplicate structs left beyond the new length share data
        // pointers with the kept entries, but `deinit` only walks
        // `items[0..len]`, so each buffer is freed exactly once.
        std.mem.copyForwards(StoredEntry, self.entries.items[0..], self.entries.items[offset..]);
        self.entries.shrinkRetainingCapacity(self.entries.items.len - offset);
    }

    /// Reset the log to a compacted snapshot point: discard every entry
    /// and set the sentinel to {index, term}. `first_index` becomes
    /// `index + 1`. Recovery uses this when a compaction record says the
    /// log starts above 1 — the state through `index` lives in the
    /// application's snapshot (kvexp), not the raft log, so there are no
    /// entries to replay below it, only a sentinel to anchor `term`.
    pub fn resetToSnapshot(self: *MemStorage, index: u64, term: u64) !void {
        for (self.entries.items) |e| {
            if (e.data.len > 0) self.allocator.free(e.data);
            if (e.context.len > 0) self.allocator.free(e.context);
        }
        self.entries.clearRetainingCapacity();
        try self.entries.append(self.allocator, .{
            .entry_type = 0,
            .term = term,
            .index = index,
            .data = &.{},
            .context = &.{},
            .sync_log = false,
        });
    }

    fn appendOne(self: *MemStorage, e: c.RaftEntryFfi) !void {
        const last_idx = self.lastIndex();
        if (e.index <= last_idx) {
            // Truncate from e.index onwards (raft may overwrite uncommitted tail).
            const dummy_idx = self.entries.items[0].index;
            const truncate_at: usize = @intCast(e.index - dummy_idx);
            for (self.entries.items[truncate_at..]) |old| {
                if (old.data.len > 0) self.allocator.free(old.data);
                if (old.context.len > 0) self.allocator.free(old.context);
            }
            self.entries.shrinkRetainingCapacity(truncate_at);
        } else if (e.index > last_idx + 1) {
            return error.GapInLog;
        }
        const data_copy: []u8 = if (e.data_len > 0)
            try self.allocator.dupe(u8, e.data[0..e.data_len])
        else
            &[_]u8{};
        const ctx_copy: []u8 = if (e.context_len > 0)
            try self.allocator.dupe(u8, e.context[0..e.context_len])
        else
            &[_]u8{};
        try self.entries.append(self.allocator, .{
            .entry_type = e.entry_type,
            .term = e.term,
            .index = e.index,
            .data = data_copy,
            .context = ctx_copy,
            .sync_log = e.sync_log,
        });
    }
};

// ── Vtable callbacks ─────────────────────────────────────────────────────────

fn destroyCb(ud: ?*anyopaque) callconv(.c) void {
    const self: *MemStorage = @ptrCast(@alignCast(ud.?));
    self.deinit();
}

fn initialStateCb(
    ud: ?*anyopaque,
    out_hs: [*c]c.RaftHardStateFfi,
    out_cs: [*c]c.RaftConfStateFfi,
) callconv(.c) i32 {
    const self: *MemStorage = @ptrCast(@alignCast(ud.?));
    out_hs.* = .{ .term = self.hs_term, .vote = self.hs_vote, .commit = self.hs_commit };
    out_cs.* = .{
        .voters = self.voters.items.ptr,
        .voters_len = self.voters.items.len,
        .learners = self.learners.items.ptr,
        .learners_len = self.learners.items.len,
        .voters_outgoing = null,
        .voters_outgoing_len = 0,
        .learners_next = null,
        .learners_next_len = 0,
        .auto_leave = false,
    };
    return 0;
}

fn entriesCb(
    ud: ?*anyopaque,
    low: u64,
    high: u64,
    max_size: u64,
    out: [*c]c.RaftEntriesOut,
) callconv(.c) i32 {
    _ = max_size;
    const self: *MemStorage = @ptrCast(@alignCast(ud.?));
    // -2 = Compacted (asked below first_index → entries are gone, raft
    // should fall back to a snapshot); -1 = Unavailable (asked past the
    // tail → not yet present). raft-rs treats these very differently.
    if (low < self.firstIndex()) return -2;
    if (high > self.lastIndex() + 1) return -1;

    self.entries_buf.clearRetainingCapacity();
    const dummy_idx = self.entries.items[0].index;
    const start: usize = @intCast(low - dummy_idx);
    const end: usize = @intCast(high - dummy_idx);
    for (self.entries.items[start..end]) |e| {
        self.entries_buf.append(self.allocator, .{
            .entry_type = e.entry_type,
            .term = e.term,
            .index = e.index,
            .data = e.data.ptr,
            .data_len = e.data.len,
            .context = e.context.ptr,
            .context_len = e.context.len,
            .sync_log = e.sync_log,
        }) catch return -1;
    }
    out.* = .{
        .entries = self.entries_buf.items.ptr,
        .len = self.entries_buf.items.len,
        .free_fn = null, // buffer reused, no free needed
    };
    return 0;
}

fn termCb(ud: ?*anyopaque, idx: u64, out: [*c]u64) callconv(.c) i32 {
    const self: *MemStorage = @ptrCast(@alignCast(ud.?));
    // Below the sentinel index the term is gone to compaction (-2);
    // term(sentinel_idx) itself is still valid (the snapshot's term).
    // Above the last index it's simply not present yet (-1).
    if (idx < self.entries.items[0].index) return -2;
    if (self.termAt(idx)) |t| {
        out.* = t;
        return 0;
    }
    return -1;
}

fn firstIndexCb(ud: ?*anyopaque, out: [*c]u64) callconv(.c) i32 {
    const self: *MemStorage = @ptrCast(@alignCast(ud.?));
    out.* = self.firstIndex();
    return 0;
}

fn lastIndexCb(ud: ?*anyopaque, out: [*c]u64) callconv(.c) i32 {
    const self: *MemStorage = @ptrCast(@alignCast(ud.?));
    out.* = self.lastIndex();
    return 0;
}

fn snapshotCb(
    ud: ?*anyopaque,
    request_index: u64,
    out_data: [*c][*c]const u8,
    out_data_len: [*c]usize,
    out_meta_index: [*c]u64,
    out_meta_term: [*c]u64,
) callconv(.c) i32 {
    _ = ud;
    _ = request_index;
    _ = out_data;
    _ = out_data_len;
    _ = out_meta_index;
    _ = out_meta_term;
    return -1; // SnapshotTemporarilyUnavailable
}

fn appendEntriesCb(
    ud: ?*anyopaque,
    entries: [*c]const c.RaftEntryFfi,
    n: usize,
) callconv(.c) i32 {
    const self: *MemStorage = @ptrCast(@alignCast(ud.?));
    var i: usize = 0;
    while (i < n) : (i += 1) {
        self.appendOne(entries[i]) catch return -1;
    }
    return 0;
}

fn setHardStateCb(ud: ?*anyopaque, hs: [*c]const c.RaftHardStateFfi) callconv(.c) i32 {
    const self: *MemStorage = @ptrCast(@alignCast(ud.?));
    self.hs_term = hs.*.term;
    self.hs_vote = hs.*.vote;
    self.hs_commit = hs.*.commit;
    return 0;
}

fn applySnapshotCb(
    ud: ?*anyopaque,
    data: [*c]const u8,
    data_len: usize,
    meta_index: u64,
    meta_term: u64,
) callconv(.c) i32 {
    _ = ud;
    _ = data;
    _ = data_len;
    _ = meta_index;
    _ = meta_term;
    return 0; // stub
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

fn te(term: u64, index: u64, data: []const u8) c.RaftEntryFfi {
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

test "MemStorage.compact: advances first_index, keeps sentinel term + later entries" {
    const s = try MemStorage.init(testing.allocator, &.{1});
    defer s.deinit();
    try s.appendOne(te(1, 1, "a"));
    try s.appendOne(te(1, 2, "b"));
    try s.appendOne(te(2, 3, "c"));
    try testing.expectEqual(@as(u64, 1), s.firstIndex());
    try testing.expectEqual(@as(u64, 3), s.lastIndex());

    try s.compact(2);
    try testing.expectEqual(@as(u64, 3), s.firstIndex()); // compact_index + 1
    try testing.expectEqual(@as(u64, 3), s.lastIndex());
    // The sentinel sits at index 2 carrying entry 2's term; entry 3 stays.
    try testing.expectEqual(@as(?u64, 1), s.termAt(2));
    try testing.expectEqual(@as(?u64, 2), s.termAt(3));
    // Entry 1 is gone.
    try testing.expectEqual(@as(?u64, null), s.termAt(1));
}

test "MemStorage.compact: no-op below the watermark, error past the log" {
    const s = try MemStorage.init(testing.allocator, &.{1});
    defer s.deinit();
    try s.appendOne(te(1, 1, "a"));
    try s.appendOne(te(1, 2, "b"));

    try s.compact(2);
    try s.compact(1); // below current sentinel → no-op
    try s.compact(2); // at current sentinel → no-op
    try testing.expectEqual(@as(u64, 3), s.firstIndex());

    try testing.expectError(error.CompactIndexOutOfBounds, s.compact(5));
}

test "MemStorage: term/entries callbacks report Compacted (-2) vs Unavailable (-1)" {
    const s = try MemStorage.init(testing.allocator, &.{1});
    defer s.deinit();
    try s.appendOne(te(1, 1, "a"));
    try s.appendOne(te(1, 2, "b"));
    try s.appendOne(te(1, 3, "c"));
    try s.compact(2);

    var t: u64 = 0;
    try testing.expectEqual(@as(i32, -2), termCb(s, 1, &t)); // compacted away
    try testing.expectEqual(@as(i32, 0), termCb(s, 2, &t)); // sentinel term survives
    try testing.expectEqual(@as(i32, -1), termCb(s, 9, &t)); // past the tail

    var out: c.RaftEntriesOut = undefined;
    const max = std.math.maxInt(u64);
    try testing.expectEqual(@as(i32, -2), entriesCb(s, 1, 4, max, &out)); // low compacted
    try testing.expectEqual(@as(i32, -1), entriesCb(s, 3, 9, max, &out)); // high past tail
    try testing.expectEqual(@as(i32, 0), entriesCb(s, 3, 4, max, &out)); // valid window
}
