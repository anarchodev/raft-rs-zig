//! Idiomatic Zig wrapper around the C FFI exposed by raft-sys
//! (which itself wraps tikv's `raft-rs 0.7` via cbindgen). The
//! wrapper is intentionally thin — it doesn't hide the lifecycle
//! quirks of raft-rs (need to call `tick` periodically to drive
//! timers, then `poll_ready` to discover groups with pending work,
//! then `process_ready` to apply that work via a callback). It
//! does erase the C-ABI noise: bool returns, slices instead of
//! pointer+length pairs, Zig errors instead of int return codes.
//!
//! Apply callbacks remain raw `callconv(.c)` for now — consumers
//! that want a "give me a Zig closure" wrapper can build one on
//! top, but the trampoline pattern adds a hop that we don't pay
//! for unless we need it. The cost matters: `process_ready` fires
//! the callback once per applied entry, on the hot path.

const std = @import("std");
const c = @cImport({
    @cInclude("raft_sys.h");
});
const storage_mod = @import("storage.zig");
const file_storage_mod = @import("file_storage.zig");
const grouped_file_storage_mod = @import("grouped_file_storage.zig");

pub const MemStorage = storage_mod.MemStorage;
pub const FileStorage = file_storage_mod.FileStorage;
pub const SharedWal = grouped_file_storage_mod.SharedWal;
pub const GroupedFileStorage = grouped_file_storage_mod.GroupedFileStorage;
/// Re-export of the C-ABI storage vtable type so consumers can
/// pass it around (as `*const StorageVTable`) without their own
/// `@cImport` of `raft_sys.h`.
pub const StorageVTable = c.RaftStorageVTable;
pub const storage_vtable: *const c.RaftStorageVTable = @ptrCast(&storage_mod.vtable);
pub const file_storage_vtable: *const c.RaftStorageVTable = @ptrCast(&file_storage_mod.vtable);
pub const grouped_file_storage_vtable: *const c.RaftStorageVTable =
    @ptrCast(&grouped_file_storage_mod.vtable);

/// Re-export of the C-ABI per-group raft config (the tunable subset of
/// `raft::Config`). Build one with `defaultGroupConfig()` and tweak the
/// fields you care about, then pass `&cfg` to `createGroupEpoch`.
/// Passing `null` instead selects raft-sys's historical defaults
/// (identical to `defaultGroupConfig()`).
pub const GroupConfig = c.RaftGroupConfig;

/// A valid `GroupConfig` carrying the defaults raft-sys has always used
/// (raft-rs 0.7 defaults, except `election_tick`/`heartbeat_tick` which
/// we pin to 10/3). Start here and override individual fields — e.g.
/// `var cfg = defaultGroupConfig(); cfg.pre_vote = true;`. Every field
/// below is chosen so `RawNode::new`'s `validate()` passes as-is.
pub fn defaultGroupConfig() GroupConfig {
    return .{
        .election_tick = 10,
        .heartbeat_tick = 3,
        .applied = 0,
        .max_size_per_msg = 0,
        .max_inflight_msgs = 256,
        .check_quorum = false,
        .pre_vote = false,
        .min_election_tick = 0, // 0 → raft uses election_tick
        .max_election_tick = 0, // 0 → raft uses 2 * election_tick
        .read_only_option = 0, // 0 = Safe, 1 = LeaseBased (needs check_quorum)
        .skip_bcast_commit = false,
        .batch_append = false,
        .priority = 0,
        .max_uncommitted_size = std.math.maxInt(u64), // raft's NO_LIMIT
        .max_committed_size_per_ready = std.math.maxInt(u64), // raft's NO_LIMIT
    };
}

/// Raw C-ABI apply callback signature. Re-exported so consumers
/// don't have to repeat the calling-convention boilerplate.
pub const ApplyCb = *const fn (
    userdata: ?*anyopaque,
    group_id: u64,
    index: u64,
    term: u64,
    data: [*c]const u8,
    len: usize,
) callconv(.c) void;

/// Raw C-ABI outbound-message callback signature. Fires once per
/// message queued during `processReady`, when the caller drives
/// `takeMessages`. `to` is the destination raft node id;
/// `msg_bytes` is the rust-protobuf serialization of the
/// `eraftpb::Message` and is valid only for the duration of the
/// callback. The caller hands `msg_bytes` to the peer's `step`.
pub const MessageCb = *const fn (
    userdata: ?*anyopaque,
    to: u64,
    msg_bytes: [*c]const u8,
    msg_len: usize,
) callconv(.c) void;

pub const Error = error{
    ManagerInitFailed,
    CreateGroupFailed,
    DestroyGroupFailed,
    ClearTombstoneFailed,
    CampaignFailed,
    ProposeFailed,
    /// A conf-change demote/remove was refused because it would leave the
    /// group with fewer than 2 voters (the propose-side quorum guard).
    ConfChangeQuorumGuard,
    /// The propose was refused because this node is not the group's
    /// raft leader — nothing was stepped into raft, so nothing can
    /// commit from it. (Backstop for raft-rs 0.7's unconditional
    /// follower proposal forwarding; see raft_manager_propose.)
    NotLeader,
    /// A local-snapshot baseline install was refused because the requested
    /// index is not ahead of the group's committed index (nothing to install).
    SnapshotStale,
    /// A local-snapshot baseline was malformed: index 0, or term 0 for an
    /// index>0 (a term-0 baseline crashes raft's restore — rejected both at this
    /// wrapper and in the FFI engine, code -5).
    InvalidBaseline,
    /// A caller-supplied snapshot ConfState (membership SSOT) did not contain
    /// this node — raft's `restore` would discard the snapshot (raft.rs:2581), so
    /// the FFI rejected it (code -6). The node must be conf-change-added to the
    /// group's membership BEFORE its baseline is installed (TiKV's add-peer-then-
    /// snapshot ordering).
    SelfNotInConfState,
    ProcessReadyFailed,
    TakeMessagesFailed,
    StepFailed,
    StepDecodeFailed,
    /// `stepFenced` dropped the message: its sender epoch was older
    /// than the group's current incarnation (migration fence).
    StepFenced,
    /// `setEpoch` rejected a request to lower a group's epoch.
    EpochRegression,
    UnknownGroup,
};

/// One `raft-rs` MultiRaft manager. Owns all the groups beneath
/// it; `deinit` walks them and frees their storage via each
/// group's destroy-vtable callback.
pub const Manager = struct {
    ptr: *c.RaftManager,

    pub fn init() Error!Manager {
        const p = c.raft_manager_new() orelse return Error.ManagerInitFailed;
        return .{ .ptr = p };
    }

    pub fn deinit(self: *Manager) void {
        c.raft_manager_free(self.ptr);
        self.* = undefined;
    }

    /// Create a raft group identified by `group_id`. `node_id` is
    /// this node's identity within the group's voter set. The
    /// `storage` pointer is held by raft-rs via the vtable's
    /// userdata slot; the storage is freed when the group is
    /// destroyed (via the `destroy` vtable callback).
    ///
    /// `vtable` selects the storage implementation. Pass
    /// `storage_vtable` for the in-memory MemStorage (most callers)
    /// or `file_storage.vtable` for the WAL-backed FileStorage. Any
    /// vtable whose userdata layout matches the pointer passed in
    /// `storage_userdata` works — the C ABI here is opaque.
    pub fn createGroup(
        self: *Manager,
        group_id: u64,
        node_id: u64,
        vtable: *const c.RaftStorageVTable,
        storage_userdata: *anyopaque,
    ) Error!void {
        // Birth epoch 0 — the never-migrated case. A group that is
        // being attached as part of a migration uses
        // `createGroupEpoch` so the fence is live from creation.
        // `null` cfg → raft-sys defaults.
        return self.createGroupEpoch(group_id, node_id, 0, vtable, storage_userdata, null);
    }

    /// `createGroup` with an explicit migration fence epoch (see
    /// `stepFenced`). The control plane passes the epoch it assigned
    /// to this move so inbound messages from the group's previous
    /// incarnation are fenced out from the instant the group exists.
    pub fn createGroupEpoch(
        self: *Manager,
        group_id: u64,
        node_id: u64,
        epoch: u64,
        vtable: *const c.RaftStorageVTable,
        storage_userdata: *anyopaque,
        cfg: ?*const GroupConfig,
    ) Error!void {
        // `node_id` is this node's identity within the group's
        // voter set — must match one of the `voters` the storage
        // returns from `initial_state`. Single-node tests/demos
        // always pass 1; multi-node uses 1..=cluster_size.
        //
        // `cfg` tunes the group's raft::Config (pre_vote, check_quorum,
        // election-tick window, …); `null` selects raft-sys defaults.
        // An invalid config is rejected by RawNode::new → -3 →
        // CreateGroupFailed.
        const rc = c.raft_manager_create_group(self.ptr, group_id, node_id, epoch, vtable, storage_userdata, cfg);
        if (rc != 0) return Error.CreateGroupFailed;
    }

    /// Read a group's current migration fence epoch. The transport
    /// stamps outbound messages with this; the peer hands it back to
    /// `stepFenced`. Returns 0 for an unknown group (same value as a
    /// legitimate epoch-0 group — check `hasGroup` if you must tell
    /// them apart).
    pub fn groupEpoch(self: *const Manager, group_id: u64) u64 {
        return c.raft_manager_group_epoch(self.ptr, group_id);
    }

    /// Set a live group's migration fence epoch. Monotonic — lowering
    /// it is rejected (`Error.EpochRegression`) because that would
    /// re-admit already-fenced traffic. Birth epoch for a migration
    /// attach belongs in `createGroupEpoch`, not here.
    pub fn setEpoch(self: *Manager, group_id: u64, epoch: u64) Error!void {
        return switch (c.raft_manager_set_epoch(self.ptr, group_id, epoch)) {
            0 => {},
            -2 => Error.EpochRegression,
            else => Error.UnknownGroup,
        };
    }

    /// Tear down the group. Storage is freed via the `destroy`
    /// vtable callback; any outstanding pointers into it are
    /// dangling after this call. The group's id enters a tombstone
    /// set so a later `createGroup` with the same id will fail —
    /// preventing accidental id reuse. Migration-style attach (an
    /// intentional reuse after detach) must call `clearTombstone`
    /// before `createGroup`.
    pub fn destroyGroup(self: *Manager, group_id: u64) Error!void {
        if (c.raft_manager_destroy_group(self.ptr, group_id) != 0) return Error.DestroyGroupFailed;
    }

    /// Lift the tombstone for `group_id`, so a subsequent
    /// `createGroup` with that id can succeed. The intended caller
    /// is `attachGroup` (Stage 3d migration) reusing a tenant id
    /// that was previously destroyed via `detachGroup`. A no-op on
    /// a non-tombstoned id.
    pub fn clearTombstone(self: *Manager, group_id: u64) Error!void {
        if (c.raft_manager_clear_tombstone(self.ptr, group_id) != 0)
            return Error.ClearTombstoneFailed;
    }

    /// Force this node to become leader of `group_id`. Useful for
    /// single-node testing; in multi-node deployments the
    /// election fires automatically when a campaign timeout
    /// elapses (driven via `tickAll`).
    pub fn campaign(self: *Manager, group_id: u64) Error!void {
        if (c.raft_manager_campaign(self.ptr, group_id) != 0) return Error.CampaignFailed;
    }

    /// Force an election on `group_id` that bypasses peers' `check_quorum`
    /// leases — the recovery primitive for a hibernated survivor after a HARD
    /// (SIGKILL) leader loss, where a normal campaign's votes are ignored by
    /// frozen followers whose lease has not visibly expired. Drives
    /// `Raft::campaign(CAMPAIGN_TRANSFER)` (forced, pre-vote-free). Safe no-op
    /// on a leader, a learner, or a group with a pending conf-change. Call ONLY
    /// when genuinely leaderless (`leaderId == 0`); pump-thread only. -1 (unknown
    /// group) is swallowed — a stale gid is a no-op, like the other per-id ops.
    pub fn campaignForce(self: *Manager, group_id: u64) void {
        _ = c.raft_manager_campaign_force(self.ptr, group_id);
    }

    /// Graceful pre-shutdown leadership handoff: if this node leads
    /// `group_id`, transfer leadership to the most caught-up follower
    /// (voter) so a rolling restart costs ~one heartbeat instead of a full
    /// election timeout. Returns the transferee node id when a transfer was
    /// initiated, or null if this node does not lead the group, is the sole
    /// voter, or the group is unknown. Drives `RawNode::transfer_leader`, so
    /// it must be called on the thread that owns the Manager (the pump).
    pub fn transferLeadershipAway(self: *Manager, group_id: u64) ?u64 {
        const rc = c.raft_manager_transfer_leadership_away(self.ptr, group_id);
        return if (rc > 0) @intCast(rc) else null;
    }

    /// Minimum `matched` index across all voters of `group_id` — the safe
    /// multi-node WAL compaction floor (every voter holds every entry at or
    /// below it). Only meaningful on the leader; returns `maxInt(u64)` on a
    /// follower / unknown group, so a `@min(..)` compaction floor is never
    /// *lowered* by it. Pump-thread only (reads the Manager).
    pub fn minMatchIndex(self: *const Manager, group_id: u64) u64 {
        return c.raft_manager_min_match_index(self.ptr, group_id);
    }

    /// This group's local raft last log index, on ANY replica (not leader-gated).
    /// `null` on an unknown group. The reconciler's learner→promote catch-up gate
    /// — see `raft_manager_last_index`.
    pub fn lastIndex(self: *const Manager, group_id: u64) ?u64 {
        var out: u64 = 0;
        return if (c.raft_manager_last_index(self.ptr, group_id, &out) == 0) out else null;
    }

    /// Single-change membership operation, proposed on the leader of `group_id`.
    pub const ConfChange = enum(u8) {
        /// Add `node` as a voter, or promote an existing learner to voter.
        add_voter = 0,
        /// Remove `node` from the group entirely.
        remove = 1,
        /// Add `node` as a learner, or DEMOTE an existing voter to learner.
        add_learner = 2,
    };

    /// Propose a membership change. Applies (and persists the new ConfState)
    /// when the entry commits, via `processReady`. Pump-thread only.
    /// `Error.NotLeader` off-leader; `Error.ConfChangeQuorumGuard` if a
    /// demote/remove would drop below 2 voters.
    ///
    /// `context` is replicated WITH the committed conf-change entry and surfaced
    /// on every replica via `setConfChangeObserver` as the change applies — the
    /// caller uses it to carry the changing node's transport address so the
    /// id→address map rides the log like the membership. Empty = no context.
    pub fn proposeConfChange(self: *Manager, group_id: u64, node: u64, change: ConfChange, context: []const u8) Error!void {
        const rc = c.raft_manager_propose_conf_change(
            self.ptr,
            group_id,
            node,
            @intFromEnum(change),
            if (context.len > 0) context.ptr else null,
            context.len,
        );
        return switch (rc) {
            0 => {},
            -1 => Error.UnknownGroup, // the bhs-3 id-drift signal — must NOT look like a transient ProposeFailed
            -2 => Error.NotLeader,
            -4 => Error.ConfChangeQuorumGuard,
            else => Error.ProposeFailed,
        };
    }

    /// The committed-conf-change observer: `fn(ctx, node_id, context_bytes)`.
    /// `context_bytes` aliases raft-owned memory valid only for the callback.
    pub const ConfChangeObserver = struct {
        ctx: *anyopaque,
        func: *const fn (ctx: *anyopaque, node_id: u64, context: []const u8) void,
    };

    /// Register a `ConfChangeObserver` fired once per committed conf-change as it
    /// applies on THIS node — on every replica — during `processReady`. The
    /// caller learns the changing node's address from the entry context the
    /// proposer attached. `obs` is CALLER-OWNED and must outlive the manager (a
    /// `Manager` is a by-value handle, so it can't safely store the observer
    /// inline). Call once at setup, before the pump runs.
    pub fn setConfChangeObserver(self: *Manager, obs: *const ConfChangeObserver) void {
        const Trampoline = struct {
            fn cb(ud: ?*anyopaque, node_id: u64, ctx_ptr: ?[*]const u8, ctx_len: usize) callconv(.c) void {
                const o: *const ConfChangeObserver = @ptrCast(@alignCast(ud.?));
                const bytes: []const u8 = if (ctx_ptr) |p| p[0..ctx_len] else &.{};
                o.func(o.ctx, node_id, bytes);
            }
        };
        c.raft_manager_set_conf_change_observer(self.ptr, @constCast(obs), Trampoline.cb);
    }

    pub const ConfStateView = struct { voters: []const u64, learners: []const u64 };

    /// Read `group_id`'s current membership into the caller's buffers; returns
    /// slices into them (truncated to the buffer if membership is larger), or
    /// null for an unknown group.
    pub fn confState(self: *const Manager, group_id: u64, voters_buf: []u64, learners_buf: []u64) ?ConfStateView {
        var vn: usize = 0;
        var ln: usize = 0;
        const rc = c.raft_manager_conf_state(
            self.ptr,
            group_id,
            voters_buf.ptr,
            voters_buf.len,
            &vn,
            learners_buf.ptr,
            learners_buf.len,
            &ln,
        );
        if (rc != 0) return null;
        // The FFI reports the TRUE count even past the cap so truncation is
        // detectable; surface it loudly rather than silently returning a short
        // (wrong) membership view that a reconciler would compare against desired.
        if (vn > voters_buf.len or ln > learners_buf.len)
            std.log.err("raft confState(group {d}): membership exceeds buffer ({d} voters/{d} learners vs {d}/{d}) — view INCOMPLETE", .{ group_id, vn, ln, voters_buf.len, learners_buf.len });
        return .{
            .voters = voters_buf[0..@min(vn, voters_buf.len)],
            .learners = learners_buf[0..@min(ln, learners_buf.len)],
        };
    }

    /// One peer voter's replication progress on the leader (auto-demote input).
    pub const VoterProgress = struct { id: u64, matched: u64, recent_active: bool };
    pub const VoterProgressView = struct {
        /// Peer voters (self excluded), truncated to the caller's buffers.
        peers: []const VoterProgress,
        /// The leader's own last log index; `peer.lag = leader_last - matched`.
        leader_last: u64,
    };

    /// Snapshot each peer voter's `matched` + `recent_active` on `group_id`'s
    /// leader, plus the leader's last log index, into the caller's parallel
    /// buffers (which must be the same length; the view's `peers` is truncated
    /// to that length). Null on a follower / unknown group. Pump-thread only.
    pub fn voterProgress(
        self: *const Manager,
        group_id: u64,
        ids_buf: []u64,
        matched_buf: []u64,
        active_buf: []u8,
        out: []VoterProgress,
    ) ?VoterProgressView {
        var n: usize = 0;
        var leader_last: u64 = 0;
        const cap = @min(@min(ids_buf.len, matched_buf.len), @min(active_buf.len, out.len));
        const rc = c.raft_manager_voter_progress(
            self.ptr,
            group_id,
            ids_buf.ptr,
            matched_buf.ptr,
            active_buf.ptr,
            cap,
            &n,
            &leader_last,
        );
        if (rc != 0) return null;
        // Truncation must not be silent: an omitted live voter is invisible to the
        // auto-demote policy. The FFI reports the true count past the cap so log it.
        if (n > cap)
            std.log.err("raft voterProgress(group {d}): {d} peer voters exceed buffer cap {d} — view INCOMPLETE (a live voter is hidden from the policy)", .{ group_id, n, cap });
        const count = @min(n, cap);
        for (0..count) |i| out[i] = .{ .id = ids_buf[i], .matched = matched_buf[i], .recent_active = active_buf[i] != 0 };
        return .{ .peers = out[0..count], .leader_last = leader_last };
    }

    /// Peer ids whose leader-side Progress is in `ProgressState::Snapshot` — the
    /// trigger for rove's out-of-band catch-up (see
    /// `raft_manager_snapshot_pending_peers`). Writes into `ids_buf`; returns the
    /// populated prefix, or null if this node is not the leader / unknown group.
    pub fn snapshotPendingPeers(self: *const Manager, group_id: u64, ids_buf: []u64) ?[]u64 {
        var n: usize = 0;
        const rc = c.raft_manager_snapshot_pending_peers(self.ptr, group_id, ids_buf.ptr, ids_buf.len, &n);
        if (rc != 0) return null;
        if (n > ids_buf.len)
            std.log.err("raft snapshotPendingPeers(group {d}): {d} peers exceed buffer cap {d} — list INCOMPLETE", .{ group_id, n, ids_buf.len });
        return ids_buf[0..@min(n, ids_buf.len)];
    }

    /// The term of the log entry at `index` on `group_id`, or `null` when no term
    /// is resolvable — the group is unknown, or `index` is compacted away / beyond
    /// the log. The leader reports `term(applied)` so a returning learner's
    /// promote-back baseline matches the leader's log. `null` is DISTINCT from a
    /// genuine term of 0 (the genesis index): the old u64-only form collapsed
    /// "unknown group" into a fake 0 that a caller could stamp into a baseline.
    pub fn logTerm(self: *const Manager, group_id: u64, index: u64) ?u64 {
        var out: u64 = 0;
        return if (c.raft_manager_log_term(self.ptr, group_id, index, &out) == 0) out else null;
    }

    pub const LogEntry = struct { term: u64, data: []const u8 };

    /// Read the raft LOG entry at `index` on `group_id` into `buf` (diagnostics —
    /// the replicated log content, distinct from the store). Returns `null` on
    /// unknown group, no entry at `index` (compacted / beyond the log), or `buf`
    /// too small. `data` slices into `buf` (may be empty for a no-op/conf-change).
    pub fn logEntry(self: *const Manager, group_id: u64, index: u64, buf: []u8) ?LogEntry {
        var term: u64 = 0;
        var len: usize = 0;
        const rc = c.raft_manager_log_entry(self.ptr, group_id, index, &term, buf.ptr, buf.len, &len);
        if (rc != 0) return null;
        return .{ .term = term, .data = buf[0..len] };
    }

    /// Install a data-free snapshot baseline at {index, term} into a LOCAL group
    /// (conf_change promote-back). The node must be a below-floor learner; the
    /// KV state for `index` must already be loaded out-of-band (the move
    /// bundle). Fast-forwards the raft log baseline so the leader can replicate
    /// the tail and the node can be promoted back. Pump-thread only.
    /// `Error.NotLeader` if this node leads the group (a leader can't restore to
    /// itself); `Error.SnapshotStale` if `index` is not ahead of committed.
    ///
    /// Membership (Phase 2 — membership SSOT): `voters`/`learners` non-null make
    /// the baseline carry the SUPPLIED membership (the source leader's ConfState),
    /// so a joining node learns its real membership from the baseline instead of a
    /// static env voter set. Null `voters` keeps the group's CURRENT membership
    /// (the membership-neutral promote-back, unchanged). Passed as primitive u64
    /// slices, NOT a ConfState struct (cross-`@cImport` structs don't unify). The
    /// supplied membership MUST contain this node or raft's `restore` discards the
    /// snapshot — `Error.SelfNotInConfState`.
    pub fn applyLocalSnapshot(
        self: *Manager,
        group_id: u64,
        index: u64,
        term: u64,
        voters: ?[]const u64,
        learners: ?[]const u64,
    ) Error!void {
        // Second gate (the FFI engine is the first): a baseline must be a real
        // {index>0, term>0} pair. A {0,_} or {n,0} baseline is meaningless and a
        // term-0 baseline crashes raft's restore — reject loudly here.
        if (index == 0 or term == 0) return Error.InvalidBaseline;
        const v_ptr: ?[*]const u64 = if (voters) |v| v.ptr else null;
        const v_len: usize = if (voters) |v| v.len else 0;
        const l_ptr: ?[*]const u64 = if (learners) |l| l.ptr else null;
        const l_len: usize = if (learners) |l| l.len else 0;
        const rc = c.raft_manager_apply_local_snapshot(self.ptr, group_id, index, term, v_ptr, v_len, l_ptr, l_len);
        return switch (rc) {
            0 => {},
            -1 => Error.UnknownGroup,
            -2 => Error.NotLeader,
            -3 => Error.SnapshotStale,
            -5 => Error.InvalidBaseline, // engine rejected a term-0 baseline
            -6 => Error.SelfNotInConfState, // supplied ConfState omits this node
            else => Error.ProposeFailed,
        };
    }

    /// Propose an entry to `group_id`. Returns when the entry is
    /// in raft-rs's pending list, NOT when it's applied —
    /// application happens via `processReady` after the entry
    /// commits.
    pub fn propose(self: *Manager, group_id: u64, data: []const u8) Error!void {
        const rc = c.raft_manager_propose(self.ptr, group_id, data.ptr, data.len);
        if (rc == -2) return Error.NotLeader;
        if (rc != 0) return Error.ProposeFailed;
    }

    /// Drive the per-group election/heartbeat/snapshot timers
    /// once. In a real deployment, call this periodically (every
    /// ~10ms is typical for raft-rs) so leader-election + log
    /// replication timeouts fire on schedule.
    pub fn tickAll(self: *Manager) void {
        _ = c.raft_manager_tick_all(self.ptr);
    }

    /// Tick exactly one group. Used by callers that implement
    /// hibernation policy at their layer (raft-rs doesn't have a
    /// hibernate concept; TiKV's raftstore is the design template).
    /// Skipping a hibernated group's tick lets the per-cycle cost
    /// scale with the active-group count, not total-group count.
    /// `error.UnknownGroup` if `group_id` isn't registered.
    pub fn tick(self: *Manager, group_id: u64) Error!void {
        if (c.raft_manager_tick(self.ptr, group_id) != 0) return Error.UnknownGroup;
    }

    /// Batched per-group tick. Ticks every group id in `group_ids`
    /// that's currently live; skips unknown ids silently. One FFI
    /// call regardless of length, so the per-tenant overhead the
    /// single `tick` shape has at large active sets is eliminated.
    /// Returns the number of groups actually ticked.
    pub fn tickGroups(self: *Manager, group_ids: []const u64) usize {
        return c.raft_manager_tick_groups(self.ptr, group_ids.ptr, group_ids.len);
    }

    /// Identify groups with pending applies. Returns a slice into
    /// the caller's buffer; the buffer must be sized to fit at
    /// least as many group IDs as the manager has groups (use
    /// `groupCount`).
    pub fn pollReady(self: *Manager, buf: []u64) []u64 {
        const n = c.raft_manager_poll_ready(self.ptr, buf.ptr, buf.len);
        return buf[0..n];
    }

    /// Drive `group_id`'s pending applies through `callback`.
    /// Called once per applied entry. `userdata` is passed
    /// through verbatim. Returns success (no-op) on an unknown
    /// group id — the ready channel is allowed to surface ids for
    /// groups that have since been destroyed.
    pub fn processReady(self: *Manager, group_id: u64, callback: ApplyCb, userdata: ?*anyopaque) Error!void {
        if (c.raft_manager_process_ready(self.ptr, group_id, callback, userdata) != 0)
            return Error.ProcessReadyFailed;
    }

    /// Acknowledge that every ready `processReady` produced for this
    /// group so far is durable (the caller's WAL fsync covers it).
    /// Releases the stashed persistence-asserting messages (append
    /// acks / vote responses) to the outbox and advances raft's
    /// persist watermark — the point at which THIS node's entries
    /// count toward the commit quorum. Call only after an fsync that
    /// covers the corresponding `processReady` appends, then poll
    /// readiness again: a persist ack commonly unlocks a commit
    /// advance with committed entries to apply (the group re-enters
    /// the ready channel). Unknown group / nothing pending = no-op.
    pub fn onPersist(self: *Manager, group_id: u64) void {
        _ = c.raft_manager_on_persist(self.ptr, group_id);
    }

    /// Release a group back to IDLE after the caller has drained
    /// its pending work (`processReady` + `takeMessages`). If new
    /// work landed during the round, the slot is re-notified so
    /// the next `pollReady` returns it. Unknown group ids are
    /// success-no-op (stale ids from the ready channel can land
    /// here after destroyGroup).
    ///
    /// Pair-with-pollReady invariant: every group id returned by
    /// `pollReady` must be `release`d before the next `pollReady`
    /// can see it again. The pump loop's natural shape — drain,
    /// process each, release each — satisfies this.
    pub fn release(self: *Manager, group_id: u64) void {
        _ = c.raft_manager_release(self.ptr, group_id);
    }

    pub fn isLeader(self: *const Manager, group_id: u64) bool {
        return c.raft_manager_is_leader(self.ptr, group_id);
    }

    /// The raft id this group currently believes leads it, or 0 when unknown
    /// (mid-election / freshly formed / no recent leader contact). On a
    /// follower this is the leader it follows; on the leader, its own id. Lets
    /// a not-leader node answer "who should the caller redirect to" rather than
    /// only "am I the leader" (`isLeader`). 0 for an unknown group id.
    pub fn leaderId(self: *const Manager, group_id: u64) u64 {
        return c.raft_manager_leader_id(self.ptr, group_id);
    }

    pub fn groupCount(self: *const Manager) usize {
        return c.raft_manager_group_count(self.ptr);
    }

    /// Drain the group's outbox of outbound raft messages. `cb`
    /// fires once per message in FIFO order; the bytes handed to
    /// the callback live only until the callback returns (Rust
    /// frees them after). The typical pattern: in the callback,
    /// route `(to, bytes)` into a network sim or transport layer,
    /// which will eventually deliver them to the recipient's
    /// `step`.
    pub fn takeMessages(
        self: *Manager,
        group_id: u64,
        cb: MessageCb,
        userdata: ?*anyopaque,
    ) Error!void {
        if (c.raft_manager_take_messages(self.ptr, group_id, cb, userdata) != 0)
            return Error.TakeMessagesFailed;
    }

    /// Deliver an inbound raft message produced by a peer's
    /// `takeMessages`. `msg_bytes` is the protobuf-serialized
    /// `eraftpb::Message` — opaque to Zig; raft-rs deserializes
    /// on the Rust side.
    pub fn step(self: *Manager, group_id: u64, msg_bytes: []const u8) Error!void {
        const rc = c.raft_manager_step(self.ptr, group_id, msg_bytes.ptr, msg_bytes.len);
        return switch (rc) {
            0 => {},
            -1 => Error.UnknownGroup, // distinct from a step rejection — id drift / post-destroy race
            -2 => Error.StepDecodeFailed,
            else => Error.StepFailed,
        };
    }

    /// `step`, fenced by the migration epoch. `sender_epoch` is the
    /// fence epoch the producing group was stamped with (read at the
    /// source via `groupEpoch`). If it is strictly older than this
    /// group's current epoch the message belongs to a pre-migration
    /// incarnation and is dropped with `Error.StepFenced` — it never
    /// reaches raft, so its (reset) term cannot perturb the fresh
    /// group. Non-migrated groups stamp 0 on both ends, so the fence
    /// is a no-op there.
    pub fn stepFenced(
        self: *Manager,
        group_id: u64,
        sender_epoch: u64,
        msg_bytes: []const u8,
    ) Error!void {
        const rc = c.raft_manager_step_fenced(self.ptr, group_id, sender_epoch, msg_bytes.ptr, msg_bytes.len);
        return switch (rc) {
            0 => {},
            -1 => Error.UnknownGroup,
            -2 => Error.StepDecodeFailed,
            -4 => Error.StepFenced,
            else => Error.StepFailed,
        };
    }

    /// Batch-step many inbound messages in one FFI call. Returns
    /// the count of successful steps; unknown groups, decode
    /// failures, and step-rejected messages are silently skipped
    /// (mirrors `tickGroups`' skip-bad semantics).
    ///
    /// Use this on the receive side of a coalesced multi-raft
    /// transport — one envelope carries N groups' messages to
    /// the same node, and one `stepBatch` call delivers them all
    /// without N round-trips through the Zig↔C↔Rust FFI boundary.
    /// For per-message error reporting use single-msg `step`.
    pub fn stepBatch(self: *Manager, entries: []const StepBatchEntry) usize {
        if (entries.len == 0) return 0;
        return c.raft_manager_step_batch(self.ptr, @ptrCast(entries.ptr), entries.len);
    }
};

/// One entry in a `stepBatch` call. Layout matches the C ABI
/// `RaftStepBatchEntry` exactly (extern struct guarantees that),
/// so a `[]const StepBatchEntry` can be passed straight through
/// to the FFI with no copy.
pub const StepBatchEntry = extern struct {
    group_id: u64,
    /// Sender's migration fence epoch (see `stepFenced`). Defaults to
    /// 0 — the engine's "never-migrated, fence-is-a-no-op" convention,
    /// matching `createGroup`'s epoch-0 default — so the common
    /// non-migrating receive path needn't spell it out. The default is
    /// a Zig-source convenience only; it does not affect the C ABI
    /// layout, which matches `RaftStepBatchEntry` field-for-field.
    epoch: u64 = 0,
    msg_ptr: [*]const u8,
    msg_len: usize,
};

// ── Tests ──────────────────────────────────────────────────────────────────

const testing = std.testing;

const MsgCollector = struct {
    var bytes_buf: [4096]u8 = undefined;
    var bytes_len: usize = 0;
    var last_to: u64 = 0;
    var count: usize = 0;

    fn reset() void {
        bytes_len = 0;
        last_to = 0;
        count = 0;
    }

    fn cb(
        ud: ?*anyopaque,
        to: u64,
        msg_bytes: [*c]const u8,
        msg_len: usize,
    ) callconv(.c) void {
        _ = ud;
        last_to = to;
        count += 1;
        const n = @min(msg_len, bytes_buf.len);
        @memcpy(bytes_buf[0..n], msg_bytes[0..n]);
        bytes_len = n;
    }

    fn snapshot() []const u8 {
        return bytes_buf[0..bytes_len];
    }
};

/// Apply callback that ignores every committed entry — for tests that
/// only care about message flow / fencing, not state-machine output.
fn noopApply(
    ud: ?*anyopaque,
    gid: u64,
    idx: u64,
    term: u64,
    data: [*c]const u8,
    len: usize,
) callconv(.c) void {
    _ = ud;
    _ = gid;
    _ = idx;
    _ = term;
    _ = data;
    _ = len;
}

/// Spin up a throwaway leader (group 1, node 1, cluster {1,2}),
/// campaign it, and capture the real `RequestVote` bytes it emits to
/// peer node 2 into `MsgCollector`'s static buffer. Returns the
/// captured slice — a genuine eraftpb message, valid until the next
/// processReady + immediate persist-ack — the sync-storage test
/// shorthand (MemStorage is durable at append, so acking right after
/// the append IS the faithful ordering; WAL-backed tests ack after
/// `wal.flush()` instead).
fn processReadyAndPersist(mgr: *Manager, g: u64, callback: ApplyCb, userdata: ?*anyopaque) !void {
    try mgr.processReady(g, callback, userdata);
    mgr.onPersist(g);
}

/// `MsgCollector.reset()`. Used by the fence tests so they exercise
/// the decode path with a real message, not garbage.
fn captureRealVote() ![]const u8 {
    var mgr = try Manager.init();
    defer mgr.deinit();
    const storage = try MemStorage.init(testing.allocator, &.{ 1, 2 });
    try mgr.createGroup(1, 1, storage_vtable, storage);
    try mgr.campaign(1);
    var buf: [16]u64 = undefined;
    mgr.tickAll();
    const ready = mgr.pollReady(&buf);
    try testing.expect(ready.len >= 1);
    for (ready) |g| {
        try mgr.processReady(g, noopApply, null);
        // MemStorage is durable at append; ack immediately so the vote
        // request (a persistence-asserting message) reaches the outbox.
        mgr.onPersist(g);
    }
    MsgCollector.reset();
    try mgr.takeMessages(1, MsgCollector.cb, null);
    try testing.expectEqual(@as(u64, 2), MsgCollector.last_to);
    try testing.expect(MsgCollector.snapshot().len > 0);
    return MsgCollector.snapshot();
}

test "Manager: take_messages drains the outbox after a campaign (multi-node sanity)" {
    // Two nodes (groups 1 and 2 internally, but they're really two
    // logical raft nodes with peer ids 1, 2). Node 1 thinks the
    // cluster is {1, 2}; node 2 thinks the same. Node 1 campaigns;
    // raft-rs queues a RequestVote message destined for node 2.
    // We drain it and assert.

    var mgr = try Manager.init();
    defer mgr.deinit();

    const storage = try MemStorage.init(testing.allocator, &.{ 1, 2 });
    try mgr.createGroup(1, 1, storage_vtable, storage);
    try mgr.campaign(1);

    // The campaign synchronously stages a RequestVote message;
    // processReady extracts it into the outbox. One tick + one
    // ready pump is enough for the message to materialize.
    var buf: [16]u64 = undefined;
    mgr.tickAll();
    const ready = mgr.pollReady(&buf);
    try testing.expect(ready.len >= 1);
    for (ready) |g| try processReadyAndPersist(&mgr, g, struct {
        fn cb(
            ud: ?*anyopaque,
            gid: u64,
            idx: u64,
            term: u64,
            data: [*c]const u8,
            len: usize,
        ) callconv(.c) void {
            _ = ud;
            _ = gid;
            _ = idx;
            _ = term;
            _ = data;
            _ = len;
        }
    }.cb, null);

    MsgCollector.reset();
    try mgr.takeMessages(1, MsgCollector.cb, null);
    // RequestVote goes to node 2.
    try testing.expectEqual(@as(u64, 2), MsgCollector.last_to);
    try testing.expect(MsgCollector.count >= 1);
    try testing.expect(MsgCollector.snapshot().len > 0);
}

test "Manager: step rejects garbage bytes with StepDecodeFailed" {
    var mgr = try Manager.init();
    defer mgr.deinit();
    const storage = try MemStorage.init(testing.allocator, &.{1});
    try mgr.createGroup(1, 1, storage_vtable, storage);

    const garbage = [_]u8{ 0xff, 0xff, 0xff, 0xff };
    try testing.expectError(Error.StepDecodeFailed, mgr.step(1, &garbage));
}

test "Manager: epoch defaults to 0, is readable, and unknown groups read 0" {
    var mgr = try Manager.init();
    defer mgr.deinit();
    const storage = try MemStorage.init(testing.allocator, &.{1});
    try mgr.createGroup(1, 1, storage_vtable, storage);
    try testing.expectEqual(@as(u64, 0), mgr.groupEpoch(1));
    // Unknown group reads 0 too — same value as a real epoch-0 group.
    try testing.expectEqual(@as(u64, 0), mgr.groupEpoch(999));
}

test "Manager: createGroupEpoch records the birth epoch" {
    var mgr = try Manager.init();
    defer mgr.deinit();
    const storage = try MemStorage.init(testing.allocator, &.{1});
    try mgr.createGroupEpoch(1, 1, 7, storage_vtable, storage, null);
    try testing.expectEqual(@as(u64, 7), mgr.groupEpoch(1));
}

test "Manager: createGroupEpoch accepts a tuned GroupConfig (pre_vote)" {
    var mgr = try Manager.init();
    defer mgr.deinit();
    const storage = try MemStorage.init(testing.allocator, &.{1});
    var cfg = defaultGroupConfig();
    cfg.pre_vote = true;
    cfg.check_quorum = true;
    try mgr.createGroupEpoch(1, 1, 0, storage_vtable, storage, &cfg);
    // A configured group is otherwise identical — campaign still drives
    // it to leadership on a single-node voter set.
    try mgr.campaign(1);
    try testing.expect(mgr.isLeader(1));
}

test "Manager: createGroupEpoch rejects an invalid GroupConfig" {
    var mgr = try Manager.init();
    defer mgr.deinit();
    const storage = try MemStorage.init(testing.allocator, &.{1});
    var cfg = defaultGroupConfig();
    // election_tick must be > heartbeat_tick; this fails raft's validate(),
    // which surfaces as -3 → CreateGroupFailed (storage is freed by the
    // destroy vtable on the failed-create path).
    cfg.election_tick = 1;
    cfg.heartbeat_tick = 3;
    try testing.expectError(Error.CreateGroupFailed, mgr.createGroupEpoch(1, 1, 0, storage_vtable, storage, &cfg));
}

test "Manager: setEpoch is monotonic — raise allowed, regression rejected" {
    var mgr = try Manager.init();
    defer mgr.deinit();
    const storage = try MemStorage.init(testing.allocator, &.{1});
    try mgr.createGroupEpoch(1, 1, 5, storage_vtable, storage, null);

    try mgr.setEpoch(1, 5); // equal is fine
    try mgr.setEpoch(1, 8); // raise is fine
    try testing.expectEqual(@as(u64, 8), mgr.groupEpoch(1));

    // Lowering re-admits already-fenced traffic — rejected, no change.
    try testing.expectError(Error.EpochRegression, mgr.setEpoch(1, 7));
    try testing.expectEqual(@as(u64, 8), mgr.groupEpoch(1));

    try testing.expectError(Error.UnknownGroup, mgr.setEpoch(999, 1));
}

test "Manager: stepFenced drops stale-epoch messages before decode" {
    var mgr = try Manager.init();
    defer mgr.deinit();
    const storage = try MemStorage.init(testing.allocator, &.{1});
    try mgr.createGroupEpoch(1, 1, 5, storage_vtable, storage, null);

    // Garbage bytes: a stale epoch is fenced *before* the protobuf
    // decode runs, so we get StepFenced — not StepDecodeFailed. That
    // ordering is the point: a flood of stale traffic costs one compare
    // per message, never a decode.
    const garbage = [_]u8{ 0xff, 0xff, 0xff, 0xff };
    try testing.expectError(Error.StepFenced, mgr.stepFenced(1, 4, &garbage));
    // At the current epoch (or newer) the fence passes and the decode
    // runs — and rejects the garbage.
    try testing.expectError(Error.StepDecodeFailed, mgr.stepFenced(1, 5, &garbage));
    try testing.expectError(Error.StepDecodeFailed, mgr.stepFenced(1, 6, &garbage));
}

test "Manager: migration fence — fresh incarnation rejects old-epoch traffic, admits current" {
    // A real RequestVote from the group's previous home (node 1).
    const vote = try captureRealVote();

    // The group has migrated: node 2 now hosts the same group id at
    // incarnation epoch 5 (the control plane bumped the epoch for the
    // move). This is the scenario term-based fencing can't handle — the
    // fresh group's term reset, so the stale vote could carry a higher
    // term and depose its leader if it weren't fenced.
    var dst = try Manager.init();
    defer dst.deinit();
    const dst_storage = try MemStorage.init(testing.allocator, &.{ 1, 2 });
    try dst.createGroupEpoch(1, 2, 5, storage_vtable, dst_storage, null);

    // Stamped with the previous incarnation's epoch (4) → fenced.
    try testing.expectError(Error.StepFenced, dst.stepFenced(1, 4, vote));
    // The same message at the current epoch is admitted.
    try dst.stepFenced(1, 5, vote);
}

test "Manager: stepBatch fences stale-epoch entries (skipped, not stepped)" {
    const vote = try captureRealVote();

    var dst = try Manager.init();
    defer dst.deinit();
    const dst_storage = try MemStorage.init(testing.allocator, &.{ 1, 2 });
    try dst.createGroupEpoch(1, 2, 5, storage_vtable, dst_storage, null);

    // The fence applies on the coalesced fast path too: a stale entry
    // is skipped (not counted as a successful step).
    var stale = [_]StepBatchEntry{.{
        .group_id = 1,
        .epoch = 4,
        .msg_ptr = vote.ptr,
        .msg_len = vote.len,
    }};
    try testing.expectEqual(@as(usize, 0), dst.stepBatch(&stale));

    // A current-epoch entry steps successfully.
    var current = [_]StepBatchEntry{.{
        .group_id = 1,
        .epoch = 5,
        .msg_ptr = vote.ptr,
        .msg_len = vote.len,
    }};
    try testing.expectEqual(@as(usize, 1), dst.stepBatch(&current));
}

// ── HardState.commit persistence regression (BUG-hardstate-commit-not-persisted) ─
//
// The durable `HardState.commit` must track the advancing commit
// index. Under the original sync flow that meant persisting the
// commit the `LightReady` carried; under the async-append flow a
// commit advance (post `onPersist`) surfaces as a changed `hs()` in a
// SUBSEQUENT ready and rides the normal hard-state persist — one
// fsync behind the advance, which is safe (raft only needs the
// durable commit to stay ≥ the compaction point). If it were dropped
// entirely, the durable commit would stay at its pre-advance value (0
// for a fresh single-node leader): a silent durability gap without
// compaction (re-election re-derives the commit) and a hard panic
// with it (`RawNode::new` asserts `first_index-1 <= hs.commit`).
// These two tests pin both: the recovered commit must equal the last
// committed index, and a compacted+recovered group must re-open.

/// Apply callback that records the highest applied index it sees.
const HsReproApply = struct {
    var last: u64 = 0;
    fn reset() void {
        last = 0;
    }
    fn cb(ud: ?*anyopaque, gid: u64, idx: u64, term: u64, data: [*c]const u8, len: usize) callconv(.c) void {
        _ = ud;
        _ = gid;
        _ = term;
        _ = data;
        _ = len;
        last = idx;
    }
};

/// Pump the manager until quiescent: tick, drain the ready channel,
/// process each group, fsync the shared WAL, ACK persistence (the
/// async-append flow — commit only advances after the ack), then
/// release. Mirrors a host's pump loop.
fn hsReproPump(mgr: *Manager, wal: *SharedWal) !void {
    var buf: [8]u64 = undefined;
    var i: u32 = 0;
    while (i < 40) : (i += 1) {
        mgr.tickAll();
        const r = mgr.pollReady(&buf);
        for (r) |g| try mgr.processReady(g, HsReproApply.cb, null);
        try wal.flush();
        for (r) |g| {
            mgr.onPersist(g);
            mgr.release(g);
        }
    }
}

const CcObs = struct {
    var node_id: u64 = 0;
    var ctx_buf: [64]u8 = undefined;
    var ctx_len: usize = 0;
    var fired: u32 = 0;
    fn reset() void {
        node_id = 0;
        ctx_len = 0;
        fired = 0;
    }
    fn observe(_: *anyopaque, nid: u64, context: []const u8) void {
        node_id = nid;
        ctx_len = @min(context.len, ctx_buf.len);
        @memcpy(ctx_buf[0..ctx_len], context[0..ctx_len]);
        fired += 1;
    }
};

test "Manager: conf-change observer fires on apply with the entry context" {
    // The address-rides-the-log contract: a conf-change proposed with a context
    // surfaces that context — on the applying node — via the observer, keyed by
    // the change's node id. (In rove the context is the joining node's transport
    // address, so every replica learns id→addr as the membership change applies.)
    const a = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try tmp.dir.realpathAlloc(a, ".");
    defer a.free(path);
    const wal_path = try std.fmt.allocPrint(a, "{s}/wal", .{path});
    defer a.free(wal_path);

    const wal = try SharedWal.init(a, wal_path);
    defer wal.deinit();
    const gfs = try GroupedFileStorage.init(a, &.{1}, wal, 1);
    var mgr = try Manager.init();
    defer mgr.deinit();

    CcObs.reset();
    var dummy: u8 = 0;
    const obs = Manager.ConfChangeObserver{ .ctx = &dummy, .func = CcObs.observe };
    mgr.setConfChangeObserver(&obs);

    try mgr.createGroup(1, 1, grouped_file_storage_vtable, gfs);
    try mgr.campaign(1);
    HsReproApply.reset();
    try hsReproPump(&mgr, wal); // elect
    try testing.expect(mgr.isLeader(1));

    // Add node 2 as a learner, carrying its address as the conf-change context.
    try mgr.proposeConfChange(1, 2, .add_learner, "10.0.0.2:9001");
    try hsReproPump(&mgr, wal); // commit + apply → observer fires on this node

    try testing.expect(CcObs.fired >= 1);
    try testing.expectEqual(@as(u64, 2), CcObs.node_id);
    try testing.expectEqualStrings("10.0.0.2:9001", CcObs.ctx_buf[0..CcObs.ctx_len]);
}

test "async-append: commit waits for the persist ack (no quorum from volatile entries)" {
    // The persist-before-quorum property this module's async-append
    // flow exists for: a proposed entry is appended (buffered) by
    // `processReady`, but it must NOT commit — and the apply callback
    // must NOT fire — until `onPersist` acks that the WAL fsync covers
    // it. Under the old sync flow (`advance` inside `processReady`)
    // the single voter's own un-fsynced append satisfied quorum
    // instantly, so an entry could commit + apply entirely ahead of
    // the fsync; on a multi-node leader the same early self-ack let
    // commit be reached with one durable copy fewer than quorum.
    const a = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try tmp.dir.realpathAlloc(a, ".");
    defer a.free(path);
    const wal_path = try std.fmt.allocPrint(a, "{s}/wal", .{path});
    defer a.free(wal_path);

    const wal = try SharedWal.init(a, wal_path);
    defer wal.deinit();
    const gfs = try GroupedFileStorage.init(a, &.{1}, wal, 1);
    var mgr = try Manager.init();
    defer mgr.deinit();
    try mgr.createGroup(1, 1, grouped_file_storage_vtable, gfs);
    try mgr.campaign(1);
    HsReproApply.reset();
    try hsReproPump(&mgr, wal); // elect (with proper flush+ack)
    try testing.expect(mgr.isLeader(1));
    const applied_after_elect = HsReproApply.last;

    // Propose, then pump WITHOUT acking persistence (we even fsync —
    // the gate is the ACK, not the physical flush). The entry must
    // not commit/apply.
    try mgr.propose(1, "gated");
    var buf: [8]u64 = undefined;
    var processed: [8]u64 = undefined;
    var nproc: usize = 0;
    var i: u32 = 0;
    while (i < 20) : (i += 1) {
        mgr.tickAll();
        const r = mgr.pollReady(&buf);
        for (r) |g| {
            try mgr.processReady(g, HsReproApply.cb, null);
            if (nproc < processed.len) {
                processed[nproc] = g;
                nproc += 1;
            }
            mgr.release(g);
        }
        try wal.flush();
    }
    try testing.expectEqual(applied_after_elect, HsReproApply.last); // NOT applied

    // Ack persistence → the commit unlocks and the next ready applies it.
    for (processed[0..nproc]) |g| mgr.onPersist(g);
    try hsReproPump(&mgr, wal);
    try testing.expect(HsReproApply.last > applied_after_elect); // applied now
}

test "HardState.commit: recovered commit equals last committed index (no compaction)" {
    const a = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try tmp.dir.realpathAlloc(a, ".");
    defer a.free(path);
    const wal_path = try std.fmt.allocPrint(a, "{s}/wal", .{path});
    defer a.free(wal_path);

    var last_committed: u64 = 0;
    {
        const wal = try SharedWal.init(a, wal_path);
        defer wal.deinit();
        const gfs = try GroupedFileStorage.init(a, &.{1}, wal, 1);
        var mgr = try Manager.init();
        defer mgr.deinit();
        try mgr.createGroup(1, 1, grouped_file_storage_vtable, gfs);
        try mgr.campaign(1);
        HsReproApply.reset();
        try hsReproPump(&mgr, wal);
        var n: u32 = 0;
        while (n < 5) : (n += 1) {
            try mgr.propose(1, "x");
            try hsReproPump(&mgr, wal);
        }
        try wal.flush();
        last_committed = gfs.mem.lastIndex();
    }

    const wal = try SharedWal.open(a, wal_path);
    defer wal.deinit();
    const gfs2 = try GroupedFileStorage.initRecover(a, &.{1}, wal, 1);
    defer gfs2.deinit();
    // The masked bug: hs_commit recovered as 0. After the fix it tracks
    // the durable commit, which on a single-node leader is the last
    // committed index.
    try testing.expect(last_committed > 0);
    try testing.expectEqual(last_committed, gfs2.mem.hs_commit);
}

test "HardState.commit: compact-then-recover round-trips and re-opens" {
    const a = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try tmp.dir.realpathAlloc(a, ".");
    defer a.free(path);
    const wal_path = try std.fmt.allocPrint(a, "{s}/wal", .{path});
    defer a.free(wal_path);

    {
        const wal = try SharedWal.init(a, wal_path);
        defer wal.deinit();
        const gfs = try GroupedFileStorage.init(a, &.{1}, wal, 1);
        var mgr = try Manager.init();
        defer mgr.deinit();
        try mgr.createGroup(1, 1, grouped_file_storage_vtable, gfs);
        try mgr.campaign(1);
        HsReproApply.reset();
        try hsReproPump(&mgr, wal);
        var n: u32 = 0;
        while (n < 5) : (n += 1) {
            try mgr.propose(1, "x");
            try hsReproPump(&mgr, wal);
        }
        try wal.flush();
        try gfs.compact(2); // application checkpointed through index 2
        try wal.flush();
    }

    const wal = try SharedWal.open(a, wal_path);
    defer wal.deinit();
    const gfs2 = try GroupedFileStorage.initRecover(a, &.{1}, wal, 1);
    // first_index is now 2; before the fix hs_commit recovered as 0 and
    // `createGroup` -> `RawNode::new` panicked "hs.commit 0 out of range".
    try testing.expect(gfs2.mem.entries.items[0].index == 2);
    try testing.expect(gfs2.mem.hs_commit >= gfs2.mem.entries.items[0].index);
    var mgr = try Manager.init();
    defer mgr.deinit();
    try mgr.createGroup(1, 1, grouped_file_storage_vtable, gfs2); // must not panic
    try testing.expect(mgr.groupCount() == 1);
}

test "recovery: a snapshot's ConfState is persisted (membership SSOT survives restart)" {
    // Instance #2 of the "recovery-critical state must be durable" class (the
    // genesis-ConfState fix is instance #1). A node that joins via an out-of-band
    // baseline (rove move / catch-up / reconciler bootstrap) learns the leader's
    // membership from the snapshot's ConfState; raft's `restore` installs it in
    // memory, but `apply_snapshot` records only the LOG baseline. process_ready
    // must ALSO persist the snapshot's ConfState (set_conf_state) — else the
    // membership is lost on restart: initRecover reseeds from the static voter set
    // and the group silently changes membership (the __auth__ fork class).
    const a = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try tmp.dir.realpathAlloc(a, ".");
    defer a.free(path);
    const wal_path = try std.fmt.allocPrint(a, "{s}/wal", .{path});
    defer a.free(wal_path);

    {
        const wal = try SharedWal.init(a, wal_path);
        defer wal.deinit();
        // Born {1,2}; node 1 is a FOLLOWER (never campaigns), so it can install a
        // baseline — a leader cannot restore a snapshot to itself.
        const gfs = try GroupedFileStorage.init(a, &.{ 1, 2 }, wal, 1);
        var mgr = try Manager.init();
        defer mgr.deinit();
        try mgr.createGroup(1, 1, grouped_file_storage_vtable, gfs);
        // Install a baseline whose ConfState ADDS node 3 → membership {1,2,3},
        // different from the born {1,2}. (Node 1 is in the new set, so restore
        // keeps the baseline rather than discarding it.)
        try mgr.applyLocalSnapshot(1, 5, 2, &.{ 1, 2, 3 }, null);
        HsReproApply.reset();
        try hsReproPump(&mgr, wal); // process_ready installs + persists the ConfState
        try wal.flush();
        // In-memory membership now reflects the baseline (proves set_conf_state ran).
        try testing.expectEqualSlices(u64, &.{ 1, 2, 3 }, gfs.mem.voters.items);
    }

    // Restart: seed initRecover with the BORN {1,2}, deliberately ≠ the baseline's
    // {1,2,3}. The baseline's ConfState must win (it was persisted), NOT the seed.
    // Without the fix this recovers {1,2} (the seed); with it, {1,2,3}.
    const wal = try SharedWal.open(a, wal_path);
    defer wal.deinit();
    const gfs2 = try GroupedFileStorage.initRecover(a, &.{ 1, 2 }, wal, 1);
    defer gfs2.deinit();
    try testing.expectEqualSlices(u64, &.{ 1, 2, 3 }, gfs2.mem.voters.items);
}

// ── Threaded group-creation repro (BUG-e2c4aea-process-ready-confchange-gpf) ──
//
// The bug report claims a threaded host SIGSEGVs in `confchange::restore`
// at `RawNode::new` on the first `createGroup` over a fresh WAL — and that
// the same lifecycle on the main thread is fine. This mirrors a host's
// pump thread: the full `createGroup` -> `campaign` -> `propose` -> pump
// cycle runs on a NON-main `std.Thread` over a fresh empty WAL. If the
// report's "threaded-only" trigger is real, this test faults. As of
// e2c4aea it passes, which means the crash is not reproduced by the
// threaded lifecycle alone — the trigger is more environment-specific
// than the report's framing. Kept as the in-tree anchor for any future
// repro and for the controlled A/B against `Cargo.lock`.

const ThreadReproApply = struct {
    fn cb(ud: ?*anyopaque, gid: u64, idx: u64, term: u64, data: [*c]const u8, len: usize) callconv(.c) void {
        _ = ud;
        _ = gid;
        _ = idx;
        _ = term;
        _ = data;
        _ = len;
    }
};

/// Pump until quiescent: tick, drain ready, process + release each group.
fn threadReproPump(mgr: *Manager, wal: *SharedWal) !void {
    var buf: [8]u64 = undefined;
    var i: u32 = 0;
    while (i < 40) : (i += 1) {
        mgr.tickAll();
        const r = mgr.pollReady(&buf);
        for (r) |g| try mgr.processReady(g, ThreadReproApply.cb, null);
        try wal.flush();
        for (r) |g| {
            mgr.onPersist(g);
            mgr.release(g);
        }
    }
}

/// The whole group lifecycle, run on a spawned thread. Errors are routed
/// back through `err_out` so the test thread can surface them; a SIGSEGV
/// (the reported failure mode) takes the process down regardless.
fn threadReproWorker(wal_path: []const u8, err_out: *?anyerror) void {
    threadReproBody(wal_path) catch |e| {
        err_out.* = e;
    };
}

fn threadReproBody(wal_path: []const u8) !void {
    const a = std.heap.page_allocator;
    const wal = try SharedWal.init(a, wal_path);
    defer wal.deinit();
    const gfs = try GroupedFileStorage.init(a, &.{1}, wal, 1);
    var mgr = try Manager.init();
    defer mgr.deinit();
    // The report's crash site: createGroup -> RawNode::new -> confchange::restore.
    try mgr.createGroup(1, 1, grouped_file_storage_vtable, gfs);
    try mgr.campaign(1);
    try threadReproPump(&mgr, wal);
    var n: u32 = 0;
    while (n < 5) : (n += 1) {
        try mgr.propose(1, "x");
        try threadReproPump(&mgr, wal);
    }
    try wal.flush();
}

test "threaded createGroup over fresh WAL on a spawned thread does not crash" {
    const a = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try tmp.dir.realpathAlloc(a, ".");
    defer a.free(path);
    const wal_path = try std.fmt.allocPrint(a, "{s}/wal", .{path});
    defer a.free(wal_path);

    var worker_err: ?anyerror = null;
    const t = try std.Thread.spawn(.{}, threadReproWorker, .{ wal_path, &worker_err });
    t.join();
    if (worker_err) |e| return e;
}
