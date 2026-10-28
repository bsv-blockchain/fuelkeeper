package store

import (
	"context"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
)

// Store is the fuel store contract. Every implementation must pass storetest.RunSuite.
//
// # Correctness model
//
// Each Row has exactly one State. Every mutation is a compare-and-set on the row's
// current state (and on ReservedBy == token for Release and Spend). Backends must
// never overwrite a row without re-asserting that predicate in the same atomic
// write. Rows that fail the predicate are silently skipped and reported through
// the returned count / applied set; they are never errors.
//
// Shards carry a lease (Owner, LeaseUntil) that lets one instance serve a shard's
// rows without contention. The lease is an optimisation only: two instances may
// race Reserve on the same row and the row CAS decides. Adoption never writes rows.
//
// A lease is free when Owner == "" or LeaseUntil <= now; it is held while
// now < LeaseUntil. Owner == "" if and only if LeaseUntil is the zero time. Only a
// successful AcquireShard by another holder, or ReleaseShard, clears Owner: an
// expired lease is adoptable but its Owner/RenewShard rights are untouched until
// someone actually acquires or releases it.
//
// # Idempotency
//
// Reserve applies to a row when state == available OR (state == reserved AND
// reserved_by == op.Token). A replay under the same token is therefore reported
// as applied without changing reserved_at / expires_at / updated_at. Callers
// retry Reserve on ErrUnavailable instead of compensating. A replay stays
// applicable after its expiry, right up until the reaper's Quarantine actually
// moves the row: expiry is Quarantine's job, not Reserve's.
//
// # Provenance
//
// ReservedBy, ReservedAt and ExpiresAt are set by Reserve; QuarantinedAt is set by
// Quarantine and by a non-terminal Spend; SpentHint is set by a non-terminal
// Spend. All five are then kept for provenance across every later transition that
// is not a return to available: both Spend modes, Quarantine, Resolve to spent
// and RetireShard's cascade all preserve them. Only Release, or Resolve to
// available, clears all five. UpdatedAt is stamped by every state change except a
// same-token Reserve replay (see "Idempotency").
//
// # Names
//
// pool, holder (AcquireShard, RenewShard, ReleaseShard, TryAcquireOrRenew,
// ReleaseLease) and lease names (TryAcquireOrRenew, ReleaseLease) must be
// non-empty, at most their documented byte length, valid UTF-8, and free of
// NUL bytes: ValidatePool, ValidateHolder and ValidateLeaseName enforce this
// uniformly so every backend rejects the same malformed name the same way,
// with ErrInvalidArgument — rather than a SQL backend surfacing a raw
// driver-level error for a TEXT column that cannot hold invalid UTF-8 or a
// NUL byte, or a JSON-backed backend silently corrupting the name (Go's JSON
// encoder replaces an invalid UTF-8 byte with the Unicode replacement
// character U+FFFD on a string field) instead of rejecting it up front.
//
// # Clocks
//
// Backends stamp created_at / reserved_at / quarantined_at / updated_at and lease
// expiry from their injected clock (time.Now by default). Callers pass now, before
// and until explicitly where a comparison is made, so a single clock policy holds
// across processes (see the design spec, "Restart / recovery, identity, clocks").
//
// All stored times have microsecond precision; backends truncate to the
// microsecond on write and callers compare with time.Time.Truncate(time.Microsecond).
//
// A non-zero time argument that is stored as data (AcquireShard's until,
// RenewShard's until, each Reserve op's ExpiresAt) must be strictly after the
// Unix epoch: ValidateTime rejects anything else with ErrInvalidArgument. The
// zero time itself is always accepted separately wherever it means "unset"
// (ReservedAt, ExpiresAt, QuarantinedAt and LeaseUntil before their field is
// ever set); it is never treated as a data value needing this check. This
// matters because the zero time and the exact Unix epoch instant round-trip
// through every backend's on-disk time encoding identically (both are stored
// as 0), so accepting an epoch-or-earlier value as real data would make it
// indistinguishable from "unset" once read back.
//
// ValidateTime applies only to times stored as data. Quarantine's before and
// ListQuarantined's before/after are comparison bounds, not stored data, and
// are never checked by ValidateTime: instead, an edge value that would be
// invalid as data (the zero time, the exact epoch instant, or earlier) simply
// matches nothing there, since every stored expires_at/quarantined_at is
// strictly after the epoch (see Quarantine, ListQuarantined).
//
// # Errors
//
// A returned count, applied set or row/shard result is meaningful only when
// err == nil; treat it as undefined otherwise. ErrUnavailable may wrap
// context.Canceled or context.DeadlineExceeded when the caller's own context
// ended the call early: check ctx.Err() before deciding whether to retry,
// since retrying a cancelled or expired caller context is never useful.
type Store interface {
	// Ping reports connectivity. It must not allocate or mutate.
	Ping(ctx context.Context) error
	// Capabilities is constant for the life of the store.
	Capabilities() Caps

	// PutShard inserts a shard and all its rows (state available). A shard's txid is
	// globally unique across pools: PutShard with an existing txid under a different
	// pool returns ErrShardExists. It is idempotent on (pool, txid): an identical
	// replay returns nil without touching existing rows, keeping Version 1 and the
	// original BEEF/status even if the replay carries different ones; a replay with a
	// different size, denomination or lock mode also returns ErrShardExists. Backends
	// without multi-record atomicity write rows first and the shard record last, so a
	// visible shard implies its rows exist. pool must be non-empty and BEEF must not
	// be empty (ErrInvalidArgument).
	PutShard(ctx context.Context, shard Shard, rows []Row) error
	// GetShard returns the full record including BEEF, or ErrNotFound. pool must be
	// non-empty (ErrInvalidArgument).
	GetShard(ctx context.Context, pool string, txid chainhash.Hash) (*Shard, error)
	// UpdateShard sets status (unmined, mined or retired) and, when beef != nil,
	// replaces BEEF; it bumps Version. It never touches the shard's lease (Owner,
	// LeaseUntil). Retired/invalid shards return ErrTerminalShard. Missing shard:
	// ErrNotFound. pool must be non-empty (ErrInvalidArgument).
	UpdateShard(ctx context.Context, pool string, txid chainhash.Hash, status ShardStatus, beef []byte) error
	// RetireShard marks the shard invalid, clears its lease, bumps Version, and moves
	// every non-terminal row (available, reserved, quarantined) to invalid, keeping
	// all provenance fields (ReservedBy, ReservedAt, ExpiresAt, QuarantinedAt,
	// SpentHint; see "Provenance"). It returns the number of rows this call moved to
	// invalid (concurrent calls together move each non-terminal row exactly once).
	// When the shard is already invalid and every row is terminal it returns 0 and
	// leaves Version/UpdatedAt unchanged. Backends without multi-record atomicity
	// cascade rows before marking the shard, so a retry repairs a partial retire; a
	// retry that repairs rows returns the number it repaired. A retired shard may
	// still be retired (retired -> invalid). Missing shard: ErrNotFound. pool must be
	// non-empty (ErrInvalidArgument).
	RetireShard(ctx context.Context, pool string, txid chainhash.Hash) (int, error)
	// AcquireShard atomically claims the oldest shard (by created_at, then txid
	// compared as its 32 internal bytes, not display hex) whose status is in statuses,
	// whose lease is free (Owner == "" or LeaseUntil <= now), and whose txid is not in
	// exclude. until must be strictly after now (ErrInvalidArgument otherwise). It sets
	// Owner = holder, LeaseUntil = until and returns the full record. (nil, nil) means
	// nothing was adoptable. The SQL backends (postgres, sqlite) also skip shards
	// with no available row; the others still return them, and the shard manager's
	// bounded exclusion list is all that keeps adoption away from drained shards there
	// (see ShardManager.retireIfSpentOut, "Residual").
	AcquireShard(ctx context.Context, pool, holder string, now, until time.Time, statuses []ShardStatus, exclude []chainhash.Hash) (*Shard, error)
	// RenewShard extends the lease if Owner == holder (even if it had expired: only a
	// successful AcquireShard by another holder, or ReleaseShard, clears Owner) and
	// returns the current status and version. until must be strictly after the Unix
	// epoch (see "Clocks") (ErrInvalidArgument otherwise). Otherwise ErrNotOwner;
	// missing: ErrNotFound. RenewShard is a heartbeat: it updates only lease_until
	// and never stamps UpdatedAt or bumps Version.
	RenewShard(ctx context.Context, pool string, txid chainhash.Hash, holder string, until time.Time) (ShardHead, error)
	// ReleaseShard clears the lease if Owner == holder, after which the lease is free
	// and a further RenewShard by the same holder returns ErrNotOwner; otherwise it is
	// a no-op. Missing shard: ErrNotFound.
	ReleaseShard(ctx context.Context, pool string, txid chainhash.Hash, holder string) error
	// ListShards returns shards ordered by (created_at, txid compared as its 32
	// internal bytes, not display hex) with BEEF omitted. f.Owner matches the stored
	// owner string regardless of lease expiry. f.Limit must be >= 0 and every status in
	// f.Statuses must be known (ErrInvalidArgument otherwise).
	ListShards(ctx context.Context, pool string, f ShardFilter) ([]Shard, error)

	// ListAvailable returns available rows of one shard with vout >= fromVout,
	// ordered by vout, at most limit. An unknown txid returns an empty slice, not
	// ErrNotFound, like GetRows.
	ListAvailable(ctx context.Context, pool string, txid chainhash.Hash, fromVout uint32, limit int) ([]Row, error)
	// GetRows returns the rows that exist in pool among ops, in any order. Unknown
	// outpoints are omitted.
	GetRows(ctx context.Context, pool string, ops []Outpoint) ([]Row, error)
	// Reserve performs the reservation CAS for each op and returns the applied ops,
	// in any order (ExpiresAt reflects the stored value, which on a replay is the
	// original; see "Idempotency" and "Provenance"). Ops whose row is missing, in
	// another state, or reserved under another token are omitted. Batch-level
	// ErrInvalidArgument for duplicates, zero tokens, an expiry that is not
	// strictly after the Unix epoch (see "Clocks"), or size.
	Reserve(ctx context.Context, pool string, ops []ReserveOp) ([]ReserveOp, error)
	// Release moves rows from reserved(token) back to available, clearing
	// ReservedBy, ReservedAt, ExpiresAt, QuarantinedAt and SpentHint (see
	// "Provenance"). Returns the number of rows changed.
	Release(ctx context.Context, pool string, token Token, ops []Outpoint) (int, error)
	// Spend moves rows from reserved(token) to quarantined with QuarantinedAt = now
	// and SpentHint = txid, or, when terminal is true (spend_policy=trust), directly
	// to spent with SpentBy = txid. terminal=true requires a non-zero txid
	// (ErrInvalidArgument otherwise), since SpentBy is meant to be an oracle-proven
	// fact; terminal=false accepts a zero txid and still moves the row to
	// quarantined, but records no hint (SpentHint stays nil). ReservedBy, ReservedAt
	// and ExpiresAt are kept for provenance either way (see "Provenance"). Returns
	// the number of rows changed.
	Spend(ctx context.Context, pool string, token Token, ops []Outpoint, txid chainhash.Hash, terminal bool) (int, error)
	// Quarantine moves up to limit reserved rows whose expires_at < before to
	// quarantined, picked in (expires_at, txid internal bytes, vout) ascending
	// order, and returns how many it moved. ReservedBy, ReservedAt and ExpiresAt are
	// kept for provenance (see "Provenance"). before is a comparison bound, not
	// stored data (see "Clocks"): the zero time, the exact Unix epoch instant, or
	// any earlier time all match nothing, since every stored expires_at is strictly
	// after the epoch, and Quarantine returns 0, nil for these rather than
	// ErrInvalidArgument.
	Quarantine(ctx context.Context, pool string, before time.Time, limit int) (int, error)
	// ListQuarantined returns quarantined rows ordered by (quarantined_at, txid
	// internal bytes, vout) ascending, at most limit. before is exclusive on
	// quarantined_at. When after != nil, only rows whose (quarantined_at, txid,
	// vout) tuple is strictly greater than after's are returned, letting a reaper
	// resume paging after the last row it saw (Row.QuarantineCursor) instead of
	// always restarting from before: a row stuck at the head of the queue cannot
	// starve the rest of it. before and after are comparison bounds, not stored
	// data (see "Clocks"): a before at or before the Unix epoch, or an after cursor
	// whose At is at or past before, both match nothing -- an empty page, nil --
	// rather than ErrInvalidArgument.
	ListQuarantined(ctx context.Context, pool string, before time.Time, after *RowCursor, limit int) ([]Row, error)
	// Resolve moves quarantined rows to available, clearing ReservedBy, ReservedAt,
	// ExpiresAt, QuarantinedAt and SpentHint, or to spent, setting SpentBy and
	// keeping every other provenance field (see "Provenance"). Returns the number
	// of rows changed.
	Resolve(ctx context.Context, pool string, ops []Outpoint, to State, spentBy *chainhash.Hash) (int, error)
	// Stats counts rows by state and shards by status for one pool.
	Stats(ctx context.Context, pool string) (Stats, error)

	Leaser
	// Close releases resources. It is idempotent.
	Close() error
}

// Leaser is single-runner election for background jobs. It has no fencing token:
// only gate work that is safe if two holders briefly overlap (every job write in
// FuelKeeper is a CAS, so that holds).
type Leaser interface {
	// TryAcquireOrRenew grants or extends the named lease for holder until now+ttl
	// when the lease is free (no holder, or its expiry <= now) or already held by
	// holder. It returns the new expiry, or the zero time when another holder owns an
	// unexpired lease (now < expiry). Errors are for infrastructure failures only.
	TryAcquireOrRenew(ctx context.Context, name, holder string, ttl time.Duration) (time.Time, error)
	// ReleaseLease drops the lease if holder owns it; otherwise it is a no-op.
	// (Named ReleaseLease because Store.Release is the row operation.)
	ReleaseLease(ctx context.Context, name, holder string) error
}

// SpendRequest is one Spend call's arguments, as carried by SpendBatch.
type SpendRequest struct {
	Token    Token
	Ops      []Outpoint
	TxID     chainhash.Hash
	Terminal bool
}

// SpendBatcher is an optional Store extension: a backend that implements it
// can apply many Spend calls in one durable write (one transaction, one
// fsync), which is what lets the pool group-commit spend reports from many
// concurrent callers (pkg/pool's spend batcher). A backend that does not
// implement it simply gets one Spend call per request.
//
// SpendBatch applies reqs in one durable write and returns, per request,
// exactly what Spend(ctx, pool, r.Token, r.Ops, r.TxID, r.Terminal) would
// have returned had the requests been applied one after another in slice
// order: the same outpoint named by two requests is moved by the first one
// whose token holds it, and every later request counts 0 for it. Every
// request in a batch is stamped with the same now. The rows a batch moves end
// up exactly as sequential Spend calls would leave them (state, provenance,
// SpentHint/SpentBy), apart from that shared UpdatedAt/QuarantinedAt.
//
// Validation is batch-level: ValidateSpendBatch (pool, 1..Caps.MaxBatch
// requests whose outpoints total at most Caps.MaxBatch, and every request
// valid exactly as Spend validates its own arguments). One invalid request
// fails the whole batch with ErrInvalidArgument and applies nothing, so a
// caller must pre-validate each request with ValidateSpendRequest (the pool
// does) and never submit one that Spend itself would reject.
//
// An error means none of the batch is known to be applied: it may have
// committed in full or not at all (never partially -- it is one atomic
// write), so the whole batch may be retried. A retry is state-safe, because
// Spend is a compare-and-set on reserved(token): a row the first attempt
// already moved is no longer reserved, so the retry can neither move it again
// nor overwrite its SpentHint/SpentBy. It is not count-preserving, though: a
// retry after an in-doubt commit reports 0 for every row the lost attempt
// moved, exactly as a sequential Spend replay would.
type SpendBatcher interface {
	SpendBatch(ctx context.Context, pool string, reqs []SpendRequest) ([]int, error)
}
