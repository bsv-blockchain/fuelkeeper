package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

const shardCols = `pool, txid, size, denomination, lock_mode, status, owner, lease_until, version, beef, created_at, updated_at`

type rowScanner interface{ Scan(dest ...any) error }

func scanShard(sc rowScanner, withBEEF bool) (store.Shard, error) {
	var (
		sh               store.Shard
		txid, beef       []byte
		denom            int64
		mode, stat       int
		leaseUntil       sql.NullInt64
		created, updated int64
	)
	if err := sc.Scan(&sh.Pool, &txid, &sh.Size, &denom, &mode, &stat, &sh.Owner, &leaseUntil, &sh.Version, &beef, &created, &updated); err != nil {
		return store.Shard{}, err
	}
	copy(sh.TxID[:], txid)
	sh.Denomination = uint64(denom)     //nolint:gosec // stored from uint64
	sh.LockMode = store.LockMode(mode)  //nolint:gosec // small enum
	sh.Status = store.ShardStatus(stat) //nolint:gosec // small enum
	sh.LeaseUntil = fromUS(leaseUntil)
	if withBEEF {
		sh.BEEF = beef
	}
	sh.CreatedAt = time.UnixMicro(created).UTC()
	sh.UpdatedAt = time.UnixMicro(updated).UTC()
	return sh, nil
}

// shardExists reports whether (pool, txid) names a shard, regardless of
// status. It runs on c (the writer's connection) so it is safe to call from
// inside a write() callback: nothing else can be running against this store
// concurrently with it (see the package doc).
func shardExists(ctx context.Context, c querier, pool string, txid chainhash.Hash) (bool, error) {
	var one int
	err := c.QueryRowContext(ctx, `SELECT 1 FROM fuel_shards WHERE pool=? AND txid=?`, pool, txid[:]).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// PutShard implements store.Store.
func (s *Store) PutShard(ctx context.Context, shard store.Shard, rows []store.Row) error {
	if err := store.ValidatePutShard(shard, rows); err != nil {
		return err
	}
	now := s.now().UnixMicro()
	return s.write(ctx, func(ctx context.Context, c querier) error {
		var pool string
		var size, mode int
		var denom int64
		// Look up by txid alone: a shard's txid is globally unique across pools
		// (fuel_shards_txid), so this single lookup both finds an identical-key
		// replay and detects a cross-pool or mismatched-parameter conflict.
		err := c.QueryRowContext(ctx, `SELECT pool, size, denomination, lock_mode FROM fuel_shards WHERE txid=?`, shard.TxID[:]).Scan(&pool, &size, &denom, &mode)
		switch {
		case err == nil:
			if pool != shard.Pool || size != shard.Size || uint64(denom) != shard.Denomination || store.LockMode(mode) != shard.LockMode { //nolint:gosec // stored from uint64
				return store.ErrShardExists
			}
			return nil // idempotent replay: keeps the original version/BEEF/status and rows
		case !errors.Is(err, sql.ErrNoRows):
			return err
		}
		if _, err := c.ExecContext(ctx, `INSERT INTO fuel_shards (`+shardCols+`) VALUES (?,?,?,?,?,?,'',NULL,1,?,?,?)`,
			shard.Pool, shard.TxID[:], shard.Size, int64(shard.Denomination), int(shard.LockMode), int(shard.Status), shard.BEEF, now, now); err != nil { //nolint:gosec // validated
			return err
		}
		stmt, err := c.PrepareContext(ctx, `INSERT INTO fuel_rows (txid, vout, pool, satoshis, locking_script, unlocking_script, state, updated_at) VALUES (?,?,?,?,?,?,0,?)`)
		if err != nil {
			return err
		}
		defer func() { _ = stmt.Close() }()
		for i := range rows {
			r := &rows[i]
			if _, err := stmt.ExecContext(ctx, r.TxID[:], r.Vout, r.Pool, int64(r.Satoshis), r.LockingScript, r.UnlockingScript, now); err != nil { //nolint:gosec // validated
				return err
			}
		}
		return nil
	})
}

// GetShard implements store.Store.
func (s *Store) GetShard(ctx context.Context, pool string, txid chainhash.Hash) (*store.Shard, error) {
	if err := store.ValidatePool(pool); err != nil {
		return nil, err
	}
	sh, err := scanShard(s.reader.QueryRowContext(ctx, `SELECT `+shardCols+` FROM fuel_shards WHERE pool=? AND txid=?`, pool, txid[:]), true)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, wrapCtx(ctx, err)
	}
	return &sh, nil
}

// UpdateShard implements store.Store.
func (s *Store) UpdateShard(ctx context.Context, pool string, txid chainhash.Hash, status store.ShardStatus, beef []byte) error {
	if err := store.ValidatePool(pool); err != nil {
		return err
	}
	if err := store.ValidateUpdateStatus(status); err != nil {
		return err
	}
	now := s.now().UnixMicro()
	return s.write(ctx, func(ctx context.Context, c querier) error {
		var cur int
		err := c.QueryRowContext(ctx, `SELECT status FROM fuel_shards WHERE pool=? AND txid=?`, pool, txid[:]).Scan(&cur)
		if errors.Is(err, sql.ErrNoRows) {
			return store.ErrNotFound
		}
		if err != nil {
			return err
		}
		if store.ShardStatus(cur).Terminal() { //nolint:gosec // small enum
			return store.ErrTerminalShard
		}
		if beef != nil {
			_, err = c.ExecContext(ctx, `UPDATE fuel_shards SET status=?, beef=?, version=version+1, updated_at=? WHERE pool=? AND txid=?`, int(status), beef, now, pool, txid[:])
		} else {
			_, err = c.ExecContext(ctx, `UPDATE fuel_shards SET status=?, version=version+1, updated_at=? WHERE pool=? AND txid=?`, int(status), now, pool, txid[:])
		}
		return err
	})
}

// RetireShard implements store.Store.
func (s *Store) RetireShard(ctx context.Context, pool string, txid chainhash.Hash) (int, error) {
	if err := store.ValidatePool(pool); err != nil {
		return 0, err
	}
	now := s.now().UnixMicro()
	n := 0
	err := s.write(ctx, func(ctx context.Context, c querier) error {
		// status <> 3 keeps a second retire from bumping the version; the row
		// cascade below still runs (and, being idempotent, changes nothing further)
		// so the call as a whole stays idempotent.
		res, err := c.ExecContext(ctx, `UPDATE fuel_shards SET status=3, owner='', lease_until=NULL, version=version+1, updated_at=? WHERE pool=? AND txid=? AND status <> 3`, now, pool, txid[:])
		if err != nil {
			return err
		}
		if k, _ := res.RowsAffected(); k == 0 {
			exists, err := shardExists(ctx, c, pool, txid)
			if err != nil {
				return err
			}
			if !exists {
				return store.ErrNotFound
			}
		}
		// Invalidate this shard's non-terminal rows one at a time, in ascending
		// vout order (a single txid, so this already matches store.LessOutpoint's
		// global order), re-checking state IN (0,1,2) on each row so a row a
		// concurrent Reserve just touched is still caught correctly.
		vrows, err := c.QueryContext(ctx, `SELECT vout FROM fuel_rows WHERE pool=? AND txid=? AND state IN (0,1,2) ORDER BY vout`, pool, txid[:])
		if err != nil {
			return err
		}
		defer func() { _ = vrows.Close() }()
		var vouts []uint32
		for vrows.Next() {
			var v uint32
			if err := vrows.Scan(&v); err != nil {
				return err
			}
			vouts = append(vouts, v)
		}
		if err := vrows.Err(); err != nil {
			return err
		}
		if len(vouts) == 0 {
			return nil
		}
		stmt, err := c.PrepareContext(ctx, retireShardRowSQL)
		if err != nil {
			return err
		}
		defer func() { _ = stmt.Close() }()
		for _, v := range vouts {
			res, err := stmt.ExecContext(ctx, now, pool, txid[:], v)
			if err != nil {
				return err
			}
			k, _ := res.RowsAffected()
			n += int(k)
		}
		return nil
	})
	if errors.Is(err, store.ErrNotFound) {
		return 0, store.ErrNotFound
	}
	if err != nil {
		return 0, err
	}
	return n, nil
}

// retireShardRowSQL is RetireShard's per-row cascade statement: now = ?, pool
// = ?, txid = ?, vout = ?.
const retireShardRowSQL = `
	UPDATE fuel_rows SET state=4, updated_at=?
	 WHERE pool=? AND txid=? AND vout=? AND state IN (0,1,2)`

// acquireShardProbeSQL is one status's candidate probe: pool = ?, status = ?,
// lease-free now = ? [, exclude = (?, ...)]. Equality on status (not an IN
// (...) list) is what lets this probe seek fuel_shards_adopt (pool, status,
// created_at, txid) directly in (created_at, txid) order, instead of a
// multi-value IN forcing a scan across every requested status's range (or a
// separate sort): pinned by explain_test.go. The lease-free predicate uses
// "<=" (a lease is free at exactly LeaseUntil == now, per store.go's
// "Owner == \"\" or LeaseUntil <= now" rule). The EXISTS probe
// (fuel_rows_avail, state 0 = StateAvailable) skips shards with nothing to
// serve (see store.go, AcquireShard).
func acquireShardProbeSQL(nExclude int) string {
	q := `SELECT rowid, created_at, txid FROM fuel_shards
	       WHERE pool=? AND status=? AND (owner='' OR lease_until IS NULL OR lease_until <= ?)
	         AND EXISTS (SELECT 1 FROM fuel_rows r
	                      WHERE r.pool = fuel_shards.pool AND r.txid = fuel_shards.txid AND r.state = 0)`
	if nExclude > 0 {
		q += ` AND txid NOT IN (` + placeholders(nExclude) + `)`
	}
	return q + ` ORDER BY created_at, txid LIMIT 1`
}

// acquireShardSQL is AcquireShard's statement: holder = ?, until = ?, store
// clock now = ? (updated_at; see AcquireShard's doc comment on why this is
// the store's own clock and not the passed now), then one
// acquireShardProbeSQL group (pool = ?, status = ?, lease-free now = ? [,
// exclude = (?, ...)]) per requested status, unioned and re-limited to pick
// the overall oldest candidate across every probe.
func acquireShardSQL(nStatuses, nExclude int) string {
	// Each arm is "SELECT rowid, created_at, txid FROM (<probe>)", not the
	// probe's own "SELECT ... ORDER BY ... LIMIT ..." directly: SQLite's
	// compound-select grammar has no form of "(SELECT ... ORDER BY ... LIMIT
	// ...) UNION ALL (...)" (confirmed against sqlite3 3.51: "near UNION:
	// syntax error"), so each arm's ORDER BY/LIMIT must live inside its own
	// FROM-subquery instead, which SQLite fully supports.
	arm := "SELECT rowid, created_at, txid FROM (" + acquireShardProbeSQL(nExclude) + ")"
	arms := make([]string, nStatuses)
	for i := range arms {
		arms[i] = arm
	}
	return `UPDATE fuel_shards SET owner=?, lease_until=?, updated_at=? WHERE rowid = (
	          SELECT rowid FROM (` + strings.Join(arms, " UNION ALL ") + `)
	           ORDER BY created_at, txid LIMIT 1
	        ) RETURNING ` + shardCols
}

// AcquireShard implements store.Store.
func (s *Store) AcquireShard(ctx context.Context, pool, holder string, now, until time.Time, statuses []store.ShardStatus, exclude []chainhash.Hash) (*store.Shard, error) {
	if err := store.ValidatePool(pool); err != nil {
		return nil, err
	}
	if err := store.ValidateHolder(holder); err != nil {
		return nil, err
	}
	if err := store.ValidateShardStatuses(statuses); err != nil {
		return nil, err
	}
	if err := store.ValidateAcquireUntil(until, now); err != nil {
		return nil, err
	}
	args := []any{holder, until.UnixMicro(), s.now().UnixMicro()} // updated_at from the store clock
	for _, st := range statuses {
		args = append(args, pool, int(st), now.UnixMicro())
		for i := range exclude {
			args = append(args, exclude[i][:])
		}
	}
	q := acquireShardSQL(len(statuses), len(exclude))
	var out *store.Shard
	err := s.write(ctx, func(ctx context.Context, c querier) error {
		sh, err := scanShard(c.QueryRowContext(ctx, q, args...), true)
		if errors.Is(err, sql.ErrNoRows) {
			return nil // nothing adoptable
		}
		if err != nil {
			return err
		}
		out = &sh
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// RenewShard implements store.Store.
func (s *Store) RenewShard(ctx context.Context, pool string, txid chainhash.Hash, holder string, until time.Time) (store.ShardHead, error) {
	if err := store.ValidatePool(pool); err != nil {
		return store.ShardHead{}, err
	}
	if err := store.ValidateHolder(holder); err != nil {
		return store.ShardHead{}, err
	}
	if err := store.ValidateRenewUntil(until); err != nil {
		return store.ShardHead{}, err
	}
	var head store.ShardHead
	err := s.write(ctx, func(ctx context.Context, c querier) error {
		var stat int
		var version int64
		err := c.QueryRowContext(ctx, `
			UPDATE fuel_shards SET lease_until=?
			 WHERE pool=? AND txid=? AND owner=?
			RETURNING status, version`, until.UnixMicro(), pool, txid[:], holder).Scan(&stat, &version)
		if err == nil {
			head = store.ShardHead{Status: store.ShardStatus(stat), Version: version} //nolint:gosec // small enum
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		// Zero rows matched: either the shard doesn't exist, or holder isn't the
		// current owner. Distinguish with a read instead of folding both cases
		// into the write's WHERE clause, so a non-owner call never touches the row.
		exists, existErr := shardExists(ctx, c, pool, txid)
		if existErr != nil {
			return existErr
		}
		if !exists {
			return store.ErrNotFound
		}
		return store.ErrNotOwner
	})
	if err != nil {
		return store.ShardHead{}, err
	}
	return head, nil
}

// ReleaseShard implements store.Store.
func (s *Store) ReleaseShard(ctx context.Context, pool string, txid chainhash.Hash, holder string) error {
	if err := store.ValidatePool(pool); err != nil {
		return err
	}
	if err := store.ValidateHolder(holder); err != nil {
		return err
	}
	now := s.now().UnixMicro()
	return s.write(ctx, func(ctx context.Context, c querier) error {
		// Only ever write when holder is already the owner: the WHERE clause
		// re-asserts owner=? itself, so a non-owner call matches zero rows and this
		// is naturally a no-op without a separate existence check on that path.
		res, err := c.ExecContext(ctx, `UPDATE fuel_shards SET owner='', lease_until=NULL, updated_at=? WHERE pool=? AND txid=? AND owner=?`, now, pool, txid[:], holder)
		if err != nil {
			return err
		}
		if k, _ := res.RowsAffected(); k == 1 {
			return nil
		}
		// Zero rows matched: either the shard doesn't exist (ErrNotFound) or
		// holder isn't the owner, which the contract defines as a legitimate no-op.
		exists, err := shardExists(ctx, c, pool, txid)
		if err != nil {
			return err
		}
		if !exists {
			return store.ErrNotFound
		}
		return nil
	})
}

// ListShards implements store.Store.
func (s *Store) ListShards(ctx context.Context, pool string, f store.ShardFilter) ([]store.Shard, error) {
	if err := store.ValidatePool(pool); err != nil {
		return nil, err
	}
	if err := store.ValidateListShardsFilter(f); err != nil {
		return nil, err
	}
	var sb strings.Builder
	args := []any{pool}
	sb.WriteString(`SELECT ` + shardCols + ` FROM fuel_shards WHERE pool=?`)
	if len(f.Statuses) > 0 {
		sb.WriteString(` AND status IN (` + placeholders(len(f.Statuses)) + `)`)
		for _, st := range f.Statuses {
			args = append(args, int(st))
		}
	}
	if f.Owner != nil {
		sb.WriteString(` AND owner=?`)
		args = append(args, *f.Owner)
	}
	sb.WriteString(` ORDER BY created_at, txid`)
	if f.Limit > 0 {
		sb.WriteString(` LIMIT ?`)
		args = append(args, f.Limit)
	}
	rs, err := s.reader.QueryContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, wrapCtx(ctx, err)
	}
	defer func() { _ = rs.Close() }()
	var out []store.Shard
	for rs.Next() {
		sh, err := scanShard(rs, false)
		if err != nil {
			return nil, wrapCtx(ctx, err)
		}
		out = append(out, sh)
	}
	return out, wrapCtx(ctx, rs.Err())
}
