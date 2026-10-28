package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

const shardCols = `pool, txid, size, denomination, lock_mode, status, owner, lease_until, version, beef, created_at, updated_at`

// acquireShardSQL is AcquireShard's statement: pool = $1, statuses = $2,
// now = $3, holder = $4, until = $5, exclude = $6, store-clock now = $7 (see
// the AcquireShard stamps UpdatedAt from the store clock comment there). The
// lease-free predicate uses "<=" (a lease is free at exactly LeaseUntil ==
// now, per store.go's "Owner == "" or LeaseUntil <= now" rule), and NOT
// (txid = ANY(exclude)) so a caller can skip shards it already knows about.
// The EXISTS probe (fuel_rows_avail, state 0 = StateAvailable) skips shards
// with nothing to serve, so drained shards whose rows wait in quarantine
// cannot pile up ahead of live ones (see store.go, AcquireShard). It is
// wrapped as a scalar "(SELECT EXISTS ...)" on purpose: a bare EXISTS is
// pulled up into a semi-join that reads every available row of the pool
// before touching fuel_shards; the scalar form stays a per-candidate index
// probe on the shards scan (pinned by explain_test.go).
const acquireShardSQL = `
	WITH c AS (
	  SELECT pool, txid FROM fuel_shards
	   WHERE pool = $1 AND status = ANY($2::int2[])
	     AND (owner = '' OR lease_until IS NULL OR lease_until <= $3)
	     AND NOT (txid = ANY($6::bytea[]))
	     AND (SELECT EXISTS (SELECT 1 FROM fuel_rows r
	                          WHERE r.pool = fuel_shards.pool AND r.txid = fuel_shards.txid AND r.state = 0))
	   ORDER BY created_at, txid
	   LIMIT 1
	   FOR UPDATE SKIP LOCKED)
	UPDATE fuel_shards s SET owner = $4, lease_until = $5, updated_at = $7
	  FROM c
	 WHERE s.pool = c.pool AND s.txid = c.txid
	   AND (s.owner = '' OR s.lease_until IS NULL OR s.lease_until <= $3)
	RETURNING s.pool, s.txid, s.size, s.denomination, s.lock_mode, s.status, s.owner, s.lease_until, s.version, s.beef, s.created_at, s.updated_at`

// retireShardRowSQL is RetireShard's per-row statement: pool = $1, txid = $2,
// vout = $3, now = $4.
//
// (state IN (0,1,2)) IS TRUE, not state IN (0,1,2): see the longer comment on
// updateReservedWhereSQL (rows.go). This predicate exactly matches
// fuel_rows_shard_live's partial condition, and unlike the discovery read in
// RetireShard (which legitimately wants that index, since it's scanning a
// whole shard), this is a per-row point update keyed by the primary key.
const retireShardRowSQL = `
	UPDATE fuel_rows SET state=4, updated_at=$4
	 WHERE pool=$1 AND txid=$2 AND vout=$3 AND (state IN (0,1,2)) IS TRUE`

type rowScanner interface{ Scan(dest ...any) error }

func scanShard(sc rowScanner, withBEEF bool) (store.Shard, error) {
	var (
		sh          store.Shard
		txid        []byte
		size        int32
		denom       int64
		mode, stat  int16
		leaseUntil  *time.Time
		beef        []byte
		created, up time.Time
	)
	dest := []any{&sh.Pool, &txid, &size, &denom, &mode, &stat, &sh.Owner, &leaseUntil, &sh.Version, &beef, &created, &up}
	if err := sc.Scan(dest...); err != nil {
		return store.Shard{}, err
	}
	copy(sh.TxID[:], txid)
	sh.Size = int(size)
	sh.Denomination = uint64(denom)     //nolint:gosec // stored from uint64
	sh.LockMode = store.LockMode(mode)  //nolint:gosec // small enum
	sh.Status = store.ShardStatus(stat) //nolint:gosec // small enum
	sh.LeaseUntil = utc(leaseUntil)
	if withBEEF {
		sh.BEEF = beef
	}
	sh.CreatedAt, sh.UpdatedAt = created.UTC(), up.UTC()
	return sh, nil
}

// PutShard implements store.Store.
func (s *Store) PutShard(ctx context.Context, shard store.Shard, rows []store.Row) error {
	if err := store.ValidatePutShard(shard, rows); err != nil {
		return err
	}
	now := s.now()
	return s.tx(ctx, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			INSERT INTO fuel_shards (`+shardCols+`)
			VALUES ($1, $2, $3, $4, $5, $6, '', NULL, 1, $7, $8, $8)
			ON CONFLICT (pool, txid) DO NOTHING`,
			shard.Pool, shard.TxID[:], int32(shard.Size), int64(shard.Denomination), int16(shard.LockMode), int16(shard.Status), shard.BEEF, now) //nolint:gosec // validated ranges
		if err != nil {
			var pgErr *pgconn.PgError
			// Match the specific constraint, not any 23505: the ON CONFLICT target
			// above already absorbs a (pool, txid) replay, so the only unique
			// violation that can still reach here is fuel_shards_txid (same txid
			// under a different pool) — but naming it precisely means a future
			// constraint on this table can't be silently misreported as ErrShardExists.
			if errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == "fuel_shards_txid" {
				return store.ErrShardExists
			}
			return err
		}
		if tag.RowsAffected() == 0 {
			var size int32
			var denom int64
			var mode int16
			err := tx.QueryRow(ctx, `SELECT size, denomination, lock_mode FROM fuel_shards WHERE pool=$1 AND txid=$2`, shard.Pool, shard.TxID[:]).Scan(&size, &denom, &mode)
			if err != nil {
				return err
			}
			if int(size) != shard.Size || uint64(denom) != shard.Denomination || store.LockMode(mode) != shard.LockMode { //nolint:gosec // stored from uint64
				return store.ErrShardExists
			}
			return nil // idempotent replay
		}
		vouts := make([]int32, len(rows))
		sats := make([]int64, len(rows))
		ls := make([][]byte, len(rows))
		us := make([][]byte, len(rows))
		for i := range rows {
			vouts[i] = int32(rows[i].Vout)    //nolint:gosec // bounded
			sats[i] = int64(rows[i].Satoshis) //nolint:gosec // bounded
			ls[i] = rows[i].LockingScript
			us[i] = rows[i].UnlockingScript
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO fuel_rows (txid, vout, pool, satoshis, locking_script, unlocking_script, state, updated_at)
			SELECT $1::bytea, v.vout, $2::text, v.sats, v.ls, v.us, 0, $3::timestamptz
			  FROM unnest($4::int4[], $5::int8[], $6::bytea[], $7::bytea[]) AS v(vout, sats, ls, us)
			ON CONFLICT (txid, vout) DO NOTHING`,
			shard.TxID[:], shard.Pool, now, vouts, sats, ls, us)
		return err
	})
}

// GetShard implements store.Store.
func (s *Store) GetShard(ctx context.Context, pool string, txid chainhash.Hash) (*store.Shard, error) {
	if err := store.ValidatePool(pool); err != nil {
		return nil, err
	}
	row := s.pool.QueryRow(ctx, `SELECT `+shardCols+` FROM fuel_shards WHERE pool=$1 AND txid=$2`, pool, txid[:])
	sh, err := scanShard(row, true)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, wrap(err)
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
	tag, err := s.pool.Exec(ctx, `
		UPDATE fuel_shards
		   SET status = $3,
		       beef = COALESCE($4::bytea, beef),
		       version = version + 1,
		       updated_at = $5
		 WHERE pool = $1 AND txid = $2 AND status IN (0, 1)`, pool, txid[:], int16(status), beef, s.now())
	if err != nil {
		return wrap(err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	// Nothing updated: either the shard is missing or it is terminal.
	var cur int16
	err = s.pool.QueryRow(ctx, `SELECT status FROM fuel_shards WHERE pool = $1 AND txid = $2`, pool, txid[:]).Scan(&cur)
	if errors.Is(err, pgx.ErrNoRows) {
		return store.ErrNotFound
	}
	if err != nil {
		return wrap(err)
	}
	return store.ErrTerminalShard
}

// RetireShard implements store.Store.
func (s *Store) RetireShard(ctx context.Context, pool string, txid chainhash.Hash) (int, error) {
	if err := store.ValidatePool(pool); err != nil {
		return 0, err
	}
	now := s.now()
	n := 0
	err := s.tx(ctx, func(tx pgx.Tx) error {
		// status <> 3 keeps a second retire from bumping the version; the row update
		// below still runs (and changes nothing) so the call stays idempotent.
		tag, err := tx.Exec(ctx, `
			UPDATE fuel_shards SET status=3, owner='', lease_until=NULL, version=version+1, updated_at=$3
			 WHERE pool=$1 AND txid=$2 AND status <> 3`, pool, txid[:], now)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			var exists bool
			if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM fuel_shards WHERE pool=$1 AND txid=$2)`, pool, txid[:]).Scan(&exists); err != nil {
				return err
			}
			if !exists {
				return store.ErrNotFound
			}
		}
		// Invalidate this shard's non-terminal rows one at a time, in ascending vout
		// order (a single txid, so this matches the global (txid, vout) order
		// Reserve/Release/Spend/Resolve sort their own batches into; see
		// store.LessOutpoint in sort.go). A single multi-row UPDATE here is driven by
		// whatever order Postgres's scan of fuel_rows happens to produce, which can
		// disagree with those callers' sorted order and deadlock against them.
		rs, err := tx.Query(ctx, `SELECT vout FROM fuel_rows WHERE pool=$1 AND txid=$2 AND state IN (0,1,2) ORDER BY vout`, pool, txid[:])
		if err != nil {
			return err
		}
		var vouts []int32
		for rs.Next() {
			var v int32
			if err := rs.Scan(&v); err != nil {
				rs.Close()
				return err
			}
			vouts = append(vouts, v)
		}
		if err := rs.Err(); err != nil {
			rs.Close()
			return err
		}
		rs.Close()
		if len(vouts) == 0 {
			return nil
		}
		batch := &pgx.Batch{}
		for _, v := range vouts {
			batch.Queue(retireShardRowSQL, pool, txid[:], v, now)
		}
		n, err = execBatch(ctx, tx, batch, len(vouts))
		return err
	})
	if errors.Is(err, store.ErrNotFound) {
		return 0, store.ErrNotFound
	}
	if err != nil {
		return 0, err
	}
	return n, nil
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
	st := make([]int16, len(statuses))
	for i, x := range statuses {
		st[i] = int16(x)
	}
	ex := make([][]byte, len(exclude))
	for i := range exclude {
		ex[i] = exclude[i][:]
	}
	row := s.pool.QueryRow(ctx, acquireShardSQL, pool, st, now, holder, until, ex, s.now())
	sh, err := scanShard(row, true)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil //nolint:nilnil // contract: nothing adoptable
	}
	if err != nil {
		return nil, wrap(err)
	}
	return &sh, nil
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
	// Only ever write when holder is already the owner: a CASE-guarded
	// unconditional UPDATE would still create a new (dead) tuple version for
	// every non-owner call against a busy, contended shard, since Postgres's
	// MVCC writes a new row version whenever an UPDATE statement matches a row
	// at all, whether or not any column's value actually changes.
	var stat int16
	var version int64
	err := s.pool.QueryRow(ctx, `
		UPDATE fuel_shards SET lease_until = $4
		 WHERE pool = $1 AND txid = $2 AND owner = $3
		RETURNING status, version`, pool, txid[:], holder, until).Scan(&stat, &version)
	if err == nil {
		return store.ShardHead{Status: store.ShardStatus(stat), Version: version}, nil //nolint:gosec // small enum
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return store.ShardHead{}, wrap(err)
	}
	// Zero rows matched: either the shard doesn't exist, or holder isn't the
	// current owner. Distinguish with a read instead of folding both cases into
	// the write's WHERE clause, so a non-owner call never touches the row.
	exists, err := s.shardExists(ctx, pool, txid)
	if err != nil {
		return store.ShardHead{}, wrap(err)
	}
	if !exists {
		return store.ShardHead{}, store.ErrNotFound
	}
	return store.ShardHead{}, store.ErrNotOwner
}

// ReleaseShard implements store.Store.
func (s *Store) ReleaseShard(ctx context.Context, pool string, txid chainhash.Hash, holder string) error {
	if err := store.ValidatePool(pool); err != nil {
		return err
	}
	if err := store.ValidateHolder(holder); err != nil {
		return err
	}
	// See the comment in RenewShard: only write when holder actually owns the
	// lease, so a non-owner's no-op release never creates a dead tuple.
	tag, err := s.pool.Exec(ctx, `
		UPDATE fuel_shards SET owner = '', lease_until = NULL, updated_at = $4
		 WHERE pool = $1 AND txid = $2 AND owner = $3`, pool, txid[:], holder, s.now())
	if err != nil {
		return wrap(err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	// Zero rows matched: either the shard doesn't exist (ErrNotFound) or holder
	// isn't the owner, which the contract defines as a legitimate no-op.
	exists, err := s.shardExists(ctx, pool, txid)
	if err != nil {
		return wrap(err)
	}
	if !exists {
		return store.ErrNotFound
	}
	return nil
}

// shardExists reports whether (pool, txid) names a shard, regardless of status.
func (s *Store) shardExists(ctx context.Context, pool string, txid chainhash.Hash) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM fuel_shards WHERE pool = $1 AND txid = $2)`, pool, txid[:]).Scan(&exists)
	return exists, err
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
	sb.WriteString(`SELECT ` + shardCols + ` FROM fuel_shards WHERE pool = $1`)
	if len(f.Statuses) > 0 {
		st := make([]int16, len(f.Statuses))
		for i, x := range f.Statuses {
			st[i] = int16(x)
		}
		args = append(args, st)
		fmt.Fprintf(&sb, ` AND status = ANY($%d::int2[])`, len(args))
	}
	if f.Owner != nil {
		args = append(args, *f.Owner)
		fmt.Fprintf(&sb, ` AND owner = $%d`, len(args))
	}
	sb.WriteString(` ORDER BY created_at, txid`)
	if f.Limit > 0 {
		args = append(args, f.Limit)
		fmt.Fprintf(&sb, ` LIMIT $%d`, len(args))
	}
	rs, err := s.pool.Query(ctx, sb.String(), args...)
	if err != nil {
		return nil, wrap(err)
	}
	defer rs.Close()
	var out []store.Shard
	for rs.Next() {
		sh, err := scanShard(rs, false)
		if err != nil {
			return nil, wrap(err)
		}
		out = append(out, sh)
	}
	return out, wrap(rs.Err())
}
