package sqlite_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/sqlite"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

func TestConformance(t *testing.T) {
	storetest.RunSuite(t, func(t *testing.T, now func() time.Time) store.Store {
		s, err := sqlite.Open(context.Background(), sqlite.Config{Path: filepath.Join(t.TempDir(), "fuel.sqlite")}, sqlite.WithClock(now))
		require.NoError(t, err)
		return s
	})
}

func TestAcquireSkipsDrained(t *testing.T) {
	storetest.RunAcquireSkipsDrained(t, func(t *testing.T, now func() time.Time) store.Store {
		s, err := sqlite.Open(context.Background(), sqlite.Config{Path: filepath.Join(t.TempDir(), "fuel.sqlite")}, sqlite.WithClock(now))
		require.NoError(t, err)
		return s
	})
}

func TestDSNAndPragmas(t *testing.T) {
	dsn := sqlite.DSN("/tmp/x.sqlite", sqlite.SyncFull)
	require.Equal(t, "file:/tmp/x.sqlite?_txlock=immediate&_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=journal_size_limit(67108864)", dsn)
	require.Contains(t, sqlite.DSN("/tmp/x.sqlite", sqlite.SyncNormal), "synchronous(NORMAL)")

	readerDSN := sqlite.ReaderDSN("/tmp/x.sqlite", sqlite.SyncFull)
	require.Equal(t, "file:/tmp/x.sqlite?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=journal_size_limit(67108864)&_pragma=query_only(1)", readerDSN)
	require.NotContains(t, readerDSN, "_txlock", "the reader never opens a write transaction")

	escaped := sqlite.DSN("/tmp/weird%?#name.sqlite", sqlite.SyncFull)
	require.Contains(t, escaped, "/tmp/weird%25%3F%23name.sqlite?", "%, ? and # in the path must be percent-escaped")

	s, err := sqlite.Open(context.Background(), sqlite.Config{Path: filepath.Join(t.TempDir(), "p.sqlite")})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	p, err := s.Pragmas(context.Background())
	require.NoError(t, err)
	require.Equal(t, "wal", p.JournalMode)
	require.Equal(t, 5000, p.BusyTimeout)
	require.Equal(t, 2, p.Synchronous, "FULL")
	require.Equal(t, int64(67108864), p.JournalSizeLimit)
	require.True(t, p.ReaderQueryOnly)
}

// TestOpen_LeadingDoubleSlashPath proves a path that happens to start with a
// doubled leading slash (e.g. built by naively joining an empty root with an
// absolute path) still opens: without filepath.Clean, modernc.org/sqlite's
// URI parser would read "//..." as naming an authority component and fail
// with "invalid uri authority".
func TestOpen_LeadingDoubleSlashPath(t *testing.T) {
	path := "//" + filepath.Join(t.TempDir(), "x.sqlite")
	s, err := sqlite.Open(context.Background(), sqlite.Config{Path: path})
	require.NoError(t, err)
	require.NoError(t, s.Close())
}

func TestOpen_InvalidSync(t *testing.T) {
	_, err := sqlite.Open(context.Background(), sqlite.Config{Path: filepath.Join(t.TempDir(), "bad.sqlite"), Sync: "full"})
	require.ErrorIs(t, err, store.ErrInvalidArgument, "Sync is case-sensitive: lowercase must be rejected")

	_, err = sqlite.Open(context.Background(), sqlite.Config{Path: filepath.Join(t.TempDir(), "bad2.sqlite"), Sync: "OFF"})
	require.ErrorIs(t, err, store.ErrInvalidArgument)
}

func TestCheckpoint(t *testing.T) {
	ctx := context.Background()
	s, err := sqlite.Open(ctx, sqlite.Config{Path: filepath.Join(t.TempDir(), "checkpoint.sqlite")})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	_, rows := storetest.MintShard(t, s, "main", 50, store.ShardMined)
	_, err = s.Reserve(ctx, "main", storetest.ReserveOps(rows, store.NewToken(), time.Now().Add(time.Hour)))
	require.NoError(t, err)

	require.NoError(t, s.Checkpoint(ctx))
	// A maintenance job may call it repeatedly; it must stay safe to call again.
	require.NoError(t, s.Checkpoint(ctx))
}

var _ store.SpendBatcher = (*sqlite.Store)(nil)
