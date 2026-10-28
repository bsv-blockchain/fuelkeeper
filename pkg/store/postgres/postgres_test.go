//go:build postgres

package postgres_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/postgres"
	"github.com/bsv-blockchain/fuelkeeper/pkg/store/storetest"
)

func testDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("FUELKEEPER_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("FUELKEEPER_TEST_POSTGRES_DSN not set")
	}
	return dsn
}

// isolatedStoreTB is isolatedStore's testing.TB-based implementation, so
// benchmarks (*testing.B) can get their own isolated schema the same way
// tests (*testing.T) do.
func isolatedStoreTB(tb testing.TB, admin *pgxpool.Pool, dsn string, now func() time.Time) *postgres.Store {
	tb.Helper()
	ctx := context.Background()
	var b [6]byte
	_, _ = rand.Read(b[:])
	schema := "fk_" + hex.EncodeToString(b[:])
	_, err := admin.Exec(ctx, "CREATE SCHEMA "+schema)
	require.NoError(tb, err)
	tb.Cleanup(func() { _, _ = admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE") })

	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	// MaxConns: 2, not the package default: RunSuite runs 32 subtests in
	// parallel, each opening its own pool, and CI's Postgres service runs with
	// the default max_connections=100 (only the local docker-compose sets it to
	// 300), so 32 * MaxConns must stay well under 100. -parallel 8 (pinned in
	// the Makefile's test-integration target) additionally bounds how many of
	// those pools are ever open at once, to 8 * MaxConns = 16 connections.
	s, err := postgres.New(ctx, postgres.Config{DSN: dsn + sep + "search_path=" + schema, MaxConns: 2}, postgres.WithClock(now))
	require.NoError(tb, err)
	return s
}

// isolatedStore creates a schema, opens a store scoped to it via search_path,
// and drops the schema on cleanup.
func isolatedStore(t *testing.T, admin *pgxpool.Pool, dsn string, now func() time.Time) *postgres.Store {
	t.Helper()
	return isolatedStoreTB(t, admin, dsn, now)
}

func TestConformance(t *testing.T) {
	dsn := testDSN(t)
	admin, err := pgxpool.New(context.Background(), dsn)
	require.NoError(t, err)
	t.Cleanup(admin.Close)

	storetest.RunSuite(t, func(t *testing.T, now func() time.Time) store.Store {
		return isolatedStore(t, admin, dsn, now)
	})
}

func TestAcquireSkipsDrained(t *testing.T) {
	dsn := testDSN(t)
	admin, err := pgxpool.New(context.Background(), dsn)
	require.NoError(t, err)
	t.Cleanup(admin.Close)

	storetest.RunAcquireSkipsDrained(t, func(t *testing.T, now func() time.Time) store.Store {
		return isolatedStore(t, admin, dsn, now)
	})
}

// TestSynchronousCommitOffRejected proves New refuses to open a store whose
// server-side synchronous_commit is off (G1 depends on an acknowledged commit
// being durable), and that a normal open against the same server still
// succeeds. The "options" connection parameter applies a session-scoped
// "-c synchronous_commit=off" as if passed on the postmaster command line,
// without touching the role or database's persistent setting.
func TestSynchronousCommitOffRejected(t *testing.T) {
	dsn := testDSN(t)
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	offDSN := dsn + sep + "options=-c%20synchronous_commit%3Doff"

	_, err := postgres.New(context.Background(), postgres.Config{DSN: offDSN})
	require.ErrorIs(t, err, store.ErrInvalidArgument)

	s, err := postgres.New(context.Background(), postgres.Config{DSN: dsn})
	require.NoError(t, err)
	require.NoError(t, s.Close())
}

var _ store.SpendBatcher = (*postgres.Store)(nil)
