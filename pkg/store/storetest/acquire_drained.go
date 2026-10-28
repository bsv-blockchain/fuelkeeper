package storetest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/fuelkeeper/pkg/store"
)

// RunAcquireSkipsDrained checks that AcquireShard passes over older shards with
// no available row (all reserved, quarantined or spent) and adopts the oldest
// shard that still has one. Only the SQL backends promise this (see store.go,
// AcquireShard), so it is not part of RunSuite.
func RunAcquireSkipsDrained(t *testing.T, newStore Factory) {
	t.Helper()
	ctx := context.Background()
	clk := NewClock(Start)
	s := newStore(t, clk.Now)
	t.Cleanup(func() { _ = s.Close() })

	// More drained shards than the shard manager's exclusion list holds.
	const drained = 300
	for range drained {
		_, rows := MintShard(t, s, "main", 3, store.ShardMined)
		tok := store.NewToken()
		MustReserve(t, s, "main", rows, tok, clk.Now().Add(time.Minute))
		_, err := s.Spend(ctx, "main", tok, Ops(rows[:1]), NewTxID(), false) // quarantined
		require.NoError(t, err)
		_, err = s.Spend(ctx, "main", tok, Ops(rows[1:2]), NewTxID(), true) // spent
		require.NoError(t, err)
		// rows[2] stays reserved
		clk.Advance(time.Millisecond)
	}
	live, _ := MintShard(t, s, "main", 3, store.ShardMined)

	now := clk.Now()
	got, err := s.AcquireShard(ctx, "main", "h1", now, now.Add(time.Minute), []store.ShardStatus{store.ShardMined}, nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, live.TxID, got.TxID)

	// Nothing else has fuel to serve.
	got, err = s.AcquireShard(ctx, "main", "h2", now, now.Add(time.Minute), []store.ShardStatus{store.ShardMined}, nil)
	require.NoError(t, err)
	require.Nil(t, got)
}
