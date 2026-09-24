package azyncpgx

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCommandTagReportsRowsAffected pins the tag the database/sql adapter
// rebuilds: pgx callers read RowsAffected and the statement kind from it.
func TestCommandTagReportsRowsAffected(t *testing.T) {
	cases := []struct {
		query  string
		rows   int64
		want   string
		insert bool
	}{
		{"INSERT INTO azync_jobs (id) VALUES ($1)", 1, "INSERT 0 1", true},
		{"\n\tinsert into azync_events (id) values ($1)", 1, "INSERT 0 1", true},
		{"UPDATE azync_jobs SET state = 'x'", 3, "UPDATE 3", false},
		{"SELECT pg_notify($1, $2)", 1, "SELECT 1", false},
		{"WITH x AS (SELECT 1) DELETE FROM azync_jobs", 0, "WITH 0", false},
	}
	for _, c := range cases {
		tag := commandTag(c.query, c.rows)
		require.Equal(t, c.want, tag.String(), c.query)
		require.Equal(t, c.rows, tag.RowsAffected(), c.query)
		require.Equal(t, c.insert, tag.Insert(), c.query)
	}
}
