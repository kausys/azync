package azyncpgx

import (
	"context"
	"strings"
	"testing"

	"github.com/kausys/azync/driver"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newLazyPool builds a pool without connecting (MinConns defaults to 0, so no
// connection is opened until first use), which is enough to exercise the
// non-networking Store construction paths.
func newLazyPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), "postgres://azync:azync@127.0.0.1:5432/azync?sslmode=disable")
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return pool
}

func TestDefaultChannel(t *testing.T) {
	assert.Equal(t, "azync", defaultChannel(""))
	assert.Equal(t, "azync_tenant42", defaultChannel("tenant42"))
	// azync_ + a 60-char schema is 66 bytes, over the 63-byte channel limit, so
	// it falls back to the bare default.
	assert.Equal(t, "azync", defaultChannel(strings.Repeat("s", 60)))
}

func TestNewAppliesOptions(t *testing.T) {
	pool := newLazyPool(t)

	s := New(pool,
		WithNotifyChannel("custom_chan"),
		WithMigrationsTable("my_migrations"),
		WithSchema("myschema"),
	)
	assert.Equal(t, "custom_chan", s.notifyChannel)
	assert.Equal(t, "my_migrations", s.migrationsTable)
	assert.Equal(t, "myschema", s.schema)
	assert.False(t, s.ownsPool, "New must not claim ownership of a caller pool")
	assert.NotNil(t, s.listener)
}

func TestNewDefaults(t *testing.T) {
	pool := newLazyPool(t)

	s := New(pool)
	assert.Equal(t, "azync", s.notifyChannel)
	assert.Equal(t, "azync_migrations", s.migrationsTable)
	assert.NotNil(t, s.logger)
}

func TestNewSchemaDefaultsChannel(t *testing.T) {
	pool := newLazyPool(t)

	s := New(pool, WithSchema("myschema"))
	assert.Equal(t, "azync_myschema", s.notifyChannel)
}

func TestWakePollOnly(t *testing.T) {
	pool := newLazyPool(t)

	s := New(pool, PollOnly())
	ch, err := s.Wake(context.Background())
	require.NoError(t, err)
	assert.Nil(t, ch, "a poll-only store must report a nil wake channel")
}

func TestCapabilityInterfaces(t *testing.T) {
	pool := newLazyPool(t)
	var store driver.Store = New(pool)

	_, ok := store.(driver.Notifier)
	assert.True(t, ok, "Store must implement Notifier")
	_, ok = store.(driver.LeaderElector)
	assert.True(t, ok, "Store must implement LeaderElector")
	_, ok = store.(driver.Migrator)
	assert.True(t, ok, "Store must implement Migrator")
	_, ok = store.(driver.TxStore[pgx.Tx])
	assert.True(t, ok, "Store must implement TxStore[pgx.Tx]")
}

// TestOpenRedactsCredentials guards the security-critical property: no Open or
// parse error may leak the DSN, which can carry a password.
func TestOpenRedactsCredentials(t *testing.T) {
	const secret = "sup3rSecretPw"

	t.Run("parse failure", func(t *testing.T) {
		// A bad percent-encoding makes ParseConfig fail before any connection.
		_, err := open("postgres://user:"+secret+"@%zz/db", driver.Config{})
		require.Error(t, err)
		assert.NotContains(t, err.Error(), secret)
	})

	t.Run("connection failure", func(t *testing.T) {
		// Port 1 is closed, so the ping fails fast; the error must stay redacted.
		_, err := open("postgres://user:"+secret+"@127.0.0.1:1/db", driver.Config{})
		require.Error(t, err)
		assert.NotContains(t, err.Error(), secret)
	})
}

func TestOpenRejectsInvalidSchema(t *testing.T) {
	_, err := open("postgres://user:pw@127.0.0.1:5432/db", driver.Config{Schema: "bad-schema"})
	require.Error(t, err)
	assert.ErrorIs(t, err, errInvalidIdentifier)
}
