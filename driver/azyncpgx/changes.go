package azyncpgx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/kausys/azync/driver"

	"github.com/google/uuid"
)

const (
	// changeChannel is the fixed NOTIFY channel the 00011 triggers emit on.
	// Deliberately not configurable: the name is baked into released
	// migration SQL, so a runtime knob could only disagree with it. Schema
	// isolation rides the payload instead — the trigger stamps
	// TG_TABLE_SCHEMA and the listener filters (see resolveChangeSchema).
	changeChannel = "azync_changes"
	// changeBuffer bounds each change subscriber's channel. A full buffer
	// collapses the dropped hints into one in-band reset.
	changeBuffer = 256
)

// Changes implements the optional driver.ChangeNotifier capability over a
// second, lazily-started LISTEN connection. It is separate from the wakeup
// listener on purpose: every worker subscribes to wakeups, and one shared
// connection would push the full change firehose at all of them; this way
// only processes that actually watch pay for the stream. A poll-only store
// returns the nil-channel contract signal.
func (s *Store) Changes(ctx context.Context) (<-chan driver.Change, error) {
	if s.pollOnly {
		//nolint:nilnil // poll-only backend: a nil channel with nil error is the contract signal
		return nil, nil
	}
	if err := s.resolveChangeSchema(ctx); err != nil {
		return nil, err
	}
	return s.changes.subscribe(ctx)
}

// resolveChangeSchema pins the schema name change payloads are filtered by.
// A configured schema is authoritative (the pool's search_path is set to
// exactly it); only an unconfigured store asks the backend, lazily on the
// first Changes call, under a plain mutex rather than sync.Once so a
// transient query failure can be retried. It runs before the listener's
// first subscription, so the parse path always sees a resolved value.
func (s *Store) resolveChangeSchema(ctx context.Context) error {
	s.changeSchemaMu.Lock()
	defer s.changeSchemaMu.Unlock()
	if s.changeSchema != "" {
		return nil
	}
	if s.schema != "" {
		s.changeSchema = s.schema
		return nil
	}
	var resolved *string
	if err := s.pool.QueryRow(ctx, "SELECT current_schema()").Scan(&resolved); err != nil {
		return fmt.Errorf("azyncpgx: resolve schema for change notifications: %w", err)
	}
	if resolved == nil || *resolved == "" {
		return errors.New("azyncpgx: current_schema() resolves to nothing; has Migrate created the schema?")
	}
	s.changeSchema = *resolved
	return nil
}

// matchesChangeSchema reports whether a payload's schema is this store's.
// Cross-schema traffic on the shared channel is expected (other azync
// installs in the same database) and silently dropped.
func (s *Store) matchesChangeSchema(schema string) bool {
	s.changeSchemaMu.Lock()
	defer s.changeSchemaMu.Unlock()
	return schema == s.changeSchema
}

// parseChangePayload is the change listener's parse hook: decode, then filter
// by schema.
func (s *Store) parseChangePayload(payload string) (driver.Change, bool) {
	c, schema, ok := parseChange(payload)
	if !ok || !s.matchesChangeSchema(schema) {
		return driver.Change{}, false
	}
	return c, true
}

// resetChange mints the in-band gap signal for the change listener.
func resetChange() driver.Change {
	return driver.Change{Entity: driver.ChangeReset, At: time.Now()}
}

// changeWire is the JSON payload shape the 00011 triggers emit. Nulls are
// stripped trigger-side, so absent fields decode to zero values.
type changeWire struct {
	Schema  string `json:"schema"`
	Entity  string `json:"entity"`
	Source  string `json:"source"`
	ID      string `json:"id"`
	DAGID   string `json:"dagId"`
	Kind    string `json:"kind"`
	TaskKey string `json:"taskKey"`
	State   string `json:"state"`
	AtMs    int64  `json:"atMs"`
	Bulk    bool   `json:"bulk"`
	Count   int    `json:"count"`
}

// parseChange decodes one azync_changes payload into a driver.Change plus its
// origin schema. Like parseWake, anything unparseable is dropped, never an
// error: the channel is shared and a foreign or future payload is not this
// listener's problem. The triggers never emit resets — those are minted
// client-side — so a "reset" entity in a payload is dropped as unknown.
func parseChange(payload string) (driver.Change, string, bool) {
	var w changeWire
	if err := json.Unmarshal([]byte(payload), &w); err != nil {
		return driver.Change{}, "", false
	}
	if w.Schema == "" {
		return driver.Change{}, "", false
	}
	var entity driver.ChangeEntity
	switch w.Entity {
	case string(driver.ChangeJob):
		entity = driver.ChangeJob
	case string(driver.ChangeDAG):
		entity = driver.ChangeDAG
	case string(driver.ChangeEvent):
		entity = driver.ChangeEvent
	default:
		return driver.Change{}, "", false
	}
	c := driver.Change{
		Entity:  entity,
		Source:  driver.Source(w.Source),
		Kind:    w.Kind,
		TaskKey: w.TaskKey,
		State:   w.State,
		At:      time.UnixMilli(w.AtMs),
		Bulk:    w.Bulk,
		Count:   w.Count,
	}
	if !w.Bulk {
		id, err := uuid.Parse(w.ID)
		if err != nil {
			return driver.Change{}, "", false
		}
		c.ID = id
		if w.DAGID != "" {
			dagID, err := uuid.Parse(w.DAGID)
			if err != nil {
				return driver.Change{}, "", false
			}
			c.DAGID = dagID
		}
	}
	return c, w.Schema, true
}
