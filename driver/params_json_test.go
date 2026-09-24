package driver_test

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/kausys/azync/driver"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// The params types are persisted by callers that hand them to a driver later
// (an outbox). These tests hold them to what that needs: every field survives
// the trip, empty values stay empty rather than turning into JSON nulls, and
// the keys never move — a renamed key would leave every row already stored
// undecodable.

var (
	fixedID   = uuid.MustParse("0192a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b")
	fixedTime = time.Date(2026, 9, 24, 13, 30, 0, 123456789, time.UTC)
)

func fullPublish() driver.PublishParams {
	return driver.PublishParams{
		ID:            fixedID,
		Type:          "orders.created.v1",
		AggregateType: "order",
		AggregateID:   "o-1",
		Version:       3,
		OccurredAt:    fixedTime,
		Payload:       json.RawMessage(`{"total":42}`),
		Meta:          map[string]string{"traceparent": "00-abc-def-01"},
	}
}

func fullEnqueue() driver.EnqueueParams {
	return driver.EnqueueParams{
		ID:                  fixedID,
		Kind:                "email.send",
		Payload:             json.RawMessage(`{"to":"a@b.c"}`),
		Meta:                map[string]string{"k": "v"},
		RunAt:               fixedTime,
		Delay:               time.Minute,
		MaxAttempts:         5,
		MaxAttemptsExplicit: true,
		IdempotencyKey:      "idem",
		IdempotencyTTL:      time.Hour,
	}
}

func fullDAG() driver.DAGParams {
	return driver.DAGParams{
		ID:             fixedID,
		Name:           "onboarding",
		OnFailure:      driver.OnFailureSuspend,
		IdempotencyKey: "once",
		Meta:           map[string]string{"k": "v"},
		Tasks: []driver.DAGTask{{
			Key:                 "charge",
			Kind:                "billing.charge",
			Payload:             json.RawMessage(`{"amount":1}`),
			MaxAttempts:         3,
			CompensationKind:    "billing.refund",
			CompensationPayload: json.RawMessage(`{"amount":1}`),
			SignalName:          "paid",
			SleepFor:            time.Second,
			IgnoreDeadDeps:      true,
			Deadline:            time.Hour,
		}},
		Deps: []driver.DAGDep{{TaskKey: "charge", DependsOnKey: "reserve"}},
	}
}

func TestParamsSurviveJSON(t *testing.T) {
	t.Run("publish", func(t *testing.T) {
		requireEveryFieldSet(t, fullPublish())
		requireRoundTrip(t, fullPublish())
		requireRoundTrip(t, driver.PublishParams{ID: fixedID, Type: "t"})
	})
	t.Run("enqueue", func(t *testing.T) {
		requireEveryFieldSet(t, fullEnqueue())
		requireRoundTrip(t, fullEnqueue())
		requireRoundTrip(t, driver.EnqueueParams{ID: fixedID, Kind: "k", Payload: json.RawMessage(`{}`)})
	})
	t.Run("dag", func(t *testing.T) {
		requireEveryFieldSet(t, fullDAG())
		requireRoundTrip(t, fullDAG())
		// A plain task and a timer: no payload, no compensation. Both must come
		// back nil, not as the JSON text null a driver would store as a value.
		requireRoundTrip(t, driver.DAGParams{ID: fixedID, Name: "n", Tasks: []driver.DAGTask{
			{Key: "a", Kind: "k"},
			{Key: "s", Kind: driver.KindSleep, SleepFor: time.Hour},
		}})
	})
}

// TestParamsJSONKeysAreFixed pins the encoded form. A change here is a change
// to data already at rest: it needs a new outbox format version, not an edit.
func TestParamsJSONKeysAreFixed(t *testing.T) {
	cases := []struct {
		name  string
		value any
		want  string
	}{
		{"publish", fullPublish(), `{"id":"0192a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b","type":"orders.created.v1",` +
			`"aggregateType":"order","aggregateId":"o-1","version":3,"occurredAt":"2026-09-24T13:30:00.123456789Z",` +
			`"payload":{"total":42},"meta":{"traceparent":"00-abc-def-01"}}`},
		{"enqueue", fullEnqueue(), `{"id":"0192a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b","kind":"email.send",` +
			`"payload":{"to":"a@b.c"},"meta":{"k":"v"},"runAt":"2026-09-24T13:30:00.123456789Z","delay":60000000000,` +
			`"maxAttempts":5,"maxAttemptsExplicit":true,"idempotencyKey":"idem","idempotencyTtl":3600000000000}`},
		{"dag", fullDAG(), `{"id":"0192a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b","name":"onboarding","onFailure":"suspend",` +
			`"idempotencyKey":"once","meta":{"k":"v"},"tasks":[{"key":"charge","kind":"billing.charge",` +
			`"payload":{"amount":1},"maxAttempts":3,"compensationKind":"billing.refund",` +
			`"compensationPayload":{"amount":1},"signalName":"paid","sleepFor":1000000000,` +
			`"ignoreDeadDeps":true,"deadline":3600000000000}],"deps":[{"taskKey":"charge","dependsOnKey":"reserve"}]}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := json.Marshal(c.value)
			require.NoError(t, err)
			require.JSONEq(t, c.want, string(got))
		})
	}
}

func requireRoundTrip[T any](t *testing.T, in T) {
	t.Helper()
	raw, err := json.Marshal(in)
	require.NoError(t, err)
	var out T
	require.NoError(t, json.Unmarshal(raw, &out))
	require.Equal(t, in, out, "decoded from %s", raw)
}

// requireEveryFieldSet fails when a fixture leaves a field at its zero value,
// descending into slices of structs. A field added to a params type later
// fails here until the fixtures cover it — and with them the round trip.
func requireEveryFieldSet(t *testing.T, v any) {
	t.Helper()
	var walk func(path string, rv reflect.Value)
	walk = func(path string, rv reflect.Value) {
		switch rv.Kind() {
		case reflect.Struct:
			if rv.Type() == reflect.TypeFor[time.Time]() {
				require.False(t, rv.Interface().(time.Time).IsZero(), "%s is zero", path)
				return
			}
			for i := range rv.NumField() {
				f := rv.Type().Field(i)
				walk(path+"."+f.Name, rv.Field(i))
			}
		case reflect.Slice:
			require.Positive(t, rv.Len(), "%s is empty", path)
			for i := range rv.Len() {
				walk(path, rv.Index(i))
			}
		default:
			require.False(t, rv.IsZero(), "%s is zero", path)
		}
	}
	walk(reflect.TypeOf(v).Name(), reflect.ValueOf(v))
}
