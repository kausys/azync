package azyncpgx

import (
	"testing"

	"github.com/kausys/azync/driver"

	"github.com/stretchr/testify/assert"
)

func TestParseWake(t *testing.T) {
	cases := []struct {
		name    string
		payload string
		want    driver.Wake
		ok      bool
	}{
		{"queue kind", "queue:email.send", driver.Wake{Source: driver.SourceQueue, Kind: "email.send"}, true},
		{"event subscriber", "event:orders-projector", driver.Wake{Source: driver.SourceEvent, Kind: "orders-projector"}, true},
		{"kind with colon", "queue:ns:worker", driver.Wake{Source: driver.SourceQueue, Kind: "ns:worker"}, true},
		{"no colon", "queue", driver.Wake{}, false},
		{"empty", "", driver.Wake{}, false},
		{"empty source", ":kind", driver.Wake{}, false},
		{"empty kind", "queue:", driver.Wake{}, false},
		{"only colon", ":", driver.Wake{}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := parseWake(tc.payload)
			assert.Equal(t, tc.ok, ok)
			assert.Equal(t, tc.want, got)
		})
	}
}
