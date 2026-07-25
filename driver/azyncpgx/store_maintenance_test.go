package azyncpgx

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStatsRetentionDays(t *testing.T) {
	is := require.New(t)

	// Whole-day retentions map exactly.
	is.Equal(1, statsRetentionDays(24*time.Hour))
	is.Equal(35, statsRetentionDays(35*24*time.Hour))

	// Sub-day and fractional retentions round UP so a day whose newest counter
	// is still within the retention window is never trimmed.
	is.Equal(1, statsRetentionDays(time.Hour))
	is.Equal(2, statsRetentionDays(25*time.Hour))
	is.Equal(2, statsRetentionDays(36*time.Hour))
}
