package engine

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type sentinel struct{ outcome Outcome }

func (s sentinel) Error() string         { return "sentinel" }
func (s sentinel) AsyncOutcome() Outcome { return s.outcome }

func TestIsPermanentReadsTheOutcomeAnywhereInTheChain(t *testing.T) {
	t.Parallel()
	is := require.New(t)

	abort := sentinel{Outcome{Kind: OutcomeAbort}}
	is.True(IsPermanent(abort))
	is.True(IsPermanent(fmt.Errorf("sending: %w", abort)), "wrapped")
	is.False(IsPermanent(errors.New("plain")), "a plain error is retried")
	is.False(IsPermanent(sentinel{Outcome{Kind: OutcomeSnooze, Delay: time.Second}}))
	is.False(IsPermanent(nil))
}

func TestWillRetryFollowsTheOutcomeAndTheBudget(t *testing.T) {
	t.Parallel()
	plain := errors.New("plain")
	cases := map[string]struct {
		err          error
		attempt, max int
		want         bool
	}{
		"nil is no failure":                {nil, 1, 3, false},
		"a plain error with attempts left": {plain, 1, 3, true},
		"a plain error on the last":        {plain, 3, 3, false},
		"an abort with attempts left":      {sentinel{Outcome{Kind: OutcomeAbort}}, 1, 3, false},
		"a skip":                           {sentinel{Outcome{Kind: OutcomeSkip}}, 1, 3, false},
		"a snooze on the last":             {sentinel{Outcome{Kind: OutcomeSnooze}}, 3, 3, true},
		"outside a job":                    {plain, 0, 0, false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, WillRetry(tc.err, tc.attempt, tc.max))
		})
	}
}

func TestLastAttempt(t *testing.T) {
	t.Parallel()
	is := require.New(t)

	is.False(LastAttempt(1, 3))
	is.True(LastAttempt(3, 3))
	is.False(LastAttempt(0, 0), "outside a job")
}
