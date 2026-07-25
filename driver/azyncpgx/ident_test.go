package azyncpgx

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidatePostgresIdentifier(t *testing.T) {
	valid := []string{
		"queue",
		"_underscore",
		"a1",
		"azync_jobs",
		"Mixed_Case",
		"has$dollar",
		"x",
		strings.Repeat("a", 63), // exactly the byte limit
	}
	for _, name := range valid {
		t.Run("valid/"+name, func(t *testing.T) {
			require.NoError(t, validatePostgresIdentifier(name))
		})
	}

	invalid := []string{
		"",                      // empty
		"1leading_digit",        // starts with a digit
		"has-hyphen",            // hyphen is not allowed
		"has space",             // space is not allowed
		"has;semicolon",         // injection attempt
		"drop table",            // space again
		"\"quoted\"",            // quotes not allowed
		strings.Repeat("a", 64), // one byte over the limit
	}
	for _, name := range invalid {
		t.Run("invalid", func(t *testing.T) {
			err := validatePostgresIdentifier(name)
			require.ErrorIs(t, err, errInvalidIdentifier)
		})
	}
}

func TestValidatePostgresChannel(t *testing.T) {
	require.NoError(t, validatePostgresChannel("azync"))
	require.NoError(t, validatePostgresChannel("azync_tenant42"))

	err := validatePostgresChannel("bad-channel")
	require.ErrorIs(t, err, errInvalidChannel)

	require.Error(t, validatePostgresChannel(""))
	require.Error(t, validatePostgresChannel(strings.Repeat("z", 64)))
}
