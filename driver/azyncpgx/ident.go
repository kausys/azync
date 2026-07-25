package azyncpgx

import "errors"

// maxPostgresIdentifierBytes is PostgreSQL's unquoted-identifier length limit.
// Schema, channel and migration-table names are validated against it before
// they are ever interpolated into SQL, so an arbitrary string never reaches the
// backend as an identifier.
const maxPostgresIdentifierBytes = 63

// errInvalidIdentifier is returned by validatePostgresIdentifier for a name that
// is not a legal unquoted PostgreSQL identifier.
var errInvalidIdentifier = errors.New("azyncpgx: invalid PostgreSQL identifier")

// errInvalidChannel is returned by validatePostgresChannel for a name that is
// not a legal unquoted PostgreSQL LISTEN/NOTIFY channel.
var errInvalidChannel = errors.New("azyncpgx: invalid PostgreSQL channel name")

// validatePostgresIdentifier reports whether name is a legal unquoted
// PostgreSQL identifier: a leading letter or underscore followed by letters,
// digits, underscores or dollar signs, at most 63 bytes.
func validatePostgresIdentifier(name string) error {
	if !isPostgresIdentifier(name) {
		return errInvalidIdentifier
	}
	return nil
}

// validatePostgresChannel reports whether name is a legal unquoted PostgreSQL
// LISTEN/NOTIFY channel name (the same lexical rule as an identifier).
func validatePostgresChannel(name string) error {
	if !isPostgresIdentifier(name) {
		return errInvalidChannel
	}
	return nil
}

func isPostgresIdentifier(name string) bool {
	if len(name) == 0 || len(name) > maxPostgresIdentifierBytes {
		return false
	}
	if !isPostgresIdentifierStart(name[0]) {
		return false
	}
	for index := 1; index < len(name); index++ {
		if !isPostgresIdentifierPart(name[index]) {
			return false
		}
	}
	return true
}

func isPostgresIdentifierStart(character byte) bool {
	return character == '_' ||
		(character >= 'a' && character <= 'z') ||
		(character >= 'A' && character <= 'Z')
}

func isPostgresIdentifierPart(character byte) bool {
	return isPostgresIdentifierStart(character) ||
		(character >= '0' && character <= '9') ||
		character == '$'
}
