package azync_test

import (
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// TestPackageBoundaries enforces the module's architectural boundaries by
// walking every Go source file in the root module and checking its imports:
//
//   - the queue, event and workflow runtimes never import each other;
//   - no root-module file (tests included) imports a concrete persistence
//     dependency (pgx, gorm, goose, or database/sql) — those live only in the
//     separate driver/azyncpgx module;
//   - the driver contract package imports nothing but the standard library and
//     github.com/google/uuid.
//
// The separate modules driver/azyncpgx and examples are excluded. The walker
// tolerates the runtime directories not yet existing.
func TestPackageBoundaries(t *testing.T) {
	t.Parallel()

	err := filepath.WalkDir(".", func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return skipDir(path, d.Name())
		}
		if !strings.HasSuffix(d.Name(), ".go") {
			return nil
		}

		slashPath := filepath.ToSlash(path)
		isTest := strings.HasSuffix(d.Name(), "_test.go")

		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, imported := range file.Imports {
			importPath := strings.Trim(imported.Path.Value, `"`)
			assertNoPersistenceDependency(t, slashPath, importPath)
			if isTest {
				continue
			}
			assertRuntimeIsolation(t, slashPath, importPath)
			assertDriverContractIsPure(t, slashPath, importPath)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WalkDir error = %v", err)
	}
}

// skipDir prunes separate modules and dotted/tooling directories from the walk.
func skipDir(path, name string) error {
	if path == "." {
		return nil
	}
	if strings.HasPrefix(name, ".") {
		return fs.SkipDir
	}
	switch filepath.ToSlash(path) {
	case "driver/azyncpgx", "examples":
		return fs.SkipDir
	}
	return nil
}

// assertNoPersistenceDependency bans concrete storage dependencies everywhere in
// the root module: they belong only to the pluggable driver modules.
func assertNoPersistenceDependency(t *testing.T, filePath, importPath string) {
	t.Helper()
	for _, banned := range []string{
		"github.com/jackc/pgx",
		"gorm.io/",
		"github.com/pressly/goose",
		"database/sql",
	} {
		if importPath == banned || strings.HasPrefix(importPath, banned) {
			t.Errorf("%s imports banned persistence dependency %q", filePath, importPath)
		}
	}
}

// assertRuntimeIsolation forbids the queue, event and workflow runtimes from
// importing one another; they compose only through the shared core.
func assertRuntimeIsolation(t *testing.T, filePath, importPath string) {
	t.Helper()
	runtimes := []struct{ dir, pkg string }{
		{"queue/", "github.com/kausys/azync/queue"},
		{"event/", "github.com/kausys/azync/event"},
		{"workflow/", "github.com/kausys/azync/workflow"},
	}
	for _, self := range runtimes {
		if !strings.HasPrefix(filePath, self.dir) {
			continue
		}
		for _, other := range runtimes {
			if other.pkg == self.pkg {
				continue
			}
			if underPackage(importPath, other.pkg) {
				t.Errorf("%s (%s) imports the %s runtime %q",
					filePath, strings.TrimSuffix(self.dir, "/"),
					strings.TrimSuffix(other.dir, "/"), importPath)
			}
		}
	}
}

// assertDriverContractIsPure keeps the public driver contract dependency-free:
// standard library plus github.com/google/uuid only. It applies to the contract
// package itself (files directly under driver/), not its sub-packages such as
// driver/drivertest, which is a testing-support package free to depend on
// testify and the contract.
func assertDriverContractIsPure(t *testing.T, filePath, importPath string) {
	t.Helper()
	rest, ok := strings.CutPrefix(filePath, "driver/")
	if !ok || strings.Contains(rest, "/") {
		return
	}
	if isStandardLibrary(importPath) || importPath == "github.com/google/uuid" {
		return
	}
	t.Errorf("%s (driver contract) imports %q; only the standard library and github.com/google/uuid are allowed", filePath, importPath)
}

// underPackage reports whether importPath is pkg or a subpackage of pkg.
func underPackage(importPath, pkg string) bool {
	return importPath == pkg || strings.HasPrefix(importPath, pkg+"/")
}

// isStandardLibrary reports whether an import path is from the standard library,
// identified by a first path segment with no dot (no domain).
func isStandardLibrary(importPath string) bool {
	first, _, _ := strings.Cut(importPath, "/")
	return !strings.Contains(first, ".")
}
