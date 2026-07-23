// Package azync provides durable background jobs and a CQRS event bus for Go,
// unified over a single job table with pluggable storage drivers.
//
// A [Core] is the shared root: it owns the storage driver, the resolved
// layered defaults and the logger. Open builds one from a DSN, resolving the
// driver from its scheme through a registry populated by a blank import (in
// the style of database/sql, see [RegisterDriver]); New wraps an
// already-constructed [driver.Store] directly. Neither migrates
// automatically — call [Core.Migrate] once the driver supports it.
//
// The queue and event runtimes each compose over a Core: their New shares one
// (so jobs and event deliveries live behind a single connection pool, schema
// and migrations table), or their own Open builds a private one. Every
// runtime setting resolves in layers — a runtime-specific option overrides a
// Core option, which overrides the built-in [Defaults] — so a queue- or
// event-only override never has to touch the shared Core.
package azync
