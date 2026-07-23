module github.com/kausys/azync/examples

go 1.26.5

require (
	github.com/kausys/azync v0.0.1
	github.com/kausys/azync/driver/azyncpgx v0.0.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/go-logr/logr v1.4.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/pgx/v5 v5.10.0 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/mfridman/interpolate v0.0.2 // indirect
	github.com/pressly/goose/v3 v3.27.3 // indirect
	github.com/robfig/cron/v3 v3.0.1 // indirect
	github.com/sethvargo/go-retry v0.4.0 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel v1.44.0 // indirect
	go.opentelemetry.io/otel/metric v1.44.0 // indirect
	go.opentelemetry.io/otel/trace v1.44.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/text v0.40.0 // indirect
)

// The root module and the pg driver are unpublished, so resolve them from the
// local tree. This is permanent here (examples are never published as a
// dependency): it lets `go mod tidy` succeed with GOWORK=off (as CI's tidy
// check runs it) without a network lookup, and keeps go.sum free of entries
// for either. The go.work workspace resolves them the same way for ordinary
// builds.
replace github.com/kausys/azync => ../

replace github.com/kausys/azync/driver/azyncpgx => ../driver/azyncpgx
