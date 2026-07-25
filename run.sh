#!/usr/bin/env bash
# run.sh — build/test/lint driver for azync. Replaces the Makefile.
#
# Usage: ./run.sh <command>
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

# Root module is "." — sub-modules are in their own directories.
ROOT_MODULE="."
SUB_MODULES=(driver/azyncpgx examples)
ALL_MODULES=("$ROOT_MODULE" "${SUB_MODULES[@]}")

# golangci-lint is installed locally (not globally) so `./run.sh lint` is
# reproducible for every contributor and CI runner without a pre-existing
# system install. The version must track the golangci-lint used in CI
# (.github/workflows/lint.yml, .github/workflows/release.yml) — keep the
# `version:` input on each golangci-lint-action step in those workflows equal
# to this value.
GOLANGCI_LINT_VERSION="v2.12.2"
BIN_DIR="$ROOT_DIR/bin"
GOLANGCI_LINT="$BIN_DIR/golangci-lint"

usage() {
	cat <<'EOF'
Usage: ./run.sh <command>

Commands:
  build             go build ./... in root + every sub-module
  test              go test -race ./... in root + every sub-module
  test-integration  go test ./... in driver/azyncpgx against AZYNC_INTEGRATION_DATABASE_URL
  lint              golangci-lint run in root + every sub-module
  lint-fix          golangci-lint run --fix in root + every sub-module
  fmt               gofmt -w in root + every sub-module
  tidy              go mod tidy in root + every sub-module
  check-tidy        go mod tidy + git diff --exit-code on go.mod/go.sum
  ci                build + test + lint + check-tidy
  db-up             docker compose up -d --wait
  db-down           docker compose down -v
EOF
}

# ensure_golangci_lint installs the pinned golangci-lint into ./bin (gitignored)
# if it is not already present, rather than relying on a global install.
ensure_golangci_lint() {
	if [[ -x "$GOLANGCI_LINT" ]]; then
		return
	fi
	echo "→ Installing golangci-lint ${GOLANGCI_LINT_VERSION} into ${BIN_DIR}"
	mkdir -p "$BIN_DIR"
	curl -sSfL https://golangci-lint.run/install.sh | sh -s -- -b "$BIN_DIR" "$GOLANGCI_LINT_VERSION"
}

cmd_build() {
	for m in "${ALL_MODULES[@]}"; do
		echo "→ Building $m"
		(cd "$m" && go build ./...)
	done
}

cmd_test() {
	for m in "${ALL_MODULES[@]}"; do
		echo "→ Testing $m"
		(cd "$m" && go test -race ./...)
	done
}

cmd_test_integration() {
	local db_url="${AZYNC_INTEGRATION_DATABASE_URL:-postgres://azync:azync@localhost:5432/azync?sslmode=disable}"
	echo "→ Integration testing driver/azyncpgx"
	(cd driver/azyncpgx && AZYNC_INTEGRATION_DATABASE_URL="$db_url" go test ./...)
}

cmd_lint() {
	ensure_golangci_lint
	for m in "${ALL_MODULES[@]}"; do
		echo "→ Linting $m"
		(cd "$m" && "$GOLANGCI_LINT" run --config "$ROOT_DIR/.golangci.yml" ./...)
	done
}

cmd_lint_fix() {
	ensure_golangci_lint
	for m in "${ALL_MODULES[@]}"; do
		echo "→ Lint-fixing $m"
		(cd "$m" && "$GOLANGCI_LINT" run --fix --config "$ROOT_DIR/.golangci.yml" ./...)
	done
}

cmd_fmt() {
	for m in "${ALL_MODULES[@]}"; do
		echo "→ Formatting $m"
		(cd "$m" && gofmt -w .)
	done
}

cmd_tidy() {
	for m in "${ALL_MODULES[@]}"; do
		echo "→ Tidying $m"
		(cd "$m" && go mod tidy)
	done
}

cmd_check_tidy() {
	for m in "${ALL_MODULES[@]}"; do
		echo "→ Checking tidy $m"
		(cd "$m" && go mod tidy && git diff --exit-code -- go.mod go.sum)
	done
}

cmd_ci() {
	cmd_build
	cmd_test
	cmd_lint
	cmd_check_tidy
}

cmd_db_up() {
	docker compose up -d --wait
}

cmd_db_down() {
	docker compose down -v
}

main() {
	if [[ $# -eq 0 ]]; then
		usage
		exit 1
	fi

	case "$1" in
	build) cmd_build ;;
	test) cmd_test ;;
	test-integration) cmd_test_integration ;;
	lint) cmd_lint ;;
	lint-fix) cmd_lint_fix ;;
	fmt) cmd_fmt ;;
	tidy) cmd_tidy ;;
	check-tidy) cmd_check_tidy ;;
	ci) cmd_ci ;;
	db-up) cmd_db_up ;;
	db-down) cmd_db_down ;;
	-h | --help | help) usage ;;
	*)
		echo "run.sh: unknown command: $1" >&2
		usage
		exit 1
		;;
	esac
}

main "$@"
