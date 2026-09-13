#!/usr/bin/env bash
# Runs the cross-implementation contract tests against the real vcmp-js and vcmp-spring peers.
#
#   contract/run.sh            # both peers
#   contract/run.sh js         # only vcmp-js
#   contract/run.sh java       # only vcmp-spring
#
# Requirements: node + npm (js), a JDK + the sibling ../vcmp-spring checkout (java).
set -euo pipefail
cd "$(dirname "$0")/.."

what="${1:-all}"

run_js() {
	echo "==> building the vcmp-js peer"
	(cd contract/js && npm ci)
	echo "==> Rust <-> vcmp-js"
	cargo test --features "client server axum" --test contract_js -- --ignored --nocapture
}

run_java() {
	if [ ! -d ../vcmp-spring ]; then
		echo "!! ../vcmp-spring not found — skipping the Java contract tests" >&2
		return 0
	fi
	echo "==> building the vcmp-spring peer (installDist)"
	./contract/java/gradlew -p contract/java installDist
	echo "==> Rust <-> vcmp-spring"
	cargo test --features "client server axum" --test contract_java -- --ignored --nocapture
}

case "$what" in
	js) run_js ;;
	java) run_java ;;
	all) run_js; run_java ;;
	*) echo "usage: contract/run.sh [js|java|all]" >&2; exit 2 ;;
esac
