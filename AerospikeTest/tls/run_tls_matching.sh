#!/usr/bin/env bash
#
# Server certificate name matching against a real Enterprise server.
#
# Cases come from aerospike-client-conformance tls/name-matching.yaml (Mirza CI guide §5):
# one certificate per shapes entry, fixture rows as the case list, blocked rows skipped.
# csharp-declared.yaml rows with result=fail are known L1 divergences (legacy CN fallback).
# Integration still asserts the fixture expect; a failing row that is declared fail does not
# fail the suite. An extra Testing Strategy N-6 proxy case (n6_proxy) follows the fixture rows.
#
# Required environment:
#   AEROSPIKE_IMAGE             Enterprise server image
#   AEROSPIKE_FEATURES_FILE     feature-key file, or
#   AEROSPIKE_FEATURES_CONTENT  feature-key file content
#   FIXTURES_DIRECTORY          aerospike-client-conformance checkout (fixture set 1.1.0+)
# Optional:
#   DECLARATIONS_FILE           defaults to AerospikeTest/conformance/csharp-declared.yaml
#   TLS_MATCHING_PORT           host port for the container TLS port (default 4433)
#   TLS_MATCHING_SKIP_BUILD     set to 1 to skip building the client first
#
# Usage: AerospikeTest/tls/run_tls_matching.sh [case ...]   (no arguments runs every case)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ROOT_DIR="$(cd "$TEST_DIR/.." && pwd)"
WORK_DIR="$TEST_DIR/bin/tls-matching"
PORT="${TLS_MATCHING_PORT:-4433}"
PREFIX="tls-matching-$$"
LOADER="$SCRIPT_DIR/load_name_matching_cases.py"
DECLARATIONS_FILE="${DECLARATIONS_FILE:-$TEST_DIR/conformance/csharp-declared.yaml}"

die() {
	echo "ERROR: $*" >&2
	exit 2
}

command -v docker >/dev/null 2>&1 || die "docker CLI not found"
docker info >/dev/null 2>&1 || die "docker daemon not reachable"
command -v openssl >/dev/null 2>&1 || die "openssl not found"
command -v python3 >/dev/null 2>&1 || die "python3 is required to load tls/name-matching.yaml"
[ -n "${AEROSPIKE_IMAGE:-}" ] || die "AEROSPIKE_IMAGE is not set"
[ -n "${FIXTURES_DIRECTORY:-}" ] || die "FIXTURES_DIRECTORY is not set (aerospike-client-conformance checkout)"
[ -f "$FIXTURES_DIRECTORY/tls/name-matching.yaml" ] || die "tls/name-matching.yaml not found under FIXTURES_DIRECTORY"
[ -f "$DECLARATIONS_FILE" ] || die "declarations file not found: $DECLARATIONS_FILE"
[ -f "$LOADER" ] || die "case loader not found: $LOADER"

if [ -n "${AEROSPIKE_FEATURES_FILE:-}" ]; then
	[ -z "${AEROSPIKE_FEATURES_CONTENT:-}" ] || die "set only one of AEROSPIKE_FEATURES_FILE and AEROSPIKE_FEATURES_CONTENT"
	[ -r "$AEROSPIKE_FEATURES_FILE" ] || die "AEROSPIKE_FEATURES_FILE '$AEROSPIKE_FEATURES_FILE' is not readable"
elif [ -z "${AEROSPIKE_FEATURES_CONTENT:-}" ]; then
	die "AEROSPIKE_FEATURES_FILE or AEROSPIKE_FEATURES_CONTENT must be set"
fi

STARTED=()
STARTED_NETWORKS=()

cleanup() {
	for c in "${STARTED[@]+"${STARTED[@]}"}"; do
		docker rm -f "$c" >/dev/null 2>&1 || true
	done
	for n in "${STARTED_NETWORKS[@]+"${STARTED_NETWORKS[@]}"}"; do
		docker network rm "$n" >/dev/null 2>&1 || true
	done
	[ -z "${AEROSPIKE_FEATURES_CONTENT:-}" ] || rm -f "$WORK_DIR/features.conf"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

# Load fixture rows: case shape tlsName subjectCN sanSpec expect declared
mapfile -t FIXTURE_ROWS < <(python3 "$LOADER" --fixtures "$FIXTURES_DIRECTORY" --declarations "$DECLARATIONS_FILE")
[ "${#FIXTURE_ROWS[@]}" -gt 0 ] || die "no runnable rows from tls/name-matching.yaml"

# name|tls-name on both nodes|subject CN|subjectAltName — Testing Strategy N-6, not a fixture row.
PROXY_CASES=(
	"n6_proxy|cluster.example.com|server-cn|DNS:cluster.example.com"
)

if [ $# -gt 0 ]; then
	SELECTED=()
	SELECTED_PROXY=()
	for want in "$@"; do
		found=0
		for row in "${FIXTURE_ROWS[@]}"; do
			IFS=$'\t' read -r case_name _ <<< "$row"
			if [ "$case_name" = "$want" ]; then
				SELECTED+=("$row")
				found=1
			fi
		done
		for c in "${PROXY_CASES[@]}"; do
			if [ "${c%%|*}" = "$want" ]; then
				SELECTED_PROXY+=("$c")
				found=1
			fi
		done
		[ $found = 1 ] || die "unknown case '$want'"
	done
	FIXTURE_ROWS=("${SELECTED[@]+"${SELECTED[@]}"}")
	PROXY_CASES=("${SELECTED_PROXY[@]+"${SELECTED_PROXY[@]}"}")
fi

if [ "${TLS_MATCHING_SKIP_BUILD:-0}" != "1" ]; then
	echo "Building AerospikeClient and AerospikeTest"
	(cd "$ROOT_DIR" && dotnet build AerospikeTest/AerospikeTest.csproj -c Release --nologo -v q) || die "client build failed"
fi

rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR"

if [ -n "${AEROSPIKE_FEATURES_FILE:-}" ]; then
	FEATURES="$(cd "$(dirname "$AEROSPIKE_FEATURES_FILE")" && pwd)/$(basename "$AEROSPIKE_FEATURES_FILE")"
else
	FEATURES="$WORK_DIR/features.conf"
	printf '%s\n' "$AEROSPIKE_FEATURES_CONTENT" > "$FEATURES"
	chmod 644 "$FEATURES"
fi

# Cache generated certificates by shape name.
declare -A SHAPE_DIR=()

gen_certs() {
	local dir="$1" cn="$2" san="$3"
	local ext=()

	openssl req -x509 -newkey rsa:4096 -nodes \
		-keyout "$dir/ca.key" -out "$dir/ca.crt" -days 365 \
		-subj "/CN=Aerospike-Server-Test-CA" 2>/dev/null

	if [ "$san" != "-" ]; then
		printf '[v3_req]\nsubjectAltName = %s\n' "$san" > "$dir/san.ext"
		ext=(-extensions v3_req -extfile "$dir/san.ext")
	fi

	openssl req -newkey rsa:4096 -nodes \
		-keyout "$dir/server.key" -out "$dir/server.csr" \
		-subj "/CN=$cn" 2>/dev/null
	openssl x509 -req -in "$dir/server.csr" \
		-CA "$dir/ca.crt" -CAkey "$dir/ca.key" -CAcreateserial \
		-out "$dir/server.crt" -days 365 "${ext[@]+"${ext[@]}"}" 2>/dev/null

	rm -f "$dir"/*.csr "$dir"/*.srl "$dir/san.ext"
	chmod 644 "$dir"/*
}

ensure_shape() {
	local shape="$1" cn="$2" san="$3"
	local dir="$WORK_DIR/shapes/$shape"

	if [ -n "${SHAPE_DIR[$shape]+x}" ]; then
		echo "${SHAPE_DIR[$shape]}"
		return 0
	fi

	mkdir -p "$dir"
	gen_certs "$dir" "$cn" "$san"
	# Server tls-name must be the CN or a dNSName on the cert; every fixture shape has a CN.
	write_conf "$dir" "$cn"
	SHAPE_DIR[$shape]="$dir"
	echo "$dir"
}

write_conf() {
	local dir="$1" server_tls_name="$2" conf="${3:-aerospike.conf}" node_id="${4:-}" mesh_seed="${5:-}"
	cat > "$dir/$conf" <<EOF
service {
    feature-key-file /etc/aerospike/features.conf
    cluster-name docker
    proto-fd-max 15000
$([ -n "$node_id" ] && echo "    node-id $node_id")
}

logging {
    console {
        context any info
    }
}

network {
    tls $server_tls_name {
        cert-file /etc/aerospike/tls/server.crt
        key-file  /etc/aerospike/tls/server.key
        ca-file   /etc/aerospike/tls/ca.crt
    }
    service {
        address any
        port 3000
        tls-port 4333
        tls-authenticate-client false
        tls-name $server_tls_name
    }
    heartbeat {
        mode mesh
        port 3002
$([ -n "$mesh_seed" ] && echo "        mesh-seed-address-port $mesh_seed 3002")
    }
    fabric {
        port 3001
    }
}

namespace test {
    replication-factor 1
    storage-engine memory {
        data-size 1G
    }
}
EOF
}

wait_ready() {
	local name="$1"
	local i

	for i in $(seq 1 60); do
		if [ "$(docker inspect -f '{{.State.Running}}' "$name" 2>/dev/null)" != "true" ]; then
			docker logs "$name" 2>&1 | tail -30 >&2
			die "container $name exited"
		fi
		if docker logs "$name" 2>&1 | grep "rebalanced:" >/dev/null; then
			sleep 1
			return 0
		fi
		sleep 1
	done
	docker logs "$name" 2>&1 | tail -30 >&2
	die "container $name not ready after 60s"
}

wait_cluster_size() {
	local name="$1" size="$2"
	local i

	for i in $(seq 1 60); do
		if docker logs "$name" 2>&1 | grep "CLUSTER-SIZE $size" >/dev/null; then
			sleep 2
			return 0
		fi
		sleep 1
	done
	docker logs "$name" 2>&1 | tail -30 >&2
	die "container $name did not reach cluster size $size after 60s"
}

report_case() {
	local name="$1" expect="$2" rc="$3" log="$4" note="${5:-}"

	grep -E "TLS-RESULT|TLS-PROXY|CLIENT-LOG WARN" "$log" | sed 's/^/  /' || true
	grep -E "Passed!|Failed!|failed|error" "$log" | tail -5 | sed 's/^/  /' || true

	if [ "$rc" -eq 0 ]; then
		status=PASS
	else
		status=FAIL
		FAILED=$((FAILED + 1))
	fi
	echo "CASE-STATUS $name $status${note:+ ($note)}"
	SUMMARY+=("$(printf '%-40s %-6s %-6s %s' "$name" "$expect" "$status" "$note")")
}

SUMMARY=()
FAILED=0

for row in "${FIXTURE_ROWS[@]+"${FIXTURE_ROWS[@]}"}"; do
	IFS=$'\t' read -r name shape tls_name cn san fixture_expect declared <<< "$row"

	expect="$fixture_expect"
	warning=false
	note=""
	if [ "$declared" != "pass" ] && [ "$declared" != "fail" ]; then
		die "unsupported declaration result '$declared' for case $name"
	fi

	shape_dir="$(ensure_shape "$shape" "$cn" "$san")"
	dir="$WORK_DIR/cases/$name"
	mkdir -p "$dir"
	# Reuse the shape cert/conf; keep a per-case log directory.
	cp -a "$shape_dir/." "$dir/"

	echo
	echo "=== CASE $name (shape=$shape)"
	echo "certificate:"
	openssl x509 -in "$dir/server.crt" -noout -subject -ext subjectAltName 2>&1 | sed 's/^/  /'
	echo "tlsName: $tls_name"
	echo "fixture expect: $expect; declared: $declared"

	container="$PREFIX-$name"
	docker run -d --name "$container" -p "127.0.0.1:$PORT:4333" --ulimit nofile=15000 \
		-v "$dir:/etc/aerospike/tls:ro" \
		-v "$FEATURES:/etc/aerospike/features.conf:ro" \
		--entrypoint asd "$AEROSPIKE_IMAGE" --foreground --config-file /etc/aerospike/tls/aerospike.conf >/dev/null
	STARTED+=("$container")
	wait_ready "$container"

	sudo cp "$dir/ca.crt" /usr/local/share/ca-certificates/aerospike-tls-matching.crt
	sudo update-ca-certificates >/dev/null 2>&1 || true

	log="$dir/dotnet.log"
	set +e
	(
		cd "$ROOT_DIR"
		export TLS_CASE="$name" TLS_NAME="$tls_name" TLS_EXPECT="$expect" TLS_WARNING="$warning"
		export TLS_BOOTSTRAP="" TLS_REVOKE="" TLS_CA="$dir/ca.crt"
		export TLS_HOST=127.0.0.1 TLS_PORT="$PORT" TLS_NAMESPACE=test
		filter='FullyQualifiedName~TestTlsMatching'
		dotnet test AerospikeTest/AerospikeTest.csproj -c Release --no-build \
			--filter "$filter" --logger "console;verbosity=normal" -- \
			--filter "$filter"
	) > "$log" 2>&1
	rc=$?
	set -e

	if [ "$declared" = "fail" ]; then
		if [ "$rc" -ne 0 ]; then
			note="declared-fail still diverges"
			rc=0
		else
			note="declared-fail now matches fixture"
		fi
	fi

	report_case "$name" "$expect" "$rc" "$log" "$note"

	docker rm -f "$container" >/dev/null 2>&1 || true
done

for c in "${PROXY_CASES[@]+"${PROXY_CASES[@]}"}"; do
	IFS='|' read -r name tls_name cn san <<< "$c"
	dir="$WORK_DIR/cases/$name"
	mkdir -p "$dir"

	gen_certs "$dir" "$cn" "$san"

	network="$PREFIX-$name"
	node_a="$PREFIX-$name-a"
	node_b="$PREFIX-$name-b"
	port_b=$((PORT + 1))
	write_conf "$dir" "$tls_name" aerospike-a.conf A1 "$node_b"
	write_conf "$dir" "$tls_name" aerospike-b.conf B2 "$node_a"

	echo
	echo "=== CASE $name"
	echo "certificate:"
	openssl x509 -in "$dir/server.crt" -noout -subject -ext subjectAltName 2>&1 | sed 's/^/  /'
	echo "tls-name on both nodes: $tls_name (seed node A1 only; B2 learned from peers-*)"
	echo "expected: accept through a TCP pass-through listener per node, no deprecation warning"

	docker network create "$network" >/dev/null
	STARTED_NETWORKS+=("$network")

	for spec in "$node_a|$PORT|aerospike-a.conf" "$node_b|$port_b|aerospike-b.conf"; do
		IFS='|' read -r container host_port conf <<< "$spec"
		docker run -d --name "$container" --network "$network" -p "127.0.0.1:$host_port:4333" --ulimit nofile=15000 \
			-v "$dir:/etc/aerospike/tls:ro" \
			-v "$FEATURES:/etc/aerospike/features.conf:ro" \
			--entrypoint asd "$AEROSPIKE_IMAGE" --foreground --config-file "/etc/aerospike/tls/$conf" >/dev/null
		STARTED+=("$container")
	done
	wait_cluster_size "$node_a" 2

	sudo cp "$dir/ca.crt" /usr/local/share/ca-certificates/aerospike-tls-matching.crt
	sudo update-ca-certificates >/dev/null 2>&1 || true

	log="$dir/dotnet.log"
	set +e
	(
		cd "$ROOT_DIR"
		export TLS_NAME="$tls_name" TLS_NODES="A1=$PORT,B2=$port_b" TLS_CA="$dir/ca.crt"
		export TLS_HOST=127.0.0.1 TLS_NAMESPACE=test
		filter='FullyQualifiedName~TestTlsProxy'
		dotnet test AerospikeTest/AerospikeTest.csproj -c Release --no-build \
			--filter "$filter" --logger "console;verbosity=normal" -- \
			--filter "$filter"
	) > "$log" 2>&1
	rc=$?
	set -e

	report_case "$name" accept "$rc" "$log" "N-6 proxy"

	docker rm -f "$node_a" "$node_b" >/dev/null 2>&1 || true
	docker network rm "$network" >/dev/null 2>&1 || true
done

echo
echo "=== SUMMARY (case, expected, status, note)"
for s in "${SUMMARY[@]}"; do
	echo "$s"
done
echo "cases: $((${#FIXTURE_ROWS[@]} + ${#PROXY_CASES[@]})) failed: $FAILED"

[ $FAILED -eq 0 ]
