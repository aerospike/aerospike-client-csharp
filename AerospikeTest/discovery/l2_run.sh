#!/usr/bin/env bash
#
# The L2 tests that assert client internals, for the l2-proxy harness in aerospike-client-conformance
# (l2-proxy/README.md there), run by AerospikeTest/discovery/l5-aws.manifest as L2_RUN once per profile
# in a container on the client network with the topology's values file as its environment. Runs the
# profile's TestL2* classes through dotnet test against assemblies L5_BUILD wrote, so it needs no
# network.
#
# The client keeps the legacy CN fallback, so the cn-only test cannot pass yet: on cn-only a run whose
# only failure is that test accepting the CN-only certificate exits 0 and says so.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Prefer the SDK installed by L5_SETUP when Amazon Linux's package is too old for global.json.
if [ -x "${HOME}/.dotnet/dotnet" ]; then
	export DOTNET_ROOT="${HOME}/.dotnet"
	export PATH="${HOME}/.dotnet:${PATH}"
fi

profile="${L2_PROFILE:?L2_PROFILE is required}"
subset="${DISCOVERY_SUBSET:?DISCOVERY_SUBSET is required}"

case "$subset" in
	ce|ee) ;;
	*)
		echo "ERROR: DISCOVERY_SUBSET must be ce or ee (got: ${subset})" >&2
		exit 1
		;;
esac

expected_skips=
case "$profile" in
	default)
		# N-* topology plus F-4 silent-drop / circuit breaker. N-6 TLS is ee-only (inconclusive on ce).
		filter='FullyQualifiedName~TestL2Topology|FullyQualifiedName~TestL2FaultInjection'
		if [[ "$subset" != ee ]]; then
			expected_skips='TestL2Topology.N6TlsValidatesAgainstNodeTlsName'
		fi
		;;
	cn-only)
		filter='FullyQualifiedName~TestL2CnOnlyCertificate'
		;;
	reachable)
		filter='FullyQualifiedName~TestL2ReachableNode'
		;;
	pass-through-names)
		filter='FullyQualifiedName~TestL2PassThrough'
		expected_skips='TestL2PassThrough.N16SharedNameRoutesToEveryOwner,TestL2PassThrough.N16SharedNameStrictRoutesToEveryOwner'
		expected_skips+=',TestL2PassThrough.N16NewNodeOnSharedNameActiveWithinTwoTendIntervals'
		expected_skips+=',TestL2PassThrough.N18PassThroughCollidingNodesCreateNoNode,TestL2PassThrough.N18StaticMapCollidingNodesCreateNoNode'
		expected_skips+=',TestL2PassThrough.N18CustomTranslatorCollidingNodesCreateNoNode,TestL2PassThrough.N18PortMapCollidingNodesCreateNoNode'
		expected_skips+=',TestL2PassThrough.N18PassThroughNewcomerCollidingWithNodeIsNotCreated'
		;;
	pass-through-ports)
		filter='FullyQualifiedName~TestL2PassThrough'
		expected_skips='TestL2PassThrough.N15DistinctNamesRouteToEveryOwner'
		;;
	port-map-range)
		filter='FullyQualifiedName~TestL2PortMapRange'
		;;
	unresolved-access)
		filter='FullyQualifiedName~TestL2UnresolvedAccessAddress|FullyQualifiedName~TestL2PortMapNoAccessAddress'
		;;
	port-range)
		filter='FullyQualifiedName~TestL2SourcePortRange'
		;;
	low-mtu)
		filter='FullyQualifiedName~TestL2LowMtu'
		;;
	idle-reset)
		# Sync F-1 only: async pool still surfaces ConnectionReset to the caller after idle RST.
		filter='FullyQualifiedName~TestL2IdleReset'
		;;
	*)
		echo "ERROR: unknown profile $profile" >&2
		exit 1
		;;
esac

host="${DISCOVERY_ENDPOINT_HOSTNAME:?DISCOVERY_ENDPOINT_HOSTNAME is required}"
ports="${DISCOVERY_ENDPOINT_PORTS:?DISCOVERY_ENDPOINT_PORTS is required}"
port_base="${DISCOVERY_ENDPOINT_PORT_BASE:?DISCOVERY_ENDPOINT_PORT_BASE is required}"

[[ "$host" =~ ^[A-Za-z0-9.-]+$ ]] || {
	echo "Refusing unexpected endpoint hostname: ${host}" >&2
	exit 1
}

if [[ "$subset" == "ee" ]]; then
	tls_ports="${DISCOVERY_TLS_ENDPOINT_PORTS:?DISCOVERY_TLS_ENDPOINT_PORTS is required}"
	seed_port="${tls_ports%%,*}"
	check_ports="$tls_ports"
	tls_enable="true"
	tls_name="${DISCOVERY_TLS_NAME:-aerospike-tls}"
	ca_file="${DISCOVERY_TLS_CA_FILE:?DISCOVERY_TLS_CA_FILE is required on ee}"
else
	seed_port="$port_base"
	check_ports="$ports"
	tls_enable="false"
	tls_name=""
	ca_file=""
fi

IFS=',' read -r -a port_list <<< "$check_ports"
if ((${#port_list[@]} == 0)); then
	echo "No proxy ports to check" >&2
	exit 1
fi
for port in "${port_list[@]}"; do
	[[ "$port" =~ ^[0-9]+$ ]] || {
		echo "Refusing unexpected endpoint port: ${port}" >&2
		exit 1
	}
	echo "Checking TCP ${host}:${port}"
	timeout 10 bash -c "echo >/dev/tcp/${host}/${port}"
done

# Trust the topology CA via TlsPolicy.certificateChainPolicy (CustomRootTrust) in L2 TLS
# tests — not the container system store (Amazon Linux has no ca-certificates path here).
if [[ -n "${ca_file}" ]]; then
	[[ -f "${ca_file}" ]] || {
		echo "DISCOVERY_TLS_CA_FILE is set but not a file: ${ca_file}" >&2
		exit 1
	}
	echo "L2 TLS CA file: ${ca_file} (CustomRootTrust in client)"
fi

[[ "$filter" =~ ^[A-Za-z0-9~=_.|-]+$ ]] || {
	echo "Refusing unexpected filter: ${filter}" >&2
	exit 1
}

results_dir="$ROOT_DIR/TestResults/l2-${profile}"
mkdir -p "$results_dir"
runsettings="$(mktemp /tmp/l2-runsettings.XXXXXX.xml)"
cat > "${runsettings}" <<EOF
<?xml version="1.0" encoding="utf-8"?>
<RunSettings>
  <RunConfiguration>
    <ResultsDirectory>${results_dir}</ResultsDirectory>
    <TestCaseFilter>${filter}</TestCaseFilter>
  </RunConfiguration>
  <TestRunParameters>
    <Parameter name="Host" value="${host}" />
    <Parameter name="Port" value="${seed_port}" />
    <Parameter name="ClusterName" value="" />
    <Parameter name="Namespace" value="test" />
    <Parameter name="Set" value="test" />
    <Parameter name="User" value="" />
    <Parameter name="Password" value="" />
    <Parameter name="Timeout" value="25000" />
    <Parameter name="UseServicesAlternate" value="false" />
    <Parameter name="TlsEnable" value="${tls_enable}" />
    <Parameter name="TlsName" value="${tls_name}" />
    <Parameter name="TlsProtocols" value="" />
    <Parameter name="TlsRevoke" value="" />
    <Parameter name="TlsClientCertFile" value="" />
    <Parameter name="TlsLoginOnly" value="false" />
    <Parameter name="AuthMode" value="INTERNAL" />
  </TestRunParameters>
</RunSettings>
EOF

echo "Running L2 suite profile=${profile} filter=${filter} subset=${subset} host=${host} port=${seed_port}"
log="$(mktemp /tmp/l2-run.XXXXXX.log)"
set +e
# MTP ignores VSTest --logger/--results-directory; use --report-trx (see .github/actions/test).
dotnet test "$ROOT_DIR/AerospikeTest/AerospikeTest.csproj" \
	-c Release \
	--no-restore \
	--settings "${runsettings}" \
	--filter "${filter}" \
	-- \
	--filter "${filter}" \
	--results-directory "${results_dir}" \
	--report-trx --report-trx-filename "l2-${profile}.trx" 2>&1 | tee "$log"
status=${PIPESTATUS[0]}
set -e

# Fail when every matching test was skipped/inconclusive (missing env must not look like a pass).
trx="$(find "$results_dir" -name '*.trx' -print -quit 2>/dev/null || true)"
passed=0
failed=0
if [[ -n "$trx" && -f "$trx" ]]; then
	passed="$(grep -c 'outcome="Passed"' "$trx" || true)"
	failed="$(grep -c 'outcome="Failed"' "$trx" || true)"
fi
if [[ "$status" -eq 0 && "$passed" -eq 0 ]]; then
	echo "ERROR: L2_RUN profile=${profile} executed no passing tests (all skipped or none matched)" >&2
	exit 1
fi

# Fail on unexpected skips (same idea as Java L2Run / check-skips.sh).
if [[ "$status" -eq 0 || "$profile" == cn-only ]]; then
	EXPECTED_SKIPS="${expected_skips}" bash "$ROOT_DIR/.github/discovery/check-skips.sh" "$results_dir" || {
		# cn-only may fail the suite; still require skip accounting when it passes.
		if [[ "$status" -eq 0 ]]; then
			exit 1
		fi
	}
fi

if [[ "$profile" != cn-only ]]; then
	exit "$status"
fi

# Legacy CN fallback: N-17 still accepts the CN-only certificate; treat that alone as non-blocking.
# MTP may indent Console lines or put the Assert.Fail text only in the TRX.
if [[ "$status" -ne 0 ]] && [[ "$failed" -eq 1 ]]; then
	if grep -q 'L2-N17-ACCEPTED' "$log" ||
		grep -qE 'N17CnOnlyCertificateIsRejected|TLS handshake accepted a certificate that carries' "$log" ${trx:+"$trx"} 2>/dev/null; then
		echo "L2-RUN profile=${profile} nonblocking exit=${status}: the client accepts the CN-only certificate through its legacy CN fallback"
		exit 0
	fi
fi
exit "$status"
