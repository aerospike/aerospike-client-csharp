#!/usr/bin/env bash
# Run inside the L2 client container. The discovery workflow injects
# DISCOVERY_* and TOXIPROXY_*. DISCOVERY_SUBSET is ce or ee.
#
# Seeds a minimal .runsettings from the endpoint env (CE plain, EE TLS +
# AEROSPIKE_CA_CERT). Suite.AssemblyInit skips shared-client connect when
# DISCOVERY_ENDPOINT_HOSTNAME is set so each TestL2* builds its own policy.
set -euo pipefail

host="${DISCOVERY_ENDPOINT_HOSTNAME:?DISCOVERY_ENDPOINT_HOSTNAME is required}"
ports="${DISCOVERY_ENDPOINT_PORTS:?DISCOVERY_ENDPOINT_PORTS is required}"
port_base="${DISCOVERY_ENDPOINT_PORT_BASE:?DISCOVERY_ENDPOINT_PORT_BASE is required}"
subset="${DISCOVERY_SUBSET:?DISCOVERY_SUBSET is required}"

case "$subset" in
  ce|ee) ;;
  *)
    echo "DISCOVERY_SUBSET must be ce or ee (got: ${subset})" >&2
    exit 1
    ;;
esac

[[ "$host" =~ ^[A-Za-z0-9.-]+$ ]] || {
  echo "Refusing unexpected endpoint hostname: ${host}" >&2
  exit 1
}

if [[ "$subset" == "ee" ]]; then
  tls_ports="${DISCOVERY_TLS_ENDPOINT_PORTS:?DISCOVERY_TLS_ENDPOINT_PORTS is required}"
  seed_port="${tls_ports%%,*}"
  check_ports="$tls_ports"
  tls_enable="true"
  tls_name="aerospike-tls"
else
  seed_port="$port_base"
  check_ports="$ports"
  tls_enable="false"
  tls_name=""
fi

# A cluster that never came up must fail here, not look like a skipped suite.
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

filter="${L2_SUITE:-FullyQualifiedName~TestL2Topology}"
[[ "$filter" =~ ^[A-Za-z0-9~=_.-]+$ ]] || {
  echo "Refusing unexpected filter: ${filter}" >&2
  exit 1
}

runsettings="$(mktemp /tmp/l2-runsettings.XXXXXX.xml)"
cat > "${runsettings}" <<EOF
<?xml version="1.0" encoding="utf-8"?>
<RunSettings>
  <RunConfiguration>
    <ResultsDirectory>TestResults</ResultsDirectory>
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

echo "Running L2 suite filter=${filter} subset=${subset} host=${host} port=${seed_port}"
# MSTest.Sdk / Microsoft.Testing.Platform: pass filter after -- as well as RunSettings TestCaseFilter.
dotnet test AerospikeTest/AerospikeTest.csproj \
  -c Release \
  --no-restore \
  --settings "${runsettings}" \
  --filter "${filter}" \
  --logger "trx;LogFileName=l2-${subset}.trx" \
  --results-directory TestResults \
  -- \
  --filter "${filter}"
