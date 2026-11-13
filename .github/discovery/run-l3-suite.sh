#!/usr/bin/env bash
# Run on the runner after setup-kind-ako and, in the TLS job, enable-l3-tls.sh.
# discovery-k8s.yaml injects DISCOVERY_* , L3_SEED and AEROSPIKE_ADMIN_PASSWORD.
# Suite.AssemblyInit skips shared-client connect when DISCOVERY_KUBECONFIG is set so
# K8s tests build their own clients. L3_SUITE selects the test filter.
set -euo pipefail

host="${DISCOVERY_ENDPOINT_HOSTNAME:?DISCOVERY_ENDPOINT_HOSTNAME is required}"
ingress_ports="${DISCOVERY_INGRESS_PORTS:?DISCOVERY_INGRESS_PORTS is required}"
node_ports="${DISCOVERY_SERVICE_NODE_PORTS:?DISCOVERY_SERVICE_NODE_PORTS is required}"
password="${AEROSPIKE_ADMIN_PASSWORD:?AEROSPIKE_ADMIN_PASSWORD is required}"
kubeconfig="${DISCOVERY_KUBECONFIG:?DISCOVERY_KUBECONFIG is required}"

[[ "$host" =~ ^[A-Za-z0-9.-]+$ ]] || {
  echo "Refusing unexpected endpoint hostname: ${host}" >&2
  exit 1
}
[[ "$password" =~ ^[A-Za-z0-9]+$ ]] || {
  echo "Refusing unexpected admin password characters" >&2
  exit 1
}
[[ -f "$kubeconfig" ]] || {
  echo "kubeconfig does not exist: ${kubeconfig}" >&2
  exit 1
}

# Same OR shape as run-l2-suite: FullyQualifiedName~A|FullyQualifiedName~B.
# MTP rejects the grouped form FullyQualifiedName~(A|B) (zero tests).
filter="${L3_SUITE:-FullyQualifiedName~TestK8sPassThrough|FullyQualifiedName~TestK8sScale}"
[[ "$filter" =~ ^[A-Za-z0-9~=_.()|-]+$ ]] || {
  echo "Refusing unexpected filter: ${filter}" >&2
  exit 1
}

if [[ -n "${DISCOVERY_TLS_CA_FILE:-}${DISCOVERY_TLS_NAME:-}" ]]; then
  [[ -f "${DISCOVERY_TLS_CA_FILE:-}" ]] || {
    echo "DISCOVERY_TLS_CA_FILE does not exist: ${DISCOVERY_TLS_CA_FILE:-}" >&2
    exit 1
  }
  [[ "${DISCOVERY_TLS_NAME:-}" =~ ^[A-Za-z0-9.-]+$ ]] || {
    echo "Refusing unexpected tls-name: ${DISCOVERY_TLS_NAME:-}" >&2
    exit 1
  }
  ca_name="discovery-l3-ca.crt"
  sudo cp "${DISCOVERY_TLS_CA_FILE}" "/usr/local/share/ca-certificates/${ca_name}"
  sudo update-ca-certificates >/dev/null 2>&1 || true
fi

check_ports() {
  local label=$1
  local ports=$2
  local port
  local -a port_list
  IFS=',' read -r -a port_list <<< "$ports"
  if ((${#port_list[@]} == 0)); then
    echo "${label} is empty" >&2
    exit 1
  fi
  for port in "${port_list[@]}"; do
    [[ "$port" =~ ^[0-9]+$ ]] || {
      echo "Refusing unexpected ${label} port: ${port}" >&2
      exit 1
    }
    echo "Checking TCP ${host}:${port}"
    timeout 10 bash -c "echo >/dev/tcp/${host}/${port}"
  done
}

check_ports "ingress" "$ingress_ports"
check_ports "service node" "$node_ports"

seed_port="${ingress_ports%%,*}"
export KUBECONFIG="$kubeconfig"
export L3_SEED="${L3_SEED:-${host}:${seed_port}}"

runsettings="$(mktemp /tmp/l3-runsettings.XXXXXX.xml)"
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
    <Parameter name="User" value="admin" />
    <Parameter name="Password" value="${password}" />
    <Parameter name="Timeout" value="25000" />
    <Parameter name="UseServicesAlternate" value="false" />
    <Parameter name="TlsEnable" value="false" />
    <Parameter name="TlsName" value="" />
    <Parameter name="TlsProtocols" value="" />
    <Parameter name="TlsRevoke" value="" />
    <Parameter name="TlsClientCertFile" value="" />
    <Parameter name="TlsLoginOnly" value="false" />
    <Parameter name="AuthMode" value="INTERNAL" />
  </TestRunParameters>
</RunSettings>
EOF

echo "Running L3 suite filter=${filter} seed=${L3_SEED}"
dotnet restore AerospikeTest/AerospikeTest.csproj
dotnet build AerospikeTest/AerospikeTest.csproj -c Release --no-restore
set +e
# MSTest.Sdk / Microsoft.Testing.Platform: pass filter after -- as well (same as run-l2-suite).
dotnet test AerospikeTest/AerospikeTest.csproj \
  -c Release \
  --no-build \
  --settings "${runsettings}" \
  --filter "${filter}" \
  --logger "trx;LogFileName=l3-k8s.trx" \
  --logger "console;verbosity=detailed" \
  --results-directory TestResults \
  -- \
  --filter "${filter}"
status=$?
set -e
rm -f "${runsettings}"
if [[ "${status}" -ne 0 ]]; then
  echo "::group::L3 test failure details"
  find TestResults AerospikeTest/bin -type f \( -name '*.log' -o -name '*.trx' -o -name '*.diag' \) 2>/dev/null \
    | while read -r f; do
        echo "===== ${f} ====="
        head -n 400 "${f}" || true
      done
  echo "::endgroup::"
fi
exit "${status}"
