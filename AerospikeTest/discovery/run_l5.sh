#!/usr/bin/env bash
#
# One L5 row of the aerospike-client-conformance l5-aws harness (l5-aws/README.md there), run by
# AerospikeTest/discovery/l5-aws.manifest as L5_RUN. L5_ROW selects the row's test class; the class
# reads the rest of the harness environment and is inconclusive when none are set.
#
# Required:
#   L5_ROW                    W-1, W-2, W-3 or W-5
#
# Usage: L5_ROW=W-1 AerospikeTest/discovery/run_l5.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

die() {
	echo "ERROR: $*" >&2
	exit 2
}

case "${L5_ROW:-}" in
	W-1) filter='FullyQualifiedName~TestL5EndpointShapes' ;;
	W-2) filter='FullyQualifiedName~TestL5SpotInterruptAndRollout' ;;
	W-3) filter='FullyQualifiedName~TestL5ScaleOutBeforeListener' ;;
	W-5) filter='FullyQualifiedName~TestL5AzOutage' ;;
	*) die "L5_ROW must be W-1, W-2, W-3 or W-5 (got: ${L5_ROW:-})" ;;
esac

[[ "$filter" =~ ^[A-Za-z0-9~=_.|-]+$ ]] || die "Refusing unexpected filter: ${filter}"

echo "Running filter=${filter} for row ${L5_ROW}"
cd "$ROOT_DIR"
# MSTest.Sdk / Microsoft.Testing.Platform: pass filter after -- as well.
dotnet test AerospikeTest/AerospikeTest.csproj \
	-c Release \
	--no-restore \
	--filter "${filter}" \
	--logger "console;verbosity=detailed" \
	--results-directory TestResults \
	-- \
	--filter "${filter}"
