#!/usr/bin/env bash
#
# The L5 driver for the l5-aws harness in aerospike-client-conformance (l5-aws/DRIVER.md there),
# run by AerospikeTest/discovery/l5-aws.manifest as L5_DRIVER once L5_BUILD has compiled
# AerospikeL5Driver. Runs AerospikeL5Driver with this script's arguments.
#
# Usage: AerospikeTest/discovery/l5_driver.sh --seed HOST:PORT --shape pass-through|port-map ...

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
DRIVER="$ROOT_DIR/AerospikeL5Driver/bin/Release/net8.0/AerospikeL5Driver.dll"

# Prefer the SDK installed by L5_SETUP when Amazon Linux's package is too old for global.json.
if [ -x "${HOME}/.dotnet/dotnet" ]; then
	export DOTNET_ROOT="${HOME}/.dotnet"
	export PATH="${HOME}/.dotnet:${PATH}"
fi

if [ ! -f "$DRIVER" ]; then
	echo "ERROR: $DRIVER not found; run L5_BUILD first" >&2
	exit 1
fi

exec dotnet "$DRIVER" "$@"
