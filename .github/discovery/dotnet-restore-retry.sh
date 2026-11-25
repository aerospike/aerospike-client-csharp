#!/usr/bin/env bash
# Retry NuGet restore: nuget.org occasionally fails mid-download (404 / feed race).
set -euo pipefail

project="${1:-AerospikeTest/AerospikeTest.csproj}"
attempts="${2:-3}"

for i in $(seq 1 "${attempts}"); do
	if dotnet restore "${project}" --nologo -v q; then
		exit 0
	fi
	echo "nuget restore retry ${i}/${attempts} for ${project}" >&2
	if [[ "${i}" -eq "${attempts}" ]]; then
		exit 1
	fi
	sleep 5
done
