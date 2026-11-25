#!/usr/bin/env bash
# Fail when a test in the TRX reports skipped/inconclusive, unless EXPECTED_SKIPS names it.
# EXPECTED_SKIPS is a comma-separated list of <simple class name>.<method>, or
# <simple class name>.* for every test of a class. The result publisher fails only on
# test failures, so a suite whose environment is missing would otherwise pass with
# every test skipped.
#
# Usage: check-skips.sh <results directory with *.trx>
set -euo pipefail

reports="${1:?results directory is required}"

python3 -I - "$reports" "${EXPECTED_SKIPS:-}" <<'EOF'
import glob
import os
import sys
import xml.etree.ElementTree as ET

reports, expected = sys.argv[1], {name for name in sys.argv[2].split(",") if name}
files = sorted(glob.glob(os.path.join(reports, "**", "*.trx"), recursive=True))
if not files:
    print(f"::error::No TRX reports in {reports}")
    sys.exit(1)

# MSTest TRX namespace
NS = {"t": "http://microsoft.com/schemas/VisualStudio/TeamTest/2010"}
total = 0
unexpected = []

for path in files:
    root = ET.parse(path).getroot()
    for unit in root.findall(".//t:UnitTestResult", NS):
        outcome = unit.get("outcome", "")
        # Passed / Failed / Inconclusive / NotExecuted / Timeout / ...
        total += 1
        if outcome not in ("Inconclusive", "NotExecuted"):
            continue
        test_name = unit.get("testName", "")
        # testName is often Namespace.Class.Method or Class.Method; MTP may emit method only.
        parts = test_name.split(".")
        if len(parts) >= 2:
            cls, method = parts[-2], parts[-1]
        else:
            cls, method = test_name, "*"
        name = f"{cls}.{method}"
        matched = (
            name in expected
            or f"{cls}.*" in expected
            or test_name in expected
            or any(e.endswith("." + test_name) for e in expected)
            or (method != "*" and any(e.endswith("." + method) or e == method for e in expected))
        )
        if matched:
            print(f"Expected skip: {name}")
        else:
            print(f"::error::{name} skipped (outcome={outcome})")
            unexpected.append(name)

if total == 0:
    print(f"::error::No tests in {reports}")
    sys.exit(1)
if unexpected:
    print(f"{len(unexpected)} of {total} tests skipped unexpectedly")
    sys.exit(1)
print(f"{total} tests, no unexpected skips")
EOF
