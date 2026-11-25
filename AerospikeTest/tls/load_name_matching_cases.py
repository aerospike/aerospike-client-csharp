#!/usr/bin/env python3
"""Emit tls/name-matching.yaml integration cases as TSV for run_tls_matching.sh.

One line per runnable row:
  case<TAB>shape<TAB>tlsName<TAB>subjectCN<TAB>sanSpec<TAB>expect<TAB>declared

sanSpec is openssl subjectAltName text, or "-" when the shape has no SAN.
declared is pass|fail|unsupported|blocked from csharp-declared.yaml (default pass).
Blocked fixture rows are omitted.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

try:
	import yaml
except ImportError:
	sys.stderr.write("PyYAML is required: pip install pyyaml\n")
	sys.exit(2)


def load_declarations(path: Path) -> dict[str, str]:
	"""Map case name -> result for tls/name-matching.yaml rows."""
	if not path.is_file():
		return {}
	doc = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
	rows = doc.get("tls/name-matching.yaml") or {}
	out: dict[str, str] = {}
	for case, meta in rows.items():
		if isinstance(meta, dict) and "result" in meta:
			out[str(case)] = str(meta["result"])
	return out


def san_spec(san: list | None) -> str:
	if not san:
		return "-"
	parts: list[str] = []
	for entry in san:
		text = str(entry)
		if text.startswith("DNS:") or text.startswith("IP:"):
			parts.append(text)
		else:
			raise SystemExit(f"unsupported SAN entry: {entry!r}")
	return ",".join(parts)


def main() -> int:
	parser = argparse.ArgumentParser()
	parser.add_argument("--fixtures", required=True, help="aerospike-client-conformance root")
	parser.add_argument(
		"--declarations",
		required=True,
		help="path to csharp-declared.yaml",
	)
	args = parser.parse_args()

	fixtures = Path(args.fixtures)
	fixture = fixtures / "tls" / "name-matching.yaml"
	if not fixture.is_file():
		sys.stderr.write(f"fixture not found: {fixture}\n")
		return 2

	doc = yaml.safe_load(fixture.read_text(encoding="utf-8"))
	shapes = doc.get("shapes") or {}
	rows = doc.get("rows") or []
	declared = load_declarations(Path(args.declarations))

	for row in rows:
		case = row["case"]
		if row.get("state") == "blocked":
			continue
		shape_name = row["input"]["shape"]
		shape = shapes[shape_name]
		tls_name = str(row["input"]["tlsName"])
		expect = str(row["expect"]["outcome"])
		result = declared.get(case, "pass")
		cn = str(shape["subjectCN"])
		san = san_spec(shape.get("san"))
		print("\t".join([case, shape_name, tls_name, cn, san, expect, result]))
	return 0


if __name__ == "__main__":
	raise SystemExit(main())
