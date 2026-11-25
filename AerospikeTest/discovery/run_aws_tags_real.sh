#!/usr/bin/env bash
#
# AWS EC2 tag seed discovery (AwsEc2TagSeedCandidateProvider) against real AWS.
#
# Run on an EC2 instance in the same region as a small self-managed Aerospike cluster whose nodes
# carry the tag AWS_TAGS_REAL_TAG. The instance's instance profile role must allow only
# ec2:DescribeInstances (the documented minimum policy) and is the only credential source: the script
# fails when any AWS credential environment variable is set. Runs TestAwsTagsReal once, which:
#   - runs AWS_TAGS_REAL_RUNS fresh processes, each with one cold refresh (credentials through IMDSv2
#     included) and AWS_TAGS_REAL_WARM warm refreshes, and reports min, median, p99 and max of each
#     against the default discoveryTendRefreshDeadline (100ms) and the recommended timeout (80ms);
#   - checks that credentials came from IMDS and that AWS accepted the signature;
#   - checks that the seed list holds AWS_TAGS_REAL_EXPECTED_NODES hosts and none of
#     AWS_TAGS_REAL_EXCLUDED (untagged and stopped instances);
#   - runs one fresh process with the temporary credentials of a role with no policy in
#     AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_SESSION_TOKEN, and checks that EC2 refuses it
#     with UnauthorizedOperation and a request id;
#   - joins every tagged node with a client seeded only by the provider and reports the overrun
#     count after AWS_TAGS_REAL_TEND_REFRESHES tend refreshes.
# The account allows roles and instance profiles, not IAM users. Obtain the no-policy role's
# temporary credentials from another principal allowed to assume it, for example
#   aws sts assume-role --role-arn <arn> --role-session-name aws-tags-denied
#
# Required environment:
#   AWS_TAGS_REAL_REGION                   region of the instances, e.g. us-east-1
#   AWS_TAGS_REAL_TAG                      tag filter key=value, e.g. aerospike-cluster=aws-tags-real
#   AWS_TAGS_REAL_EXPECTED_NODES           number of tagged running cluster nodes
#   AWS_TAGS_REAL_EXCLUDED                 comma-separated private IPs of untagged and stopped instances
#   AWS_TAGS_REAL_DENIED_ACCESS_KEY_ID     temporary credentials of a role with no policy
#   AWS_TAGS_REAL_DENIED_SECRET_ACCESS_KEY
#   AWS_TAGS_REAL_DENIED_SESSION_TOKEN
# Optional environment:
#   AWS_TAGS_REAL_NAMESPACE                namespace of the cluster (default test)
#   AWS_TAGS_REAL_RUNS                     fresh-process latency runs (default 20)
#   AWS_TAGS_REAL_WARM                     warm refreshes per run (default 20)
#   AWS_TAGS_REAL_TEND_REFRESHES           tend refreshes counted by the join case (default 30)
#   AWS_TAGS_REAL_SKIP_BUILD               set to 1 to skip building first
#   AWS_TAGS_REAL_CONFIGURATION            build configuration (default Release)
#
# Usage: AerospikeTest/discovery/run_aws_tags_real.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ROOT_DIR="$(cd "$TEST_DIR/.." && pwd)"
WORK_DIR="$TEST_DIR/bin/discovery-aws-tags-real"
CONFIG="${AWS_TAGS_REAL_CONFIGURATION:-Release}"
FILTER='FullyQualifiedName~TestAwsTagsReal'

die() {
	echo "ERROR: $*" >&2
	exit 2
}

missing=()
for name in AWS_TAGS_REAL_REGION AWS_TAGS_REAL_TAG AWS_TAGS_REAL_EXPECTED_NODES AWS_TAGS_REAL_EXCLUDED \
	AWS_TAGS_REAL_DENIED_ACCESS_KEY_ID AWS_TAGS_REAL_DENIED_SECRET_ACCESS_KEY AWS_TAGS_REAL_DENIED_SESSION_TOKEN; do
	[ -n "${!name:-}" ] || missing+=("$name")
done
[ ${#missing[@]} -eq 0 ] || die "required variables not set: ${missing[*]}"

case "$AWS_TAGS_REAL_TAG" in
	?*=?*) ;;
	*) die "AWS_TAGS_REAL_TAG '$AWS_TAGS_REAL_TAG' is not key=value" ;;
esac

for name in AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN AWS_PROFILE AWS_SHARED_CREDENTIALS_FILE \
	AWS_WEB_IDENTITY_TOKEN_FILE AWS_ROLE_ARN AWS_CONTAINER_CREDENTIALS_RELATIVE_URI AWS_CONTAINER_CREDENTIALS_FULL_URI; do
	[ -z "${!name:-}" ] || die "$name is set; the instance profile must be the only credential source"
done
[ ! -e "$HOME/.aws/credentials" ] || die "$HOME/.aws/credentials exists; the instance profile must be the only credential source"

command -v curl >/dev/null 2>&1 || die "curl not found (IMDSv2 check)"
IMDS_TOKEN="$(curl -sf -m 2 -X PUT http://169.254.169.254/latest/api/token \
	-H 'X-aws-ec2-metadata-token-ttl-seconds: 60' || true)"
[ -n "$IMDS_TOKEN" ] || die "IMDSv2 is not reachable: run on an EC2 instance with IMDS enabled"
ROLE="$(curl -sf -m 2 -H "X-aws-ec2-metadata-token: $IMDS_TOKEN" \
	http://169.254.169.254/latest/meta-data/iam/security-credentials/ || true)"
[ -n "$ROLE" ] || die "no AWS credentials: the instance has no instance profile role"
echo "Instance profile role: $ROLE"

command -v dotnet >/dev/null 2>&1 || die "dotnet not found"

if [ "${AWS_TAGS_REAL_SKIP_BUILD:-0}" != "1" ]; then
	echo "Building AerospikeClient, AwsTagsRealProbe, AerospikeTest ($CONFIG)"
	(cd "$ROOT_DIR" && dotnet build AwsTagsRealProbe/AwsTagsRealProbe.csproj -c "$CONFIG" --nologo) \
		|| die "AwsTagsRealProbe build failed"
	(cd "$ROOT_DIR" && dotnet build AerospikeTest/AerospikeTest.csproj -c "$CONFIG" --nologo) \
		|| die "AerospikeTest build failed"
fi

PROBE_DLL="$ROOT_DIR/AwsTagsRealProbe/bin/$CONFIG/net8.0/AwsTagsRealProbe.dll"
[ -f "$PROBE_DLL" ] || die "probe dll missing: $PROBE_DLL"
export AWS_TAGS_REAL_PROBE="$PROBE_DLL"

rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR"
log="$WORK_DIR/dotnet-test.log"
runsettings="$WORK_DIR/aws-tags-real.runsettings"
cat > "$runsettings" <<EOF
<?xml version="1.0" encoding="utf-8"?>
<RunSettings>
  <RunConfiguration>
    <ResultsDirectory>$WORK_DIR/TestResults</ResultsDirectory>
    <TestCaseFilter>$FILTER</TestCaseFilter>
  </RunConfiguration>
  <TestRunParameters>
    <Parameter name="Host" value="" />
    <Parameter name="Port" value="3000" />
    <Parameter name="ClusterName" value="" />
    <Parameter name="Namespace" value="${AWS_TAGS_REAL_NAMESPACE:-test}" />
    <Parameter name="Set" value="test" />
    <Parameter name="User" value="" />
    <Parameter name="Password" value="" />
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

# Suite.AssemblyInit skips shared-client connect when AWS_TAGS_REAL_REGION is set.
echo "Running AWS tags real suite filter=$FILTER"
set +e
(cd "$ROOT_DIR" && dotnet test AerospikeTest/AerospikeTest.csproj -c "$CONFIG" --no-build \
	--settings "$runsettings" \
	--filter "$FILTER" \
	--logger "console;verbosity=detailed" \
	--results-directory "$WORK_DIR/TestResults" \
	-- \
	--filter "$FILTER") > "$log" 2>&1
rc=$?
set -e

grep -E "AWS-REAL-" "$log" || true
grep -E "Assert\.|error CS|Failed!|Passed!|Not run" "$log" | head -40 || true
echo "dotnet log: $log"

if [ $rc -ne 0 ]; then
	echo "RESULT FAIL"
	exit 1
fi
echo "RESULT PASS"
