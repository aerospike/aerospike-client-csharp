#!/bin/bash

set -e
set -x

# Prevent Windows shell from mangling paths
export MSYS_NO_PATHCONV=1

# Input defaults
CONTAINER_NAME=${CONTAINER_NAME:-"aerospike"}
# Server features
SECURITY=${SECURITY:-"0"}
STRONG_CONSISTENCY=${STRONG_CONSISTENCY:-"0"}

# End inputs

VOLUME_NAME=aerospike-conf-vol
docker volume create $VOLUME_NAME

volume_dest_folder="/workdir"
container_name_for_populating_volume="container_for_populating_volume"

docker run --name $container_name_for_populating_volume --rm -v $VOLUME_NAME:$volume_dest_folder -d alpine tail -f /dev/null
docker cp ./ $container_name_for_populating_volume:$volume_dest_folder
docker stop $container_name_for_populating_volume

aerospike_yaml_file_name="aerospike-dev.yaml"

# This is a function instead of a variable that we call before our arguments
# because yq takes in the expression before accepting flags
call_from_yq_container() {
    # alpine container's process is run as root user
    # So the files copied into the named volume will also be owned by root
    # Since these files only have write permission for the owner (root),
    # We also need to run yq container as root in order to write to the yaml file in this volume.
    docker run --rm --user root -v $VOLUME_NAME:$volume_dest_folder mikefarah/yq "$1" -i ${volume_dest_folder}/${aerospike_yaml_file_name}
}

# del() operations are idempotent
if [[ "$SECURITY" == "1" ]]; then
    call_from_yq_container ".security.enable-quotas = \"true\""
    call_from_yq_container ".security.log.report-violation = \"true\""
else
    call_from_yq_container "del(.security)"
fi

if [[ "$STRONG_CONSISTENCY" == "1" ]]; then
    call_from_yq_container ".namespaces[0].strong-consistency = \"true\""
    call_from_yq_container ".namespaces[0].strong-consistency-allow-expunge = \"true\""
else
    call_from_yq_container ".namespaces[0].strong-consistency = \"false\""
    call_from_yq_container ".namespaces[0].strong-consistency-allow-expunge = \"false\""
fi

# We want to save our aerospike.conf in this directory.
call_from_tools_container() {
    # Have to pass $@ within double quotes to prevent individual arguments from being word splitted
    docker run --rm -v $VOLUME_NAME:$volume_dest_folder --network host aerospike/aerospike-tools "$@"
}

aerospike_conf_name=aerospike.conf
call_from_tools_container asconfig convert -f ${volume_dest_folder}/${aerospike_yaml_file_name} -o ${volume_dest_folder}/$aerospike_conf_name

# Some Docker containers may have a lower max fd limit than the server default
docker run --ulimit nofile=15000 -d --rm --name "$CONTAINER_NAME" -p 3000:3000 \
    -v $VOLUME_NAME:$volume_dest_folder \
    "$IMAGE_FULL_NAME" --config-file $volume_dest_folder/$aerospike_conf_name

if [[ "$SECURITY" == "1" ]]; then
    export SECURITY_FLAGS="-U admin -P admin"
fi

# Wait for server to start
./wait-for-as-server-to-start.bash

# Strong consistency setup
# Set up roster
if [[ "$STRONG_CONSISTENCY" == "1" ]]; then
    # shellcheck disable=SC2086 -- SECURITY_FLAGS contains multiple arguments (-U admin -P admin) that must be word split
    call_from_tools_container asadm $SECURITY_FLAGS --enable --execute "manage roster stage observed ns test"
    # shellcheck disable=SC2086 -- SECURITY_FLAGS contains multiple arguments (-U admin -P admin) that must be word split
    call_from_tools_container asadm $SECURITY_FLAGS --enable --execute "manage recluster"
fi

