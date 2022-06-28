#!/bin/bash
# Copyright 2022 Nathan (Blaise) Bruer.  All rights reserved.
# Starts turbo-cache remote execution service.
#
# This service will try and figure out what kind of service
# it should morph into and then configure and launch it.

set -euxo pipefail

# Try to mount any volumes if needed. Ignore any errors that might occur.
~/create_filesystem.sh /mnt/data || true

INSTANCE_ID=$(curl --silent http://instance-data/latest/meta-data/instance-id)
AWS_REGION=$(curl --silent http://instance-data/latest/meta-data/placement/availability-zone | rev | cut -c2- | rev)

TAGS=$(aws ec2 describe-tags --filters "Name=resource-id,Values=$INSTANCE_ID" --region "$AWS_REGION")

TOTAL_AVAIL_MEMORY=$(( $(grep MemFree /proc/meminfo | awk '{print $2}') * 10000 / 12 ))

# These environmental variables are used inside the json file resolution.
export TURBO_CACHE_AWS_REGION="$AWS_REGION"
export TURBO_CACHE_AWS_S3_CAS_BUCKET=$(echo "$TAGS"| jq -r '.Tags[] | select(.Key == "turbo_cache:s3_cas_bucket").Value')

TYPE=$(echo "$TAGS"| jq -r '.Tags[] | select(.Key == "turbo_cache:instance_type").Value')
if [ "$TYPE" == "ami_builder" ]; then
    # This instance type is special. It is not used in production, but due to terraform being unable
    # to delete/stop instances, we wait for this instance to restart after the AMI is created then
    # just terminate it.
    aws ec2 terminate-instances --region "$AWS_REGION" --instance-ids "$INSTANCE_ID"
    sleep 30 # This is so the service doesn't try to run this script again immediately.
elif [ "$TYPE" == "cas" ]; then
    # 10% goes to action cache.
    export TURBO_CACHE_AC_CONTENT_LIMIT=$(( $TOTAL_AVAIL_MEMORY / 10 ))
    # 5% goes to the index memory cache.
    export TURBO_CACHE_CAS_INDEX_CACHE_LIMIT=$(( $TOTAL_AVAIL_MEMORY / 20 ))
    # 85% goes to the content memory cache.
    export TURBO_CACHE_CAS_CONTENT_LIMIT=$(( $TOTAL_AVAIL_MEMORY - - $TURBO_CACHE_AC_CONTENT_LIMIT - $TURBO_CACHE_CAS_INDEX_CACHE_LIMIT ))

    /usr/local/bin/turbo-cache /root/cas.json
elif [ "$TYPE" == "scheduler" ]; then
    # 10% goes to action cache.
    export TURBO_CACHE_AC_CONTENT_LIMIT=$(( $TOTAL_AVAIL_MEMORY / 10 ))
    # 5% goes to the index memory cache.
    export TURBO_CACHE_CAS_INDEX_CACHE_LIMIT=$(( $TOTAL_AVAIL_MEMORY / 20 ))
    # 85% goes to the content memory cache.
    export TURBO_CACHE_CAS_CONTENT_LIMIT=$(( $TOTAL_AVAIL_MEMORY - - $TURBO_CACHE_AC_CONTENT_LIMIT - $TURBO_CACHE_CAS_INDEX_CACHE_LIMIT ))

    /usr/local/bin/turbo-cache /root/scheduler.json
elif [ "$TYPE" == "worker" ]; then
    SCHEDULER_URL=$(echo "$TAGS"| jq -r '.Tags[] | select(.Key == "turbo_cache:scheduler_url").Value')
    DISK_SIZE=$(df /mnt/data | tail -n 1 | awk '{ print $2 }')

    # This is used inside the "worker.json" file.
    export MAX_WORKER_DISK_USAGE=$(( DISK_SIZE * 100 / 125  )) # Leave about 20% disk space for active tasks.
    export SCHEDULER_ENDPOINT=$(echo "$TAGS"| jq -r '.Tags[] | select(.Key == "turbo_cache:scheduler_endpoint").Value')

    mkdir -p /mnt/data
    mkdir -p /worker
    mount --bind /mnt/data /worker
    /usr/local/bin/turbo-cache /root/worker.json
fi
