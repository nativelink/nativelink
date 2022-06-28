#!/bin/bash
# Copyright 2022 Nathan (Blaise) Bruer.  All rights reserved.
# Creates a raid from all available drives if multiple drives.
# Then creates an ext4 filesystem on raid (or single drive if no raid).

set -euxo pipefail

if [[ $EUID -ne 0 ]]; then
   echo "This script must be run as root" 
   exit 1
fi

MOUNT_LOCATION="$1"
shift

DEVICES=( $(lsblk --fs --json | jq -r '.blockdevices[] | select(.children == null and .fstype == null) | .name') )
DEVICES_FULLNAME=()
for DEVICE in "${DEVICES[@]}"; do
  DEVICES_FULLNAME+=("/dev/$DEVICE")
done

rm -rf "$MOUNT_LOCATION"
mkdir -p "$MOUNT_LOCATION"

if [ "${#DEVICES_FULLNAME[@]}" -eq "0" ]; then
    echo "No unused drives, using root volume"
    exit 0
elif [ "${#DEVICES_FULLNAME[@]}" -eq "1" ]; then
    echo "Only one volume letting ext4 manage it directly instead of raid"
    DEVICE="${DEVICES_FULLNAME[@]}"
else
    mdadm --create --verbose /dev/md0 --level=0 --raid-devices=${#DEVICES_FULLNAME[@]} "${DEVICES_FULLNAME[@]}"
    DEVICE="/dev/md0"
fi
mkfs.ext4 "$DEVICE"
mount "$DEVICE" "$MOUNT_LOCATION"
