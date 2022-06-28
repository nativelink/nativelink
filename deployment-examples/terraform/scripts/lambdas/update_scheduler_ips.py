# Copyright 2022 Nathan (Blaise) Bruer.  All rights reserved.
"""Update Route53 record to add ip addresses of instances in auto scaling group"""

import os
import boto3

AUTO_SCALING_GROUP_NAME = os.environ["AUTO_SCALING_GROUP_NAME"]
HOSTED_ZONE_ID = os.environ["HOSTED_ZONE_ID"]
SCHEDULER_DOMAIN = os.environ["SCHEDULER_DOMAIN"]


def update_dns_record(target_ips):
    """ Change the domain record """
    records = []
    for target_ip in target_ips:
        records.append({"Value": target_ip})

    route53_client = boto3.client("route53")
    route53_client.change_resource_record_sets(
        HostedZoneId=HOSTED_ZONE_ID,
        ChangeBatch={
            "Comment": "UPSERT subdomain %s from zone %s" % (SCHEDULER_DOMAIN, HOSTED_ZONE_ID),
            "Changes": [
                {
                    "Action": "UPSERT",
                    "ResourceRecordSet": {
                        "Name": SCHEDULER_DOMAIN,
                        "Type": "A",
                        "ResourceRecords": records,
                        "TTL": 300,
                    },
                }
            ],
        },
    )


def lambda_handler(event, context):
    autoscaling_client = boto3.client("autoscaling")
    ec2_client = boto3.client("ec2")

    groups = autoscaling_client.describe_auto_scaling_groups(
        AutoScalingGroupNames = [AUTO_SCALING_GROUP_NAME]
    )
    instance_ids = []
    for group in groups["AutoScalingGroups"]:
        for instance in group["Instances"]:
            instance_ids.append(instance["InstanceId"])

    if len(instance_ids) <= 0:
        print("No schedulers active")
        return
    private_ips = []
    instances = ec2_client.describe_instances(InstanceIds = instance_ids)
    for reservation in instances["Reservations"]:
        for instance in reservation["Instances"]:
            if instance["State"]["Name"] == "running":
                private_ips.append(instance["PrivateIpAddress"])

    update_dns_record(private_ips)
    return {
        "code": 200,
        "message": f"Updated record to {private_ips}"
    }
