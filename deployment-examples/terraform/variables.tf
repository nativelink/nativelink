# Copyright 2022 Nathan (Blaise) Bruer.  All rights reserved.

variable "build_base_ami_arm" {
  description = "Base AMI for building Turbo Cache for ARM"
  default     = "ami-075200050e2c8899b"
}

variable "build_base_ami_x86" {
  description = "Base AMI for building Turbo Cache for x86"
  default     = "ami-0ddf424f81ddb0720"
}

variable "build_arm_instance_type" {
  description = "Type of EC2 instance to build Turbo Cache for ARM"
  default     = "c6gd.2xlarge"
}

variable "build_x86_instance_type" {
  description = "Type of EC2 instance to build Turbo Cache for x86"
  default     = "c6id.2xlarge"
}

variable "terminate_ami_builder" {
  description = "If we should terminate the AMI builder instances. Disabling this will development easier, but cost more money."
  default     = true
}

variable "scheduler_instance_type" {
  description = "Type of EC2 instance to use for Turbo Cache's scheduler"
  default     = "c6g.xlarge"
}

variable "allow_ssh_from_cidrs" {
  description = "List of CIDRs allowed to connect to SSH"
  default     = ["0.0.0.0/0"]
}

variable "base_domain" {
  description = "Base domain name of existing Route53 hosted zone. Subdomains will be added to this zone."
  default     = "turbo-cache.demo.allada.com"
}

variable "cas_domain_prefix" {
  description = "This will be the DNS name of the cas suffixed with `var.domain`. You may use dot notation to add more sub-domains. Example: `cas.{var.base_domain}`."
  default     = "cas"
}

variable "scheduler_domain_prefix" {
  description = "This will be the DNS name of the scheduler suffixed with `var.domain`. You may use dot notation to add more sub-domains. Example: `cas.{var.base_domain}`."
  default     = "scheduler"
}

# This is a list of Elastic Load Balancing account IDs taken from:
# https://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-access-logs.html#access-logging-bucket-permissions
# These are the accounts that will publish into the access log bucket.
variable "elb_account_id_for_region_map" {
  description = "Map of ELB account ids that publish into access log bucket."
  default     = {
    "us-east-1": "127311923021",
    "us-east-2": "033677994240",
    "us-west-1": "027434742980",
    "us-west-2": "797873946194",
    "af-south-1": "098369216593",
    "ca-central-1": "985666609251",
    "eu-central-1": "054676820928",
    "eu-west-1": "156460612806",
    "eu-west-2": "652711504416",
    "eu-south-1": "635631232127",
    "eu-west-3": "009996457667",
    "eu-north-1": "897822967062",
    "ap-east-1": "754344448648",
    "ap-northeast-1": "582318560864",
    "ap-northeast-2": "600734575887",
    "ap-northeast-3": "383597477331",
    "ap-southeast-1": "114774131450",
    "ap-southeast-2": "783225319266",
    "ap-southeast-3": "589379963580",
    "ap-south-1": "718504428378",
    "me-south-1": "076674570225",
    "sa-east-1": "507241528517",
    "us-gov-west-1": "048591011584",
    "us-gov-east-1": "190560391635",
    "cn-north-1": "638102146993",
    "cn-northwest-1": "037604701340",
  }
}
