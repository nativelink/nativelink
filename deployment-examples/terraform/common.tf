# Copyright 2022 Nathan (Blaise) Bruer.  All rights reserved.
# --- Begin commonly account variables ---

data "aws_caller_identity" "current" {}

data "aws_availability_zones" "available" {
  state = "available"
}

data "aws_vpc" "main" {
  state = "available"
}

data "aws_region" "current" {}

data "aws_subnet_ids" "main" {
  vpc_id = data.aws_vpc.main.id
}

data "aws_route53_zone" "main" {
  name = "${var.base_domain}."
}

# --- End commonly account variables ---
