# Copyright 2022 Nathan (Blaise) Bruer.  All rights reserved.
# --- Begin AMI Builder Instance ---

resource "aws_security_group" "ami_builder_instance_sg" {
  name = "turbo_cache_ami_builder_instance_sg"
}

# Note: AMI Builder is allowed to reach out to any location to download/install
# packages. This instance will be destroyed when it is done and the security group
# should not be used by any other instances. The chances of an attacker taking
# ownership of the AMI builder instance is quite low, as it should only be active
# while terraform is being applied.
resource "aws_security_group_rule" "ami_builder_allow_access_outside_world" {
  description       = "AMI Builder are allowed to access outside world"
  type              = "egress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  cidr_blocks       = ["0.0.0.0/0"]
  ipv6_cidr_blocks  = ["::/0"]
  security_group_id = aws_security_group.ami_builder_instance_sg.id
}

# --- End AMI Builder Instance ---
# --- Begin Scheduler Instances ---

resource "aws_security_group" "schedulers_instance_sg" {
  name = "turbo_cache_schedulers_instance_sg"
}

resource "aws_security_group_rule" "inbound_from_workers_to_schedulers_grpc" {
  description              = "Allow inbound traffic from workers to scheduler via grpc"
  type                     = "ingress"
  from_port                = 50061
  to_port                  = 50061
  protocol                 = "tcp"
  source_security_group_id = aws_security_group.worker_instance_sg.id
  security_group_id        = aws_security_group.schedulers_instance_sg.id
}

resource "aws_security_group_rule" "inbound_lb_traffic_to_schedulers_instances" {
  description              = "Allow scheduler loadbalancer to access scheduler instances"
  type                     = "ingress"
  from_port                = 50052
  to_port                  = 50052
  protocol                 = "tcp"
  source_security_group_id = aws_security_group.schedulers_load_balancer_sg.id
  security_group_id        = aws_security_group.schedulers_instance_sg.id
}

resource "aws_security_group" "schedulers_load_balancer_sg" {
  name = "turbo_cache_schedulers_load_balancer_sg"
}

resource "aws_security_group_rule" "allow_outside_world_to_schedulers_load_balancer" {
  description       = "Allow anywhere to access the scheduler load balancer"
  type              = "ingress"
  from_port         = 443
  to_port           = 443
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = aws_security_group.schedulers_load_balancer_sg.id
}

resource "aws_security_group_rule" "allow_lb_outbound_to_schedulers_instances" {
  description              = "Allow load balancer outbound traffic to scheduler instances"
  type                     = "egress"
  from_port                = 50052
  to_port                  = 50052
  protocol                 = "tcp"
  source_security_group_id = aws_security_group.schedulers_instance_sg.id
  security_group_id        = aws_security_group.schedulers_load_balancer_sg.id
}

# --- End Scheduler Instances ---
# --- Begin CAS Instances ---

resource "aws_security_group" "cas_instance_sg" {
  name = "turbo_cache_cas_instance_sg"
  # CAS is not allowed to access the internet.
}

resource "aws_security_group_rule" "inbound_lb_traffic_to_cas_instances" {
  description              = "Allow CAS loadbalancer to access cas instances"
  type                     = "ingress"
  from_port                = 50051
  to_port                  = 50051
  protocol                 = "tcp"
  source_security_group_id = aws_security_group.cas_load_balancer_sg.id
  security_group_id        = aws_security_group.cas_instance_sg.id
}

resource "aws_security_group" "cas_load_balancer_sg" {
  name = "turbo_cache_cas_load_balancer_sg"
}

resource "aws_security_group_rule" "allow_outside_world_to_cas_load_balancer" {
  description       = "Allow anywhere to access the CAS load balancer"
  type              = "ingress"
  from_port         = 443
  to_port           = 443
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = aws_security_group.cas_load_balancer_sg.id
}

resource "aws_security_group_rule" "allow_outbound_to_cas_instances" {
  description              = "Allow outbound traffic to CAS instances"
  type                     = "egress"
  from_port                = 50051
  to_port                  = 50051
  protocol                 = "tcp"
  source_security_group_id = aws_security_group.cas_instance_sg.id
  security_group_id        = aws_security_group.cas_load_balancer_sg.id
}

# --- Begin CAS Instances ---
# --- Begin Worker Instances ---

resource "aws_security_group" "worker_instance_sg" {
  name = "turbo_cache_worker_instance_sg"
  # Workers are not allowed to access the internet.
}

resource "aws_security_group_rule" "outbound_worker_to_schedulers_rule" {
  type                     = "egress"
  from_port                = 50061
  to_port                  = 50061
  protocol                 = "tcp"
  source_security_group_id = aws_security_group.schedulers_instance_sg.id
  security_group_id        = aws_security_group.worker_instance_sg.id
}

# --- End Worker Instances ---
# --- Begin Other ---

resource "aws_security_group" "aws_api_ec2_endpoint_sg" {
  name = "turbo_cache_aws_api_ec2_endpoint_sg"

  ingress {
    security_groups = [aws_security_group.allow_aws_ec2_and_s3_endpoints.id]
    from_port = 443
    to_port   = 443
    protocol  = "tcp"
  }
}

resource "aws_security_group" "allow_ssh_sg" {
  name = "turbo_cache_allow_ssh_sg"

  ingress {
    from_port        = 22
    to_port          = 22
    protocol         = "tcp"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }
}

resource "aws_security_group" "allow_aws_ec2_and_s3_endpoints" {
  name = "turbo_cache_allow_aws_ec2_and_s3_endpoints"
}

resource "aws_security_group_rule" "outbound_to_aws_s3_api_group_rule" {
  description       = "Allow access from instance to AWS S3 API endpoint"
  type              = "egress"
  from_port         = 443
  to_port           = 443
  protocol          = "tcp"
  prefix_list_ids   = [aws_vpc_endpoint.aws_api_s3_endpoint.prefix_list_id]
  security_group_id = aws_security_group.allow_aws_ec2_and_s3_endpoints.id
}

resource "aws_security_group_rule" "outbound_to_aws_ec2_endpoint_group_rule" {
  description              = "Allow instance to contact AWS EC2 API endpoint"
  type                     = "egress"
  from_port                = 443
  to_port                  = 443
  protocol                 = "tcp"
  source_security_group_id = aws_security_group.aws_api_ec2_endpoint_sg.id
  security_group_id        = aws_security_group.allow_aws_ec2_and_s3_endpoints.id
}

# --- End Other ---
