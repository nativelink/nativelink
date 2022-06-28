# Copyright 2022 Nathan (Blaise) Bruer.  All rights reserved.
# --- Begin Endpoints ---

resource "aws_vpc_endpoint" "aws_api_ec2_endpoint" {
  vpc_id            = data.aws_vpc.main.id
  service_name      = "com.amazonaws.${data.aws_region.current.name}.ec2"
  vpc_endpoint_type = "Interface"

  security_group_ids = [aws_security_group.aws_api_ec2_endpoint_sg.id]

  private_dns_enabled = true
  subnet_ids          = data.aws_subnet_ids.main.ids
}

resource "aws_vpc_endpoint" "aws_api_s3_endpoint" {
  vpc_id            = data.aws_vpc.main.id
  service_name      = "com.amazonaws.${data.aws_region.current.name}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.aws_api_s3_endpoint.id]
}

resource "aws_route_table" "aws_api_s3_endpoint" {
  vpc_id = data.aws_vpc.main.id
}

# --- End Endpoints ---
