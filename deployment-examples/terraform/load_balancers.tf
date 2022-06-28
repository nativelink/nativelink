# --- Begin CAS Load Balancer ---

resource "aws_lb" "cas_load_balancer" {
  name                   = "turbo-cache-cas-lb"
  internal               = false
  load_balancer_type     = "application"
  security_groups        = [aws_security_group.cas_load_balancer_sg.id]
  subnets                = data.aws_subnet_ids.main.ids
  enable_http2           = true
  idle_timeout           = 15
  desync_mitigation_mode = "strictest"

  # This is not required, but highly recommended.
  access_logs {
    bucket = aws_s3_bucket.access_logs.id
    prefix = "cas_load_balancer_access_logs"
    enabled = true
  }
}

resource "aws_lb_target_group" "cas_target_group" {
  name                          = "turbo-cache-cas-target-group"
  target_type                   = "instance"
  protocol                      = "HTTP"
  protocol_version              = "GRPC"
  vpc_id                        = data.aws_vpc.main.id
  load_balancing_algorithm_type = "least_outstanding_requests"
  port                          = 50051

  health_check {
    unhealthy_threshold = 2
    timeout             = 2
    interval            = 5
  }
}

resource "aws_lb_listener" "cas_load_balancer_listener" {
  load_balancer_arn = aws_lb.cas_load_balancer.arn
  port              = 443
  protocol          = "HTTPS"
  ssl_policy        = "ELBSecurityPolicy-2016-08"
  certificate_arn   = aws_acm_certificate.cas_lb_certificate.arn

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.cas_target_group.arn
  }
}

# --- End CAS Load Balancer ---
# --- Begin Scheduler Public Load Balancer ---

resource "aws_lb" "scheduler_load_balancer" {
  name                   = "turbo-cache-scheduler-lb"
  internal               = false
  # TODO(allada) This really should be a TCP based load balancer, but due to it being
  # GRPC and not supporting HTTP1.x causes the health checker to always fail.
  load_balancer_type     = "application"
  security_groups        = [aws_security_group.schedulers_load_balancer_sg.id]
  subnets                = data.aws_subnet_ids.main.ids
  enable_http2           = true
  idle_timeout           = 15
  desync_mitigation_mode = "strictest"

   # This is not required, but highly recommended.
  access_logs {
    bucket = aws_s3_bucket.access_logs.id
    prefix = "scheduler_load_balancer_access_logs"
    enabled = true
  }
}

resource "aws_lb_target_group" "scheduler_target_group" {
  name                          = "turbo-cache-scheduler-group"
  target_type                   = "instance"
  protocol                      = "HTTP"
  protocol_version              = "GRPC"
  vpc_id                        = data.aws_vpc.main.id
  load_balancing_algorithm_type = "least_outstanding_requests"
  port                          = 50052

  health_check {
    unhealthy_threshold = 5
    timeout             = 2
    interval            = 5
  }
}

resource "aws_lb_listener" "scheduler_load_balancer_listener" {
  load_balancer_arn = aws_lb.scheduler_load_balancer.arn
  port              = 443
  protocol          = "HTTPS"
  ssl_policy        = "ELBSecurityPolicy-2016-08"
  certificate_arn   = aws_acm_certificate.scheduler_lb_certificate.arn

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.scheduler_target_group.arn
  }
}

# --- End Scheduler Public Load Balancer ---
