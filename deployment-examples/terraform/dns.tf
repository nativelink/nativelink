# Copyright 2022 Nathan (Blaise) Bruer.  All rights reserved.
# --- Begin CAS Domain Records ---

resource "aws_route53_record" "cas_lb_domain_record_cert_verify" {
  for_each = {
    for dvo in aws_acm_certificate.cas_lb_certificate.domain_validation_options : dvo.domain_name => {
      name   = dvo.resource_record_name
      record = dvo.resource_record_value
      type   = dvo.resource_record_type
    }
  }
  name    = each.value.name
  records = [each.value.record]
  type    = each.value.type

  zone_id = data.aws_route53_zone.main.zone_id
  ttl     = 60
}

resource "aws_route53_record" "cas_lb_domain_record" {
  zone_id = data.aws_route53_zone.main.zone_id
  name    = "${var.cas_domain_prefix}.${var.base_domain}"
  type    = "A"

  alias {
    name                   = aws_lb.cas_load_balancer.dns_name
    zone_id                = aws_lb.cas_load_balancer.zone_id
    evaluate_target_health = true
  }
}

# --- End CAS Domain Records ---
# --- Begin Scheduler Domain Records ---

resource "aws_route53_record" "scheduler_lb_domain_record_cert_verify" {
  for_each = {
    for dvo in aws_acm_certificate.scheduler_lb_certificate.domain_validation_options : dvo.domain_name => {
      name   = dvo.resource_record_name
      record = dvo.resource_record_value
      type   = dvo.resource_record_type
    }
  }
  name    = each.value.name
  records = [each.value.record]
  type    = each.value.type

  zone_id = data.aws_route53_zone.main.zone_id
  ttl     = 60
}

resource "aws_route53_record" "scheduler_lb_domain_record" {
  zone_id = data.aws_route53_zone.main.zone_id
  name    = "${var.scheduler_domain_prefix}.${var.base_domain}"
  type    = "A"

  alias {
    name                   = aws_lb.scheduler_load_balancer.dns_name
    zone_id                = aws_lb.scheduler_load_balancer.zone_id
    evaluate_target_health = true
  }
}

# --- End Scheduler Domain Records ---
