# Copyright 2022 Nathan (Blaise) Bruer.  All rights reserved.
# --- Begin CAS Certificate ---

resource "aws_acm_certificate" "cas_lb_certificate" {
  domain_name       = "${var.cas_domain_prefix}.${var.base_domain}"
  validation_method = "DNS"

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_acm_certificate_validation" "cas_lb_certificate_validation" {
  certificate_arn         = aws_acm_certificate.cas_lb_certificate.arn
  validation_record_fqdns = [for record in aws_route53_record.cas_lb_domain_record_cert_verify : record.fqdn]
}

# --- End CAS Certificate ---
# --- Begin Scheduler Certificate ---

resource "aws_acm_certificate" "scheduler_lb_certificate" {
  domain_name       = "${var.scheduler_domain_prefix}.${var.base_domain}"
  validation_method = "DNS"

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_acm_certificate_validation" "scheduler_lb_certificate_validation" {
  certificate_arn         = aws_acm_certificate.scheduler_lb_certificate.arn
  validation_record_fqdns = [for record in aws_route53_record.scheduler_lb_domain_record_cert_verify : record.fqdn]
}

# --- End Scheduler Certificate ---
