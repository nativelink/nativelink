# Copyright 2022 Nathan (Blaise) Bruer.  All rights reserved.
# --- Begin CAS S3 ---

resource "aws_s3_bucket" "cas_bucket" {
  bucket        = "turbo-cache-cas-bucket-${data.aws_caller_identity.current.account_id}"

  # Sadly terraform does not support having these values be defined as a terraform variable.
  # In reality we want to allow "dev" environments to destroy the buckets, but not allow
  # prod environments to delete this bucket, since it has files that we never want to
  # have for a long time. We believe that defaulting everything to "production"
  # (ie: don't delete) is the lesser of "make developer's life more painful" vs (accidentally
  # deleting production data).
  # See: https://github.com/hashicorp/terraform/issues/22544
  force_destroy = false
}

resource "aws_s3_bucket_public_access_block" "cas_bucket_public_access_block" {
  bucket = aws_s3_bucket.cas_bucket.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "cas_bucket_ownership_controls" {
  bucket = aws_s3_bucket.cas_bucket.id

  rule {
    object_ownership = "BucketOwnerPreferred"
  }
}

# --- End CAS S3 ---
# --- Begin Access Logs S3 ---

# Access logs of the various load balancers will be stored in this bucket.
# This is a security precaution and not required, however if your deployment
# might possibly have sensitive information, it is ALWAYS a good idea to spend
# a little extra to store access pattern logs, so in the event of a security
# breach there will at least be a record of it somewhere.
resource "aws_s3_bucket" "access_logs" {
  bucket        = "turbo-cache-access-logs-${data.aws_caller_identity.current.account_id}"

  # Sadly terraform does not support having these values be defined as a terraform variable.
  # In reality we want to allow "dev" environments to destroy the buckets, but not allow
  # prod environments to delete this bucket, since it has files that we never want to
  # have for a long time. We believe that defaulting everything to "production"
  # (ie: don't delete) is the lesser of "make developer's life more painful" vs (accidentally
  # deleting production data).
  # See: https://github.com/hashicorp/terraform/issues/22544
  force_destroy = false
}

resource "aws_s3_bucket_public_access_block" "access_logs_bucket_public_access_block" {
  bucket = aws_s3_bucket.access_logs.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "access_logs_bucket_ownership_controls" {
  bucket = aws_s3_bucket.access_logs.id

  rule {
    object_ownership = "BucketOwnerPreferred"
  }
}

resource "aws_s3_bucket_policy" "allow_s3_access_log_writing_bucket_policy" {
  bucket = aws_s3_bucket.access_logs.id
  policy = data.aws_iam_policy_document.allow_s3_access_log_writing_policy_document.json
}

data "aws_iam_policy_document" "allow_s3_access_log_writing_policy_document" {
  version = "2012-10-17"

  statement {
    effect = "Allow"
    actions = ["s3:PutObject"]
    resources = [
      "arn:aws:s3:::${aws_s3_bucket.access_logs.id}/*/AWSLogs/${data.aws_caller_identity.current.account_id}/*"
    ]

    principals {
      type = "AWS"
      identifiers = ["arn:aws:iam::${var.elb_account_id_for_region_map[data.aws_region.current.name]}:root"]
    }
  }
}

# --- End Access Logs S3 ---
