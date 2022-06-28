# Copyright 2022 Nathan (Blaise) Bruer.  All rights reserved.
# --- Begin AMI Builder ---

resource "aws_iam_role" "iam_builder_role" {
  name = "turbo_cache_ami_builder_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = "sts:AssumeRole",
        Principal = {
          Service = "ec2.amazonaws.com"
        },
        Effect = "Allow",
        Sid    = ""
      }
    ]
  })

  inline_policy {
    name = "turbo_cache_terminate_instance_policy"

    policy = jsonencode({
      Version = "2012-10-17"
      Statement = [
        {
          Action   = ["ec2:TerminateInstances"]
          Effect   = "Allow"
          Resource = "arn:aws:ec2:*:${data.aws_caller_identity.current.account_id}:instance/*"
        },
      ]
    })
  }
}

resource "aws_iam_instance_profile" "builder_profile" {
  name = "turbo_cache_builder_profile"
  role = aws_iam_role.iam_builder_role.name
}

resource "aws_iam_role_policy_attachment" "turbo_cache_ami_builder_describe_ec2_tags_policy" {
  role       = aws_iam_role.iam_builder_role.name
  policy_arn = aws_iam_policy.describe_ec2_tags_policy.arn
}

# --- End AMI Builder ---
# --- Begin Scheduler ---

resource "aws_iam_role" "iam_scheduler_role" {
  name = "turbo_cache_scheduler_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = "sts:AssumeRole",
        Principal = {
          Service = "ec2.amazonaws.com"
        },
        Effect = "Allow",
        Sid    = ""
      }
    ]
  })
}

resource "aws_iam_instance_profile" "scheduler_profile" {
  name = "turbo_cache_scheduler_profile"
  role = aws_iam_role.iam_scheduler_role.name
}

resource "aws_iam_role_policy_attachment" "turbo_cache_scheduler_s3_read_policy_attachment" {
  role       = aws_iam_role.iam_scheduler_role.name
  policy_arn = aws_iam_policy.readonly_s3_access_cas_bucket_policy.arn
}

resource "aws_iam_role_policy_attachment" "turbo_cache_scheduler_describe_ec2_tags_policy" {
  role       = aws_iam_role.iam_scheduler_role.name
  policy_arn = aws_iam_policy.describe_ec2_tags_policy.arn
}

# --- End Scheduler ---
# --- Begin CAS ---

resource "aws_iam_role" "cas_iam_role" {
  name = "turbo_cache_cas_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = "sts:AssumeRole",
        Principal = {
          Service = "ec2.amazonaws.com"
        },
        Effect = "Allow",
        Sid    = ""
      }
    ]
  })
}

resource "aws_iam_instance_profile" "cas_profile" {
  name = "turbo_cache_cas_profile"
  role = aws_iam_role.cas_iam_role.name
}

resource "aws_iam_role_policy_attachment" "turbo_cache_cas_read_s3_policy_attachment" {
  role       = aws_iam_role.cas_iam_role.name
  policy_arn = aws_iam_policy.readonly_s3_access_cas_bucket_policy.arn
}

resource "aws_iam_role_policy_attachment" "turbo_cache_cas_write_s3_policy_attachment" {
  role       = aws_iam_role.cas_iam_role.name
  policy_arn = aws_iam_policy.write_s3_access_bucket_policy.arn
}

resource "aws_iam_role_policy_attachment" "turbo_cache_cas_describe_ec2_tags_policy" {
  role       = aws_iam_role.cas_iam_role.name
  policy_arn = aws_iam_policy.describe_ec2_tags_policy.arn
}

# --- End CAS ---
# --- Begin Worker ---
resource "aws_iam_role" "worker_iam_role" {
  name = "turbo_cache_ami_worker_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = "sts:AssumeRole",
        Principal = {
          Service = "ec2.amazonaws.com"
        },
        Effect = "Allow",
        Sid    = ""
      }
    ]
  })

  inline_policy {
    name = "turbo_cache_terminate_instance_policy"

    policy = jsonencode({
      Version = "2012-10-17"
      Statement = [
        {
          Action   = ["ec2:TerminateInstances"]
          Effect   = "Allow"
          Resource = "arn:aws:ec2:*:${data.aws_caller_identity.current.account_id}:instance/*"
        },
      ]
    })
  }
}

resource "aws_iam_instance_profile" "worker_profile" {
  name = "turbo_cache_worker_profile"
  role = aws_iam_role.worker_iam_role.name
}

resource "aws_iam_role_policy_attachment" "turbo_cache_worker_read_s3_policy_attachment" {
  role       = aws_iam_role.worker_iam_role.name
  policy_arn = aws_iam_policy.readonly_s3_access_cas_bucket_policy.arn
}

resource "aws_iam_role_policy_attachment" "turbo_cache_worker_write_s3_policy_attachment" {
  role       = aws_iam_role.worker_iam_role.name
  policy_arn = aws_iam_policy.write_s3_access_bucket_policy.arn
}

resource "aws_iam_role_policy_attachment" "turbo_cache_worker_describe_ec2_tags_policy" {
  role       = aws_iam_role.worker_iam_role.name
  policy_arn = aws_iam_policy.describe_ec2_tags_policy.arn
}

# --- End Worker ---
# --- Begin Update Scheduler Lambda ---

resource "aws_iam_role" "update_scheduler_ips_lambda_role" {
  name   = "turbo_cache_update_scheduler_ips_lambda_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = "sts:AssumeRole",
        Principal = {
          Service = "lambda.amazonaws.com"
        },
        Effect = "Allow",
        Sid    = ""
      }
    ]
  })

  inline_policy {
    name = "turbo_cache_update_scheduler_ips_lambda_policy"

    policy = jsonencode({
        Version = "2012-10-17",
        Statement = [
            {
                Sid = "",
                Effect = "Allow",
                Action = [
                  "autoscaling:DescribeAutoScalingGroups",
                  "ec2:DescribeInstances",
                ],
                Resource = "*"
            },
            {
                Sid = "",
                Effect = "Allow",
                Action = "route53:ChangeResourceRecordSets",
                Resource = data.aws_route53_zone.main.arn
            }
        ]
    })
  }
}

# --- End Update Scheduler Lambda ---
# --- Begin Shared ---

resource "aws_iam_policy" "readonly_s3_access_cas_bucket_policy" {
  name        = "turbo_cache_readonly_s3_access_cas_bucket"
  description = "Read only policy for cas bucket"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
          "s3:ListMultipartUploadParts"
        ]
        Effect = "Allow"
        Sid    = ""
        Resource = [
          "${aws_s3_bucket.cas_bucket.arn}/*",
          "${aws_s3_bucket.cas_bucket.arn}"
        ]
      },
    ]
  })
}

resource "aws_iam_policy" "write_s3_access_bucket_policy" {
  name        = "turbo_cache_write_s3_access_cas_bucket"
  description = "Write only policy for cas bucket"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "s3:PutObject",
          "s3:AbortMultipartUpload",
          "s3:DeleteObjectVersion",
          "s3:DeleteObject"
        ]
        Effect = "Allow"
        Sid    = ""
        Resource = [
          "${aws_s3_bucket.cas_bucket.arn}/*",
          "${aws_s3_bucket.cas_bucket.arn}"
        ]
      },
    ]
  })
}

resource "aws_iam_policy" "describe_ec2_tags_policy" {
  name        = "turbo_cache_describe_ec2_tags_policy"
  description = "Allows describe tags on ec2 instances"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action   = "ec2:DescribeTags",
        Effect   = "Allow",
        Resource = "*",
        Sid      = ""
      },
    ]
  })
}

# --- End Shared ---
