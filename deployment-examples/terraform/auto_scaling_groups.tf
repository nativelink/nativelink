# Copyright 2022 Nathan (Blaise) Bruer.  All rights reserved.
# --- Begin CAS ---

resource "aws_launch_template" "cas_launch_template" {
  name = "turbo_cache_cas_launch_template"
  update_default_version = true

  iam_instance_profile {
    arn = aws_iam_instance_profile.cas_profile.arn
  }

  image_id                             = aws_ami_from_instance.base_ami["arm"].id
  instance_initiated_shutdown_behavior = "terminate"
  key_name                             = aws_key_pair.turbo_cache_key.key_name
  vpc_security_group_ids = [
    aws_security_group.allow_ssh_sg.id,
    aws_security_group.allow_aws_ec2_and_s3_endpoints.id,
    aws_security_group.cas_instance_sg.id,
  ]

  tag_specifications {
    resource_type = "instance"
    tags = {
      "turbo_cache:instance_type" = "cas",
      "turbo_cache:s3_cas_bucket" = aws_s3_bucket.cas_bucket.id,
    }
  }
}

resource "aws_autoscaling_group" "cas_autoscaling_group" {
  name                      = "turbo_cache_cas_autoscaling_group"
  max_size                  = 25
  min_size                  = 1
  health_check_grace_period = 300
  health_check_type         = "ELB"
  desired_capacity          = 1
  force_delete              = false
  target_group_arns         = [aws_lb_target_group.cas_target_group.arn]
  availability_zones        = data.aws_availability_zones.available.names
  default_cooldown          = 300 # 5 mins.

  # This will help hold onto instances with hottest cache.
  termination_policies = ["NewestInstance"]
  enabled_metrics = [
    "GroupMinSize",
    "GroupMaxSize",
    "GroupDesiredCapacity",
    "GroupInServiceInstances",
    "GroupTotalInstances"
  ]

  timeouts {
    delete = "10m"
  }

  mixed_instances_policy {
    instances_distribution {
      on_demand_percentage_above_base_capacity = 0
      spot_allocation_strategy                 = "lowest-price"
    }

    launch_template {
      launch_template_specification {
        launch_template_id = aws_launch_template.cas_launch_template.id
      }

      # We use the formula (((memory - 1) / 2 + vcpu)  * 10) as the scaling metric.
      # This causes the algorithm to favor smaller instances and higher weight on
      # more memory.
      override {
        instance_type     = "r6g.xlarge"
        weighted_capacity = "195"
      }

      # For cost savings it is only configured to use r6g as spot instances
      # however, if you wanted to increase reliability you could specify a
      # few more instance types to help reduce chance of being starved of
      # the instance availability.
      # override {
      #   instance_type     = "r6g.2xlarge"
      #   weighted_capacity = "395"
      # }
      #
      # override {
      #   instance_type     = "m6g.xlarge"
      #   weighted_capacity = "115"
      # }
      #
      # override {
      #   instance_type     = "m6g.2xlarge"
      #   weighted_capacity = "235"
      # }
      #
      # override {
      #   instance_type     = "c6g.xlarge"
      #   weighted_capacity = "75"
      # }
      #
      # override {
      #   instance_type     = "c6g.2xlarge"
      #   weighted_capacity = "155"
      # }
      #
      # override {
      #   instance_type     = "c6gn.xlarge"
      #   weighted_capacity = "75"
      # }
      #
      # override {
      #   instance_type     = "c6gn.2xlarge"
      #   weighted_capacity = "155"
      # }
    }
  }
}

# --- End CAS ---
# --- Begin Scheduler ---

resource "aws_launch_template" "scheduler_launch_template" {
  name = "turbo_cache_scheduler_launch_template"
  update_default_version = true

  iam_instance_profile {
    arn = aws_iam_instance_profile.scheduler_profile.arn
  }

  image_id                             = aws_ami_from_instance.base_ami["arm"].id
  instance_initiated_shutdown_behavior = "terminate"
  key_name                             = aws_key_pair.turbo_cache_key.key_name
  vpc_security_group_ids = [
    aws_security_group.allow_ssh_sg.id,
    aws_security_group.allow_aws_ec2_and_s3_endpoints.id,
    aws_security_group.schedulers_instance_sg.id,
  ]

  tag_specifications {
    resource_type = "instance"
    tags = {
      "turbo_cache:instance_type" = "scheduler",
      "turbo_cache:s3_cas_bucket" = aws_s3_bucket.cas_bucket.id,
    }
  }
}

resource "aws_autoscaling_group" "scheduler_autoscaling_group" {
  name                      = "turbo_cache_scheduler_autoscaling_group"
  # TODO(allada) We currently only support 1 scheduler at a time. Trying to support
  # more than 1 scheduler at a time is undefined behavior and might end up with
  # very strange routing.
  max_size                  = 1
  min_size                  = 1
  health_check_grace_period = 300
  health_check_type         = "ELB"
  desired_capacity          = 1
  force_delete              = false
  target_group_arns         = [aws_lb_target_group.scheduler_target_group.arn]
  availability_zones        = data.aws_availability_zones.available.names
  default_cooldown          = 300 # 5 mins.

  timeouts {
    delete = "10m"
  }

  mixed_instances_policy {
    instances_distribution {
      # Ensure we always have 1 on-demand instance.
      on_demand_base_capacity = 1
      on_demand_percentage_above_base_capacity = 0
    }

    launch_template {
      launch_template_specification {
        launch_template_id = aws_launch_template.scheduler_launch_template.id
      }

      override {
        instance_type     = var.scheduler_instance_type
      }
    }
  }
}

# This will eventually trigger our lambda to run that will update our dns records.
resource "aws_autoscaling_notification" "notify_scheduler_change" {
  group_names = [aws_autoscaling_group.scheduler_autoscaling_group.name]

  notifications = [
    "autoscaling:EC2_INSTANCE_LAUNCH",
    "autoscaling:EC2_INSTANCE_TERMINATE",
    "autoscaling:EC2_INSTANCE_LAUNCH_ERROR",
    "autoscaling:EC2_INSTANCE_TERMINATE_ERROR",
  ]

  topic_arn = aws_sns_topic.update_scheduler_ips_sns_topic.arn
}

# We invoke this lambda once the aws_autoscaling_group is active and the lambda is created.
# This will ensure that we don't hit a race condition where the auto-scaling group is created
# and an instance is created before the lambda triggers are done.
resource "aws_lambda_invocation" "invoke_scheduler_ips_lambda" {
  function_name = aws_lambda_function.update_scheduler_ips_lambda.function_name
  input = "{}"

  depends_on = [
    aws_autoscaling_group.scheduler_autoscaling_group,
    aws_autoscaling_notification.notify_scheduler_change,
    aws_lambda_permission.allow_scheduler_ip_sns_execute_lambda,
    aws_sns_topic_subscription.update_scheduler_ips_sns_subscription,
    aws_lambda_function.update_scheduler_ips_lambda,
  ]
}

# --- End Scheduler ---
# --- Begin Worker ---

# Launch group for ARM workers.
resource "aws_launch_template" "worker_launch_template" {
  for_each = {
    arm = "arm",
    x86 = "x86"
  }

  name = "turbo_cache_worker_launch_template_${each.key}"
  update_default_version = true

  iam_instance_profile {
    arn = aws_iam_instance_profile.worker_profile.arn
  }

  image_id                             = aws_ami_from_instance.base_ami[each.key].id
  instance_initiated_shutdown_behavior = "terminate"
  key_name                             = aws_key_pair.turbo_cache_key.key_name
  vpc_security_group_ids = [
    aws_security_group.allow_ssh_sg.id,
    aws_security_group.allow_aws_ec2_and_s3_endpoints.id,
    aws_security_group.worker_instance_sg.id,
  ]

  tag_specifications {
    resource_type = "instance"
    tags = {
      "turbo_cache:instance_type" = "worker",
      "turbo_cache:s3_cas_bucket" = aws_s3_bucket.cas_bucket.id,
      "turbo_cache:scheduler_endpoint" = "${var.scheduler_domain_prefix}.internal.${var.base_domain}",
    }
  }
}

# ARM 1CPU Worker.
resource "aws_autoscaling_group" "worker_autoscaling_group_arm_1cpu" {
  name                      = "turbo_cache_worker_autoscaling_group_arm_1cpu"
  max_size                  = 200
  min_size                  = 1
  health_check_grace_period = 300
  health_check_type         = "ELB"
  desired_capacity          = 1
  force_delete              = false
  availability_zones        = data.aws_availability_zones.available.names
  default_cooldown          = 120 # 2 mins.

  timeouts {
    delete = "5m"
  }

  mixed_instances_policy {
    instances_distribution {
      on_demand_percentage_above_base_capacity = 0
      spot_allocation_strategy                 = "lowest-price"
    }

    launch_template {
      launch_template_specification {
        launch_template_id = aws_launch_template.worker_launch_template["arm"].id
      }

      override {
        instance_type     = "c6gd.medium"
        weighted_capacity = "1"
      }

      override {
        instance_type     = "m6gd.medium"
        weighted_capacity = "1"
      }

      override {
        instance_type     = "r6gd.medium"
        weighted_capacity = "1"
      }

      override {
        instance_type     = "c7g.medium"
        weighted_capacity = "1"
      }

      override {
        instance_type     = "is4gen.medium"
        weighted_capacity = "1"
      }
    }
  }
}

# x86 1CPU Worker.
resource "aws_autoscaling_group" "worker_autoscaling_group_x86_2cpu" {
  name                      = "turbo_cache_worker_autoscaling_group_x86_2cpu"
  max_size                  = 200
  min_size                  = 1
  health_check_grace_period = 300
  health_check_type         = "ELB"
  desired_capacity          = 1
  force_delete              = false
  availability_zones        = data.aws_availability_zones.available.names
  default_cooldown          = 120 # 2 mins.

  timeouts {
    delete = "5m"
  }

  mixed_instances_policy {
    instances_distribution {
      on_demand_percentage_above_base_capacity = 0
      spot_allocation_strategy                 = "lowest-price"
    }

    launch_template {
      launch_template_specification {
        launch_template_id = aws_launch_template.worker_launch_template["x86"].id
      }

      override {
        instance_type     = "m6id.large"
        weighted_capacity = "2"
      }

      override {
        instance_type     = "m5d.large"
        weighted_capacity = "2"
      }

      override {
        instance_type     = "m5ad.large"
        weighted_capacity = "2"
      }

      override {
        instance_type     = "m5dn.large"
        weighted_capacity = "2"
      }

      override {
        instance_type     = "c6id.large"
        weighted_capacity = "2"
      }

      override {
        instance_type     = "c5d.large"
        weighted_capacity = "2"
      }

      override {
        instance_type     = "c5ad.large"
        weighted_capacity = "2"
      }

      override {
        instance_type     = "r5ad.large"
        weighted_capacity = "2"
      }

      override {
        instance_type     = "r5dn.large"
        weighted_capacity = "2"
      }

      override {
        instance_type     = "i4i.large"
        weighted_capacity = "2"
      }

      override {
        instance_type     = "i3.large"
        weighted_capacity = "2"
      }

      override {
        instance_type     = "i3en.large"
        weighted_capacity = "2"
      }
    }
  }
}

# --- End Worker ---
