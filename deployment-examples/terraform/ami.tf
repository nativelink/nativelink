# Copyright 2022 Nathan (Blaise) Bruer.  All rights reserved.
# -- Begin Base AMI ---

resource "aws_instance" "build_turbo_cache_instance" {
  for_each = {
    arm = {
      "instance_type": var.build_arm_instance_type,
      "ami": var.build_base_ami_arm,
    }
    x86 = {
      "instance_type": var.build_x86_instance_type,
      "ami": var.build_base_ami_x86,
    }
  }

  ami                         = each.value["ami"]
  instance_type               = each.value["instance_type"]
  associate_public_ip_address = true
  key_name                    = aws_key_pair.turbo_cache_key.key_name
  iam_instance_profile        = aws_iam_instance_profile.builder_profile.name

  vpc_security_group_ids = [
    aws_security_group.allow_ssh_sg.id,
    aws_security_group.ami_builder_instance_sg.id,
    aws_security_group.allow_aws_ec2_and_s3_endpoints.id,
  ]

  root_block_device {
    volume_size = 8
    volume_type = "gp3"
  }

  tags = {
    "turbo_cache:instance_type" = "ami_builder",
  }

  connection {
    host        = coalesce(self.public_ip, self.private_ip)
    agent       = true
    type        = "ssh"
    user        = "ubuntu"
    private_key = data.tls_public_key.turbo_cache_pem.private_key_openssh
  }

  provisioner "local-exec" {
    command = <<EOT
      set -ex
      SELF_DIR=$(pwd)
      cd ../../
      rm -rf $SELF_DIR/.terraform-turbo-cache-builder
      mkdir -p $SELF_DIR/.terraform-turbo-cache-builder
      find . ! -ipath '*/target*' -and ! -ipath '*/.*' -and ! -ipath './bazel-*' -type f -print0 | tar cvf $SELF_DIR/.terraform-turbo-cache-builder/file.tar.gz --null -T -
    EOT
  }

  provisioner "file" {
    source      = "./scripts/create_filesystem.sh"
    destination = "create_filesystem.sh"
  }

  provisioner "remote-exec" {
    # By moving common temp folder locations to the nvme drives (if available)
    # will greatly reduce the amount of data on the EBS volume. This also will
    # make the AMI/EBS snapshot much faster to create, since the blocks on the
    # EBS drives was not changed.
    # When the instance starts we need to give a tiny bit of time for amazon
    # to install the keys for all the apt packages.
    inline = [
      <<EOT
        set -eux &&
        `# When the instance first starts up AWS may have not finished add the certs to the` &&
        `# apt servers, so we sleep for a few seconds` &&
        sleep 5 &&
        sudo apt update &&
        sudo apt install -y jq &&
        sudo mv ~/create_filesystem.sh /root/create_filesystem.sh &&
        sudo chmod +x /root/create_filesystem.sh &&
        sudo /root/create_filesystem.sh /mnt/data &&
        sudo rm -rf /tmp/* &&
        sudo mkdir -p /mnt/data/tmp &&
        sudo chmod 777 /mnt/data/tmp &&
        sudo mount --bind /mnt/data/tmp /tmp &&
        sudo chmod 777 /tmp &&
        sudo mkdir -p /mnt/data/docker &&
        sudo mkdir -p /var/lib/docker &&
        sudo mount --bind /mnt/data/docker /var/lib/docker
      EOT
    ]
  }

  provisioner "file" {
    source      = "./.terraform-turbo-cache-builder/file.tar.gz"
    destination = "/tmp/file.tar.gz"
  }

  provisioner "remote-exec" {
    inline = [
      <<EOT
        set -eux &&
        mkdir -p /tmp/turbo-cache &&
        cd /tmp/turbo-cache &&
        tar xvf /tmp/file.tar.gz &&
        sudo apt install -y docker.io awscli &&
        cd /tmp/turbo-cache &&
        sudo docker build -t turbo-cache-runner -f ./deployment-examples/docker-compose/Dockerfile . &&
        container_id=$(sudo docker create turbo-cache-runner) &&
        `# Copy the compiled binary out of the container` &&
        sudo docker cp $container_id:/usr/local/bin/turbo-cache /usr/local/bin/turbo-cache &&
        `# Stop and remove all containers, as they are not needed` &&
        sudo docker rm $(sudo docker ps -a -q) &&
        sudo docker rmi $(sudo docker images -q) &&
        `` &&
        sudo mv /tmp/turbo-cache/deployment-examples/terraform/scripts/scheduler.json /root/scheduler.json &&
        sudo mv /tmp/turbo-cache/deployment-examples/terraform/scripts/cas.json /root/cas.json &&
        sudo mv /tmp/turbo-cache/deployment-examples/terraform/scripts/worker.json /root/worker.json &&
        sudo mv /tmp/turbo-cache/deployment-examples/terraform/scripts/start_turbo_cache.sh /root/start_turbo_cache.sh &&
        sudo chmod +x /root/start_turbo_cache.sh &&
        sudo mv /tmp/turbo-cache/deployment-examples/terraform/scripts/turbo-cache.service /etc/systemd/system/turbo-cache.service &&
        sudo systemctl enable turbo-cache &&
        sync
      EOT
    ]
  }
}

resource "aws_ami_from_instance" "base_ami" {
  for_each = {
    arm = "arm",
    x86 = "x86"
  }

  name               = "turbo_cache_${each.key}_base"
  source_instance_id = aws_instance.build_turbo_cache_instance[each.key].id
  # If we reboot the instance it will terminate the instance because of turbo-cache.service file.
  # So, we can control if the instance should terminate only by if the instance will reboot.
  snapshot_without_reboot = !var.terminate_ami_builder
}

# -- Begin Base AMI ---
