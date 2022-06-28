# Copyright 2022 Nathan (Blaise) Bruer.  All rights reserved.

resource "tls_private_key" "ssh_key" {
  algorithm = "ED25519"
}

resource "aws_key_pair" "turbo_cache_key" {
  key_name   = "turbo-cache-key"
  public_key = data.tls_public_key.turbo_cache_pem.public_key_openssh
}

data "tls_public_key" "turbo_cache_pem" {
  private_key_openssh = tls_private_key.ssh_key.public_key_openssh
  # This is left here for convenience. Comment out the line above and uncomment and modify this
  # line to use a custom SSH key.
  # private_key_openssh = file(pathexpand("~/.ssh/id_rsa"))
}
