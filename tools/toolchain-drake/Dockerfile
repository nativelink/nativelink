# Copyright 2022-2024 The NativeLink Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM ubuntu:22.04@sha256:19478ce7fc2ffbce89df29fea5725a8d12e57de52eb9ea570890dc5852aac1ac AS dependencies
# hadolint ignore=DL3009
RUN apt-get update && DEBIAN_FRONTEND=noninteractive \
    apt-get install --no-install-recommends -y \
        git=1:2.34.1-1ubuntu1.11 \
        ca-certificates=20230311ubuntu0.22.04.1 && \
        update-ca-certificates

FROM dependencies AS final

# hadolint ignore=DL3003
RUN git clone https://github.com/blakehatch/drake.git && \
    cd drake && \
    DEBIAN_FRONTEND=noninteractive ./setup/ubuntu/install_prereqs.sh --with-bazel -y
