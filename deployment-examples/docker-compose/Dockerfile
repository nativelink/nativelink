# Copyright 2022-2023 The NativeLink Authors. All rights reserved.
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

# Override this if you want to run on a different version of ubuntu.
ARG OS_VERSION=24.04
# `--compilation_mode` to pass into bazel (eg: opt, dbg, fastbuild).
ARG OPT_LEVEL=opt
# Additional bazel flags.
ARG ADDITIONAL_BAZEL_FLAGS=
# Bash arguments my be passed in here to install any additional dependencies
# needed by the user. Useful if your worker needs specific dependencies installed.
ARG ADDITIONAL_SETUP_WORKER_CMD=

FROM ubuntu:${OS_VERSION} AS dependencies
ARG OS_VERSION
RUN apt-get update \
    && if [ "${OS_VERSION}" = "22.04" ]; then \
        DEBIAN_FRONTEND=noninteractive \
        apt-get install --no-install-recommends -y \
            npm=8.5.1~ds-1 \
            git=1:2.34.1-1ubuntu1.11 \
            gcc=4:11.2.0-1ubuntu1 \
            g++=4:11.2.0-1ubuntu1 \
            python3=3.10.6-1~22.04 \
            ca-certificates=20230311ubuntu0.22.04.1; \
    elif [ "${OS_VERSION}" = "20.04" ]; then \
        DEBIAN_FRONTEND=noninteractive \
        apt-get install --no-install-recommends -y \
            npm=6.14.4+ds-1ubuntu2 \
            git=1:2.25.1-1ubuntu3.13 \
            gcc=4:9.3.0-1ubuntu2 \
            g++=4:9.3.0-1ubuntu2 \
            python3=3.8.2-0ubuntu2 \
            ca-certificates=20230311ubuntu0.20.04.1; \
    else \
        echo "Unsupported OS version: ${OS_VERSION}" >&2; \
        exit 1; \
    fi \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && npm install -g @bazel/bazelisk@1.19.0

# Build the binary.
FROM dependencies AS builder
WORKDIR /root/nativelink
COPY . .
ARG OPT_LEVEL
ARG ADDITIONAL_BAZEL_FLAGS
RUN bazel build -c ${OPT_LEVEL} ${ADDITIONAL_BAZEL_FLAGS} nativelink && \
    cp ./bazel-bin/nativelink /root/nativelink-bin

# Go back to a fresh ubuntu container and copy only the compiled binary.
FROM ubuntu:${OS_VERSION} as final
ARG OS_VERSION
COPY --from=builder /root/nativelink-bin /usr/local/bin/nativelink

ARG ADDITIONAL_SETUP_WORKER_CMD

RUN bash -ueo pipefail -c "${ADDITIONAL_SETUP_WORKER_CMD}" \
    && mkdir -p /root/.cache/nativelink

EXPOSE 50051/tcp 50052/tcp
CMD ["nativelink"]
