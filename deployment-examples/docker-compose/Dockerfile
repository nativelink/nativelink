# Copyright 2022 Nathan (Blaise) Bruer.  All rights reserved.
FROM ubuntu:20.04 AS builder

# Install bazel and needed deps.
RUN apt update && \
    DEBIAN_FRONTEND=noninteractive apt install --no-install-recommends -y \
        npm \
        git \
        pkg-config \
        libssl-dev \
        gcc \
        g++ \
        python3 && \
    npm install -g @bazel/bazelisk

WORKDIR /root/turbo-cache
ADD . .

# Compile `cas` binary.
RUN bazel build --compilation_mode=opt //cas && \
    cp ./bazel-bin/cas/cas /root/turbo-cache-bin

# Go back to a fresh ubuntu container and copy only the compiled binary.
FROM ubuntu:20.04
COPY --from=builder /root/turbo-cache-bin /usr/local/bin/turbo-cache

# Install runtime packages.
RUN apt update && \
    DEBIAN_FRONTEND=noninteractive apt install --no-install-recommends -y \
        libssl-dev

RUN mkdir -p /root/.cache/turbo-cache

EXPOSE 50051/tcp 50052/tcp

CMD ["turbo-cache"]
