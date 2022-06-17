FROM ubuntu:20.04 AS builder

# Install bazel and needed deps.
RUN apt update && \
    apt install --no-install-recommends -y apt-transport-https curl gnupg ca-certificates && \
    curl -fsSL https://bazel.build/bazel-release.pub.gpg | gpg --dearmor >bazel-archive-keyring.gpg && \
    mv bazel-archive-keyring.gpg /usr/share/keyrings && \
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/bazel-archive-keyring.gpg] https://storage.googleapis.com/bazel-apt stable jdk1.8" | tee /etc/apt/sources.list.d/bazel.list

# Install build dependencies.
RUN apt update && \
    apt install --no-install-recommends -y \
        bazel \
        git \
        pkg-config \
        libssl-dev \
        python3

WORKDIR /root/turbo-cache
ADD . .

# Compile `cas` binary.
RUN bazel fetch //cas
RUN bazel build --compilation_mode=opt //cas && \
    mkdir -p ./build && \
    cp ./bazel-bin/cas/cas /root/cas

# Go back to a fresh ubuntu container and copy only the compiled binary.
FROM ubuntu:20.04
COPY --from=builder /root/cas /usr/local/bin/cas

# Install runtime packages.
RUN apt update && \
    apt install --no-install-recommends -y \
        libssl-dev

RUN mkdir -p /root/.cache/turbo-cache

EXPOSE 50051

CMD ["cas"]
