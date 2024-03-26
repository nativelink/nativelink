FROM ubuntu:24.04@sha256:69ce9399fe60c5710baee8416b7991183653c3d577afc2b0e3bfe508d7c76142

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive \
        apt-get install --no-install-recommends -y \
            golang=2:1.22~2 \
            git=1:2.43.0-1ubuntu1 \
            gcc=4:13.2.0-7ubuntu1 \
            g++=4:13.2.0-7ubuntu1 \
            python3=3.12.1-0ubuntu2 \
            ca-certificates=20240203 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && GOBIN=/usr/local/bin go install \
        github.com/bazelbuild/bazelisk@c7c6c19799ff408c48bdce6b7557217ad0050b17
