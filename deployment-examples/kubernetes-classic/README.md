# Kubernetes example

This deployment sets up a 3-container deployment with separate CAS, scheduler
and worker. Don't use this example deployment in production. It's insecure.

In this example we're using `kind` to set up the cluster `cilium` to provide a
`LoadBalancer` and `GatewayController`.

*Requirements:*

- An `x86_64-linux` system
- Nix installed and the nix flake active
- A functional container setup with `docker buildx` support

First set up a local development cluster. In this example we're using `kind` to
set up the cluster `cilium` to provide a `LoadBalancer` and `GatewayController`:

```bash
./00_infra.sh
```

Next, configure the `Gateway` and import the container images for the NativeLink
deployment into the cluster:

```bash
./01_operations.sh
```

The images are:

- A minimal OCI image containing a statically built `nativelink` executable.
  This makes up the `nativelink-scheduler` and `nativelink-cas` deployments.
- A worker image which is derived from a classic Ubuntu image with a C++
  toolchain. It's then wrapped with an additional container layer to add the
  `nativelink` executable.

Finally, deploy NativeLink:

```bash
./02_application.sh
```

> [!TIP]
> You can use `./03_delete_application.sh` to remove just the `nativelink`
> deployments but leave the rest of the cluster intact.

This demo setup creates two gateways to expose the `cas` and `scheduler`
deployments via your local docker network:

```bash
CACHE=$(kubectl get gtw cache -o=jsonpath='{.status.addresses[0].value}')
SCHEDULER=$(kubectl get gtw scheduler -o=jsonpath='{.status.addresses[0].value}')

echo "Cache IP: $CACHE"
echo "Scheduler IP: $SCHEDULER"

# Prints something like:
#
# Cache IP: 172.20.255.4
# Scheduler IP: 172.20.255.5
```

You can now pass these IP addresses to your Bazel invocation to use the remote
cache and executor:

```bash
bazel build \
    --config=rbe-classic \
    --remote_instance_name=main \
    --remote_cache=grpc://$CACHE:50051 \
    --remote_executor=grpc://$SCHEDULER:50052 \
    //local-remote-execution/examples:hello_lre
```

> [!TIP]
> You can add these flags to a to a `.bazelrc.user` file in the workspace root.
> Note that you'll need to pass in explicit IP addresses as this file can't
> resolve environment variables:
> ```bash
> # .bazelrc.user
> build --config=rbe-classic
> build --remote_instance_name=main
> build --remote_cache=grpc://172.20.255.4:50051
> build --remote_executor=grpc://172.20.255.5:50052
> ```

The `rbe-classic` configuration is set up in a way that every `cc_*` target is
built with the `@classic-rbe//generated/config:cc-toolchain` on an executor that
matches the `@classic-rbe//generated/config:platform`:

```
# .bazelrc
build:classic-rbe --extra_execution_platforms=@classic-rbe//generated/config:platform
build:classic-rbe --extra_toolchains=@classic-rbe//generated/config:cc-toolchain
build:classic-rbe --define=EXECUTOR=remote
build:classic-rbe --repo_env=BAZEL_DO_NOT_DETECT_CPP_TOOLCHAIN=1
build:classic-rbe --action_env=PATH=/usr/bin
```

> [!TIP]
> The [kubernetes-lre](../kubernetes) example goes into more detail on the
> actual toolchain/platform generation process.

The [generated platform](./generated/config/BUILD) is set up to require an
execution property like so:

```python
platform(
    name = "platform",
    constraint_values = [
        "@platforms//os:linux",
        "@platforms//cpu:x86_64",
        "@bazel_tools//tools/cpp:clang",
    ],
    exec_properties = {
        "container-image": "docker://classic-rbe:latest",
        "OSFamily": "Linux",
    },
    parents = ["@local_config_platform//:host"],
)
```

The [`worker.json`](./worker.json) configuration for the worker matches that
string so that the NativeLink scheduler can dispatch work to it:

```json
"platform_properties": {
  "OSFamily": {
    "values": ["Linux"]
  },
  "container-image": {
    "values": [
      // WARNING: This is *not* the container that is actually deployed
      // here. The generator container in this example was
      // `rbe-classic:latest`.
      //
      // WARNING: This example doesn't pin the toolchain to an explicit
      // tag. Using a setup like this in production would lead to
      // catastrophic cache poisoning if the underlying container changed.
      //
      // Treat the `docker//:...` string below as nothing more than a raw
      // string that is matched by the scheduler against the value
      // specified in the `exec_properties` of the corresponding platform
      // at `deployment-examples/kubernetes-classic/generated-cc/config/BUILD`.
      "docker://classic-rbe:latest",
    ]
  }
}
```

When you're done testing, delete the cluster:

```bash
kind delete cluster
```
