# visionOS example

This deployment sets up a 4-container deployment with separate CAS, scheduler
and worker. Don't use this example deployment in production. It's insecure.

> [!WARNING]
> - **The visionOS simulator must be installed in the platforms section of your XCode settings before proceeding.**
>
> ![visionOS platforms](https://developer.apple.com/documentation/xcode/installing-additional-simulator-runtimes)
>
> - The client build request must be done from macOS or else it won't work, `./03_build_visionOS_tests.sh`. It will check if the image is macOS and fail if not.
> - This tutorial has been tested in a Nix environment of version `2.
> 21.0`.
> - You need to install the [Docker](https://docs.docker.com/engine/install/ubuntu/) Engine in Ubuntu.
> - To get your Nix environment set up see the [official Nix installation documentation](https://nix.dev/install-nix).

All commands should be run from nix to ensure all dependencies exist in the environment.
## NativeLink Community
If you have any questions, please reach out to the [NativeLink Community](https://join.slack.com/t/nativelink/shared_invite/zt-2i2mipfr5-lZAEeWYEy4Eru94b3IOcdg).
