// Copyright 2023 The NativeLink Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![doc = include_str!("../README.md")]
#![doc = "\n\n"] // Add some spacing between the two README files
#![doc = "
<details>
  <summary>Example Configuration Breakdown</summary>
"]
#![doc = include_str!("../examples/README.md")]
#![doc = "</details>"]
#![doc = "
<details>
  <summary>Example Kubernetes Configuraton Breakdown</summary>
"]
#![doc = "
<details>
  <summary>CAS JSON Config</summary>
"]
#![doc = "\n"]
#![doc = "```json"]
#![doc = include_str!("../../deployment-examples/kubernetes/cas.json")]
#![doc = "```"]
#![doc = "\n"]
#![doc = "</details>"]
#![doc = "
<details>
  <summary>Scheduler JSON Config</summary>
"]
#![doc = "\n"]
#![doc = "```json"]
#![doc = include_str!("../../deployment-examples/kubernetes/scheduler.json")]
#![doc = "```"]
#![doc = "\n"]
#![doc = "</details>"]
#![doc = "
<details>
  <summary>Scheduler YAML</summary>
"]
#![doc = "\n"]
#![doc = "```yaml"]
#![doc = "# Test"]
#![doc = include_str!("../../deployment-examples/kubernetes/scheduler.yaml")]
#![doc = "```"]
#![doc = "\n"]
#![doc = "</details>"]
#![doc = "
<details>
  <summary>Worker JSON</summary>
"]
#![doc = "\n"]
#![doc = "```yaml"]
#![doc = "# Test"]
#![doc = include_str!("../../deployment-examples/kubernetes/worker.json")]
#![doc = "```"]
#![doc = "\n"]
#![doc = "</details>"]
#![doc = "
<details>
  <summary>CAS YAML</summary>
"]
#![doc = "\n"]
#![doc = "```yaml"]
#![doc = include_str!("../../deployment-examples/kubernetes/cas.yaml")]
#![doc = "```"]
#![doc = "\n"]
#![doc = "</details>"]
#![doc = include_str!("../../deployment-examples/kubernetes/README.md")]
#![doc = "</details>"]
#![doc = "\n\n"] // Add some spacing between the two README files
#![doc = "</details>"]

pub mod cas_server;
pub mod schedulers;
pub mod serde_utils;
pub mod stores;
