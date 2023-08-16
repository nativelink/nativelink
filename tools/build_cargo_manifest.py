# Copyright 2023 The Turbo Cache Authors. All rights reserved.
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
"""Generates a Cargo.toml file for each rust_library in the workspace.

This enables the project to be compiled with Bazel and with Cargo.
"""
import os
import xml.etree.ElementTree as ET
import sys
import shutil
import subprocess
from importlib.util import spec_from_loader, module_from_spec
from importlib.machinery import SourceFileLoader

ROOT_DIR = os.path.normpath(os.path.dirname(os.path.realpath(__file__)) + '/..')
# This is some voodo magic that treats packages.bzl as a python module.
# This only works because skylark is a subset of python and can be used
# as both a python module and a bazel rule.
package_full_path = ROOT_DIR + "/tools/cargo_shared.bzl"
spec = spec_from_loader("cargo_shared", SourceFileLoader("cargo_shared", package_full_path))
cargo_shared = module_from_spec(spec)
spec.loader.exec_module(cargo_shared)
PACKAGES = cargo_shared.PACKAGES
RUST_EDITION = cargo_shared.RUST_EDITION


_CARGO_MANIFEST_TEMPLATE = """\
# This file is automatically generated from `tools/build_cargo_manifest.py`.
# If you want to add a dependency add it to `tools/cargo_shared.bzl`
# then run `python tools/build_cargo_manifest.py`.
# Do not edit this file directly.

[package]
name = "{name}"
version = "0.0.0"
edition = "{RUST_EDITION}"
autobins = false
autoexamples = false
autotests = false
autobenches = false

{type}
name = "{name}"
path = "{path}"
# TODO(allada) We should support doctests.
doctest = false

[dependencies]
{external_deps}

# Local libraries.
{local_deps}
"""

_CARGO_ROOT_MANIFEST_TEMPLATE = """\
# This file is automatically generated from `tools/build_cargo_manifest.py`.
# If you want to add a dependency add it to `tools/cargo_shared.bzl`
# then run `python tools/build_cargo_manifest.py`.
# Do not edit this file directly.

[profile.release]
lto = true
opt-level = 3

[workspace]
resolver = "2"
members = [
{members}
]

[workspace.dependencies]
{external_deps}

# Local libraries.
{local_deps}
"""


def bazel_query(query):
    """Runs a bazel query and returns the results as a list of strings with the xml."""
    result = subprocess.run(
        query + " --output xml",
        shell=True,
        check=True,
        capture_output=True,
    )
    if result.returncode != 0:
        print(result.stderr.decode("utf-8"), file=sys.stderr)
        print(result.stdout.decode("utf-8"), file=sys.stdout)
        raise Exception("bazel query failed")
    root = ET.fromstring(result.stdout)
    rules = [node.attrib['name'] for node in root.findall("./rule")]
    return (rules, root)


def make_external_dep(name):
    """Returns a string that can be used in a Cargo.toml file for an external dependency."""
    package = PACKAGES[name]
    if "features" not in package:
        return f'"{package["version"]}"'
    features = [f'"{feature}\"' for feature in package['features']]
    return f"{{ version = \"{package['version']}\", features = [{', '.join(features)}] }}"


def find_path(name, xml):
    """Returns the path to the root library file in the workspace."""
    query_results = xml.findall(f"./rule[@name='{name}']/list[@name='srcs']/label")
    srcs = [node.attrib['value'] for node in query_results]
    srcs = [src[len("//"):].replace(":", "/") for src in srcs]
    if len(srcs) == 1:
        return f"../../{srcs[0]}"
    for src in srcs:
        if src.endswith("/lib.rs"):
            return f"../../{src}"
    assert False, f"Could not find sutable library to use in {srcs}"


def get_type(name, xml):
    """Returns the rust type label for Cargo.toml."""
    query_results = xml.findall(f"./rule[@name='{name}']")
    assert len(query_results) == 1, f"Expected to find one rule for {name}"
    if query_results[0].attrib['class'] == "rust_library":
        return "[lib]"
    if query_results[0].attrib['class'] == "rust_binary":
        return "[[bin]]"
    if query_results[0].attrib['class'] == "rust_test":
        return "[[test]]"
    assert False, f"Unknown rule type '{query_results[0].attrib['class']}'"


def label_to_name(label):
    """Utility to convert the bazel label to a name."""
    return label.rsplit(":", 1)[-1]


def main():
    bazel_libs, _ = bazel_query("\n".join([
        f"bazel query '",
        f"kind(\"rust_library\", //...) union",
        f"kind(\"rust_binary\", //...) union",
        f"kind(\"rust_test\", //...)",
        f"'",
    ]))

    if os.path.isdir(f"{ROOT_DIR}/cargo"):
        shutil.rmtree(f"{ROOT_DIR}/cargo")

    # This is used to track if we have already generated a library.
    lib_names = set(PACKAGES.keys())
    i = 0
    for lib in bazel_libs:
        print(f"Generating {i} of {len(bazel_libs)} {lib}")
        deps, lib_xml = bazel_query(" ".join([
            f"bazel query '",
            f"kind(\"rust_library\", deps({lib}, 1)) union",
            f"kind(\"rust_binary\", deps({lib}, 1)) union",
            f"kind(\"rust_test\", deps({lib}, 1)) union",
            f"kind(\"alias\", deps({lib}, 1))",
            f"'"
        ]))
        external_deps = []
        local_labels = []
        for dep in deps:
            if dep == lib:
                continue # Never inclue self.
            if dep.startswith("@crate_index"):
                external_deps.append(dep[len("@crate_index//:"):])
            elif dep.startswith("//"):
                local_labels.append(dep)

        lib_name = label_to_name(lib)
        assert lib_name not in lib_names, f"Library name collision '{lib_name}'"

        cargo_toml = _CARGO_MANIFEST_TEMPLATE.format(
            name=lib_name,
            type=get_type(lib, lib_xml),
            path=find_path(lib, lib_xml),
            local_deps="\n".join([f"{label_to_name(label)} = {{ workspace = true }}" for label in local_labels]),
            external_deps="\n".join([f"{name} = {{ workspace = true }}" for name in external_deps]),
            RUST_EDITION=RUST_EDITION,
        )
        os.makedirs(f"{ROOT_DIR}/gencargo/{lib_name}", exist_ok=True)
        with open(f"{ROOT_DIR}/gencargo/{lib_name}/Cargo.toml", "w+") as f:
            f.write(cargo_toml)
        lib_names.add(lib_name)
        i += 1

    with open(f"{ROOT_DIR}/Cargo.toml", "w+") as f:
        lib_names = [label_to_name(lib) for lib in bazel_libs]
        lib_names.sort()
        package_names = list(PACKAGES.keys())
        package_names.sort()
        f.write(_CARGO_ROOT_MANIFEST_TEMPLATE.format(
            members="\n".join([f'  "gencargo/{name}",' for name in lib_names]),
            local_deps="\n".join([f'{name} = {{ path = "gencargo/{name}" }}' for name in lib_names]),
            external_deps="\n".join([f'{name} = {make_external_dep(name)}' for name in package_names]),
        ))

main()
