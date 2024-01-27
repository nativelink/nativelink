"""# //local-remote-execution/examples:typst.bzl

A trivial example toolcahin and rule.
"""

def _typst_toolchain_impl(ctx):
    return [
        platform_common.ToolchainInfo(
            typst_compiler = ctx.executable.typst_compiler,
        ),
    ]

typst_toolchain = rule(
    implementation = _typst_toolchain_impl,
    executable = False,
    attrs = {
        "typst_compiler": attr.label(
            doc = "The typst compiler.",
            executable = True,
            allow_single_file = True,
            default = "@typst//:bin/typst",
            cfg = "exec",
        ),
    },
)

def _typst_binary_impl(ctx):
    toolchain = ctx.toolchains["@nativelink//local-remote-execution/examples:toolchain_type"]

    out_file = ctx.actions.declare_file(ctx.label.name + ".pdf")

    ctx.actions.run(
        outputs = [out_file],
        inputs = ctx.files.srcs,
        executable = toolchain.typst_compiler,
        arguments = [ctx.actions.args().add_all(["c", ctx.files.srcs[0], out_file])],
        mnemonic = "TypstCompile",
        use_default_shell_env = False,
    )

    return [
        DefaultInfo(
            files = depset([out_file]),
        ),
    ]

typst_doc = rule(
    implementation = _typst_binary_impl,
    executable = False,
    toolchains = [
        "@nativelink//local-remote-execution/examples:toolchain_type",
    ],
    attrs = {
        "srcs": attr.label_list(
            doc = """Compilable source files for this target.

            Files must end with `.typ`.
            """,
            allow_files = [".typ"],
        ),
    },
)
