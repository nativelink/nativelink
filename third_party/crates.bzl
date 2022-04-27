"""
@generated
cargo-raze generated Bazel file.

DO NOT EDIT! Replaced on runs of cargo-raze
"""

load("@bazel_tools//tools/build_defs/repo:git.bzl", "new_git_repository")  # buildifier: disable=load
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")  # buildifier: disable=load
load("@bazel_tools//tools/build_defs/repo:utils.bzl", "maybe")  # buildifier: disable=load

def raze_fetch_remote_crates():
    """This function defines a collection of repos and should be called in a WORKSPACE file"""
    maybe(
        http_archive,
        name = "raze__ahash__0_7_6",
        url = "https://crates.io/api/v1/crates/ahash/0.7.6/download",
        type = "tar.gz",
        sha256 = "fcb51a0695d8f838b1ee009b3fbf66bda078cd64590202a864a8f3e8c4315c47",
        strip_prefix = "ahash-0.7.6",
        build_file = Label("//third_party/remote:BUILD.ahash-0.7.6.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__aho_corasick__0_7_18",
        url = "https://crates.io/api/v1/crates/aho-corasick/0.7.18/download",
        type = "tar.gz",
        sha256 = "1e37cfd5e7657ada45f742d6e99ca5788580b5c529dc78faf11ece6dc702656f",
        strip_prefix = "aho-corasick-0.7.18",
        build_file = Label("//third_party/remote:BUILD.aho-corasick-0.7.18.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__annotate_snippets__0_8_0",
        url = "https://crates.io/api/v1/crates/annotate-snippets/0.8.0/download",
        type = "tar.gz",
        sha256 = "d78ea013094e5ea606b1c05fe35f1dd7ea1eb1ea259908d040b25bd5ec677ee5",
        strip_prefix = "annotate-snippets-0.8.0",
        build_file = Label("//third_party/remote:BUILD.annotate-snippets-0.8.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__ansi_term__0_12_1",
        url = "https://crates.io/api/v1/crates/ansi_term/0.12.1/download",
        type = "tar.gz",
        sha256 = "d52a9bb7ec0cf484c551830a7ce27bd20d67eac647e1befb56b0be4ee39a55d2",
        strip_prefix = "ansi_term-0.12.1",
        build_file = Label("//third_party/remote:BUILD.ansi_term-0.12.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__anyhow__1_0_57",
        url = "https://crates.io/api/v1/crates/anyhow/1.0.57/download",
        type = "tar.gz",
        sha256 = "08f9b8508dccb7687a1d6c4ce66b2b0ecef467c94667de27d8d7fe1f8d2a9cdc",
        strip_prefix = "anyhow-1.0.57",
        build_file = Label("//third_party/remote:BUILD.anyhow-1.0.57.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__async_stream__0_3_3",
        url = "https://crates.io/api/v1/crates/async-stream/0.3.3/download",
        type = "tar.gz",
        sha256 = "dad5c83079eae9969be7fadefe640a1c566901f05ff91ab221de4b6f68d9507e",
        strip_prefix = "async-stream-0.3.3",
        build_file = Label("//third_party/remote:BUILD.async-stream-0.3.3.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__async_stream_impl__0_3_3",
        url = "https://crates.io/api/v1/crates/async-stream-impl/0.3.3/download",
        type = "tar.gz",
        sha256 = "10f203db73a71dfa2fb6dd22763990fa26f3d2625a6da2da900d23b87d26be27",
        strip_prefix = "async-stream-impl-0.3.3",
        build_file = Label("//third_party/remote:BUILD.async-stream-impl-0.3.3.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__async_trait__0_1_53",
        url = "https://crates.io/api/v1/crates/async-trait/0.1.53/download",
        type = "tar.gz",
        sha256 = "ed6aa3524a2dfcf9fe180c51eae2b58738348d819517ceadf95789c51fff7600",
        strip_prefix = "async-trait-0.1.53",
        build_file = Label("//third_party/remote:BUILD.async-trait-0.1.53.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__atty__0_2_14",
        url = "https://crates.io/api/v1/crates/atty/0.2.14/download",
        type = "tar.gz",
        sha256 = "d9b39be18770d11421cdb1b9947a45dd3f37e93092cbf377614828a319d5fee8",
        strip_prefix = "atty-0.2.14",
        build_file = Label("//third_party/remote:BUILD.atty-0.2.14.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__autocfg__1_1_0",
        url = "https://crates.io/api/v1/crates/autocfg/1.1.0/download",
        type = "tar.gz",
        sha256 = "d468802bab17cbc0cc575e9b053f41e72aa36bfa6b7f55e3529ffa43161b97fa",
        strip_prefix = "autocfg-1.1.0",
        build_file = Label("//third_party/remote:BUILD.autocfg-1.1.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__axum__0_5_4",
        url = "https://crates.io/api/v1/crates/axum/0.5.4/download",
        type = "tar.gz",
        sha256 = "f4af7447fc1214c1f3a1ace861d0216a6c8bb13965b64bbad9650f375b67689a",
        strip_prefix = "axum-0.5.4",
        build_file = Label("//third_party/remote:BUILD.axum-0.5.4.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__axum_core__0_2_3",
        url = "https://crates.io/api/v1/crates/axum-core/0.2.3/download",
        type = "tar.gz",
        sha256 = "3bdc19781b16e32f8a7200368a336fa4509d4b72ef15dd4e41df5290855ee1e6",
        strip_prefix = "axum-core-0.2.3",
        build_file = Label("//third_party/remote:BUILD.axum-core-0.2.3.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__base_x__0_2_10",
        url = "https://crates.io/api/v1/crates/base-x/0.2.10/download",
        type = "tar.gz",
        sha256 = "dc19a4937b4fbd3fe3379793130e42060d10627a360f2127802b10b87e7baf74",
        strip_prefix = "base-x-0.2.10",
        build_file = Label("//third_party/remote:BUILD.base-x-0.2.10.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__base64__0_13_0",
        url = "https://crates.io/api/v1/crates/base64/0.13.0/download",
        type = "tar.gz",
        sha256 = "904dfeac50f3cdaba28fc6f57fdcddb75f49ed61346676a78c4ffe55877802fd",
        strip_prefix = "base64-0.13.0",
        build_file = Label("//third_party/remote:BUILD.base64-0.13.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__bincode__1_3_3",
        url = "https://crates.io/api/v1/crates/bincode/1.3.3/download",
        type = "tar.gz",
        sha256 = "b1f45e9417d87227c7a56d22e471c6206462cba514c7590c09aff4cf6d1ddcad",
        strip_prefix = "bincode-1.3.3",
        build_file = Label("//third_party/remote:BUILD.bincode-1.3.3.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__bitflags__1_3_2",
        url = "https://crates.io/api/v1/crates/bitflags/1.3.2/download",
        type = "tar.gz",
        sha256 = "bef38d45163c2f1dde094a7dfd33ccf595c92905c8f8f4fdc18d06fb1037718a",
        strip_prefix = "bitflags-1.3.2",
        build_file = Label("//third_party/remote:BUILD.bitflags-1.3.2.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__block_buffer__0_7_3",
        url = "https://crates.io/api/v1/crates/block-buffer/0.7.3/download",
        type = "tar.gz",
        sha256 = "c0940dc441f31689269e10ac70eb1002a3a1d3ad1390e030043662eb7fe4688b",
        strip_prefix = "block-buffer-0.7.3",
        build_file = Label("//third_party/remote:BUILD.block-buffer-0.7.3.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__block_buffer__0_9_0",
        url = "https://crates.io/api/v1/crates/block-buffer/0.9.0/download",
        type = "tar.gz",
        sha256 = "4152116fd6e9dadb291ae18fc1ec3575ed6d84c29642d97890f4b4a3417297e4",
        strip_prefix = "block-buffer-0.9.0",
        build_file = Label("//third_party/remote:BUILD.block-buffer-0.9.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__block_padding__0_1_5",
        url = "https://crates.io/api/v1/crates/block-padding/0.1.5/download",
        type = "tar.gz",
        sha256 = "fa79dedbb091f449f1f39e53edf88d5dbe95f895dae6135a8d7b881fb5af73f5",
        strip_prefix = "block-padding-0.1.5",
        build_file = Label("//third_party/remote:BUILD.block-padding-0.1.5.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__bstr__0_2_17",
        url = "https://crates.io/api/v1/crates/bstr/0.2.17/download",
        type = "tar.gz",
        sha256 = "ba3569f383e8f1598449f1a423e72e99569137b47740b1da11ef19af3d5c3223",
        strip_prefix = "bstr-0.2.17",
        build_file = Label("//third_party/remote:BUILD.bstr-0.2.17.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__bumpalo__3_9_1",
        url = "https://crates.io/api/v1/crates/bumpalo/3.9.1/download",
        type = "tar.gz",
        sha256 = "a4a45a46ab1f2412e53d3a0ade76ffad2025804294569aae387231a0cd6e0899",
        strip_prefix = "bumpalo-3.9.1",
        build_file = Label("//third_party/remote:BUILD.bumpalo-3.9.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__byte_tools__0_3_1",
        url = "https://crates.io/api/v1/crates/byte-tools/0.3.1/download",
        type = "tar.gz",
        sha256 = "e3b5ca7a04898ad4bcd41c90c5285445ff5b791899bb1b0abdd2a2aa791211d7",
        strip_prefix = "byte-tools-0.3.1",
        build_file = Label("//third_party/remote:BUILD.byte-tools-0.3.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__bytecount__0_6_2",
        url = "https://crates.io/api/v1/crates/bytecount/0.6.2/download",
        type = "tar.gz",
        sha256 = "72feb31ffc86498dacdbd0fcebb56138e7177a8cc5cea4516031d15ae85a742e",
        strip_prefix = "bytecount-0.6.2",
        build_file = Label("//third_party/remote:BUILD.bytecount-0.6.2.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__byteorder__1_4_3",
        url = "https://crates.io/api/v1/crates/byteorder/1.4.3/download",
        type = "tar.gz",
        sha256 = "14c189c53d098945499cdfa7ecc63567cf3886b3332b312a5b4585d8d3a6a610",
        strip_prefix = "byteorder-1.4.3",
        build_file = Label("//third_party/remote:BUILD.byteorder-1.4.3.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__bytes__1_1_0",
        url = "https://crates.io/api/v1/crates/bytes/1.1.0/download",
        type = "tar.gz",
        sha256 = "c4872d67bab6358e59559027aa3b9157c53d9358c51423c17554809a8858e0f8",
        strip_prefix = "bytes-1.1.0",
        build_file = Label("//third_party/remote:BUILD.bytes-1.1.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__camino__1_0_7",
        url = "https://crates.io/api/v1/crates/camino/1.0.7/download",
        type = "tar.gz",
        sha256 = "6f3132262930b0522068049f5870a856ab8affc80c70d08b6ecb785771a6fc23",
        strip_prefix = "camino-1.0.7",
        build_file = Label("//third_party/remote:BUILD.camino-1.0.7.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__cargo_platform__0_1_2",
        url = "https://crates.io/api/v1/crates/cargo-platform/0.1.2/download",
        type = "tar.gz",
        sha256 = "cbdb825da8a5df079a43676dbe042702f1707b1109f713a01420fbb4cc71fa27",
        strip_prefix = "cargo-platform-0.1.2",
        build_file = Label("//third_party/remote:BUILD.cargo-platform-0.1.2.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__cargo_metadata__0_14_2",
        url = "https://crates.io/api/v1/crates/cargo_metadata/0.14.2/download",
        type = "tar.gz",
        sha256 = "4acbb09d9ee8e23699b9634375c72795d095bf268439da88562cf9b501f181fa",
        strip_prefix = "cargo_metadata-0.14.2",
        build_file = Label("//third_party/remote:BUILD.cargo_metadata-0.14.2.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__cc__1_0_73",
        url = "https://crates.io/api/v1/crates/cc/1.0.73/download",
        type = "tar.gz",
        sha256 = "2fff2a6927b3bb87f9595d67196a70493f627687a71d87a0d692242c33f58c11",
        strip_prefix = "cc-1.0.73",
        build_file = Label("//third_party/remote:BUILD.cc-1.0.73.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__cfg_if__0_1_10",
        url = "https://crates.io/api/v1/crates/cfg-if/0.1.10/download",
        type = "tar.gz",
        sha256 = "4785bdd1c96b2a846b2bd7cc02e86b6b3dbf14e7e53446c4f54c92a361040822",
        strip_prefix = "cfg-if-0.1.10",
        build_file = Label("//third_party/remote:BUILD.cfg-if-0.1.10.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__cfg_if__1_0_0",
        url = "https://crates.io/api/v1/crates/cfg-if/1.0.0/download",
        type = "tar.gz",
        sha256 = "baf1de4339761588bc0619e3cbc0120ee582ebb74b53b4efbf79117bd2da40fd",
        strip_prefix = "cfg-if-1.0.0",
        build_file = Label("//third_party/remote:BUILD.cfg-if-1.0.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__chrono__0_4_19",
        url = "https://crates.io/api/v1/crates/chrono/0.4.19/download",
        type = "tar.gz",
        sha256 = "670ad68c9088c2a963aaa298cb369688cf3f9465ce5e2d4ca10e6e0098a1ce73",
        strip_prefix = "chrono-0.4.19",
        build_file = Label("//third_party/remote:BUILD.chrono-0.4.19.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__clap__2_34_0",
        url = "https://crates.io/api/v1/crates/clap/2.34.0/download",
        type = "tar.gz",
        sha256 = "a0610544180c38b88101fecf2dd634b174a62eef6946f84dfc6a7127512b381c",
        strip_prefix = "clap-2.34.0",
        build_file = Label("//third_party/remote:BUILD.clap-2.34.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__clap__3_1_12",
        url = "https://crates.io/api/v1/crates/clap/3.1.12/download",
        type = "tar.gz",
        sha256 = "7c167e37342afc5f33fd87bbc870cedd020d2a6dffa05d45ccd9241fbdd146db",
        strip_prefix = "clap-3.1.12",
        build_file = Label("//third_party/remote:BUILD.clap-3.1.12.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__clap_derive__3_1_7",
        url = "https://crates.io/api/v1/crates/clap_derive/3.1.7/download",
        type = "tar.gz",
        sha256 = "a3aab4734e083b809aaf5794e14e756d1c798d2c69c7f7de7a09a2f5214993c1",
        strip_prefix = "clap_derive-3.1.7",
        build_file = Label("//third_party/remote:BUILD.clap_derive-3.1.7.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__clap_lex__0_1_1",
        url = "https://crates.io/api/v1/crates/clap_lex/0.1.1/download",
        type = "tar.gz",
        sha256 = "189ddd3b5d32a70b35e7686054371742a937b0d99128e76dde6340210e966669",
        strip_prefix = "clap_lex-0.1.1",
        build_file = Label("//third_party/remote:BUILD.clap_lex-0.1.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__cmake__0_1_48",
        url = "https://crates.io/api/v1/crates/cmake/0.1.48/download",
        type = "tar.gz",
        sha256 = "e8ad8cef104ac57b68b89df3208164d228503abbdce70f6880ffa3d970e7443a",
        strip_prefix = "cmake-0.1.48",
        build_file = Label("//third_party/remote:BUILD.cmake-0.1.48.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__const_fn__0_4_9",
        url = "https://crates.io/api/v1/crates/const_fn/0.4.9/download",
        type = "tar.gz",
        sha256 = "fbdcdcb6d86f71c5e97409ad45898af11cbc995b4ee8112d59095a28d376c935",
        strip_prefix = "const_fn-0.4.9",
        build_file = Label("//third_party/remote:BUILD.const_fn-0.4.9.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__core_foundation__0_9_3",
        url = "https://crates.io/api/v1/crates/core-foundation/0.9.3/download",
        type = "tar.gz",
        sha256 = "194a7a9e6de53fa55116934067c844d9d749312f75c6f6d0980e8c252f8c2146",
        strip_prefix = "core-foundation-0.9.3",
        build_file = Label("//third_party/remote:BUILD.core-foundation-0.9.3.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__core_foundation_sys__0_8_3",
        url = "https://crates.io/api/v1/crates/core-foundation-sys/0.8.3/download",
        type = "tar.gz",
        sha256 = "5827cebf4670468b8772dd191856768aedcb1b0278a04f989f7766351917b9dc",
        strip_prefix = "core-foundation-sys-0.8.3",
        build_file = Label("//third_party/remote:BUILD.core-foundation-sys-0.8.3.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__cpufeatures__0_2_2",
        url = "https://crates.io/api/v1/crates/cpufeatures/0.2.2/download",
        type = "tar.gz",
        sha256 = "59a6001667ab124aebae2a495118e11d30984c3a653e99d86d58971708cf5e4b",
        strip_prefix = "cpufeatures-0.2.2",
        build_file = Label("//third_party/remote:BUILD.cpufeatures-0.2.2.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__crc32fast__1_3_2",
        url = "https://crates.io/api/v1/crates/crc32fast/1.3.2/download",
        type = "tar.gz",
        sha256 = "b540bd8bc810d3885c6ea91e2018302f68baba2129ab3e88f32389ee9370880d",
        strip_prefix = "crc32fast-1.3.2",
        build_file = Label("//third_party/remote:BUILD.crc32fast-1.3.2.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__crossbeam_utils__0_8_8",
        url = "https://crates.io/api/v1/crates/crossbeam-utils/0.8.8/download",
        type = "tar.gz",
        sha256 = "0bf124c720b7686e3c2663cf54062ab0f68a88af2fb6a030e87e30bf721fcb38",
        strip_prefix = "crossbeam-utils-0.8.8",
        build_file = Label("//third_party/remote:BUILD.crossbeam-utils-0.8.8.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__crypto_mac__0_10_1",
        url = "https://crates.io/api/v1/crates/crypto-mac/0.10.1/download",
        type = "tar.gz",
        sha256 = "bff07008ec701e8028e2ceb8f83f0e4274ee62bd2dbdc4fefff2e9a91824081a",
        strip_prefix = "crypto-mac-0.10.1",
        build_file = Label("//third_party/remote:BUILD.crypto-mac-0.10.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__ctor__0_1_22",
        url = "https://crates.io/api/v1/crates/ctor/0.1.22/download",
        type = "tar.gz",
        sha256 = "f877be4f7c9f246b183111634f75baa039715e3f46ce860677d3b19a69fb229c",
        strip_prefix = "ctor-0.1.22",
        build_file = Label("//third_party/remote:BUILD.ctor-0.1.22.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__derive_new__0_5_9",
        url = "https://crates.io/api/v1/crates/derive-new/0.5.9/download",
        type = "tar.gz",
        sha256 = "3418329ca0ad70234b9735dc4ceed10af4df60eff9c8e7b06cb5e520d92c3535",
        strip_prefix = "derive-new-0.5.9",
        build_file = Label("//third_party/remote:BUILD.derive-new-0.5.9.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__diff__0_1_12",
        url = "https://crates.io/api/v1/crates/diff/0.1.12/download",
        type = "tar.gz",
        sha256 = "0e25ea47919b1560c4e3b7fe0aaab9becf5b84a10325ddf7db0f0ba5e1026499",
        strip_prefix = "diff-0.1.12",
        build_file = Label("//third_party/remote:BUILD.diff-0.1.12.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__digest__0_8_1",
        url = "https://crates.io/api/v1/crates/digest/0.8.1/download",
        type = "tar.gz",
        sha256 = "f3d0c8c8752312f9713efd397ff63acb9f85585afbf179282e720e7704954dd5",
        strip_prefix = "digest-0.8.1",
        build_file = Label("//third_party/remote:BUILD.digest-0.8.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__digest__0_9_0",
        url = "https://crates.io/api/v1/crates/digest/0.9.0/download",
        type = "tar.gz",
        sha256 = "d3dd60d1080a57a05ab032377049e0591415d2b31afd7028356dbf3cc6dcb066",
        strip_prefix = "digest-0.9.0",
        build_file = Label("//third_party/remote:BUILD.digest-0.9.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__dirs__2_0_2",
        url = "https://crates.io/api/v1/crates/dirs/2.0.2/download",
        type = "tar.gz",
        sha256 = "13aea89a5c93364a98e9b37b2fa237effbb694d5cfe01c5b70941f7eb087d5e3",
        strip_prefix = "dirs-2.0.2",
        build_file = Label("//third_party/remote:BUILD.dirs-2.0.2.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__dirs_next__2_0_0",
        url = "https://crates.io/api/v1/crates/dirs-next/2.0.0/download",
        type = "tar.gz",
        sha256 = "b98cf8ebf19c3d1b223e151f99a4f9f0690dca41414773390fc824184ac833e1",
        strip_prefix = "dirs-next-2.0.0",
        build_file = Label("//third_party/remote:BUILD.dirs-next-2.0.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__dirs_sys__0_3_7",
        url = "https://crates.io/api/v1/crates/dirs-sys/0.3.7/download",
        type = "tar.gz",
        sha256 = "1b1d1d91c932ef41c0f2663aa8b0ca0342d444d842c06914aa0a7e352d0bada6",
        strip_prefix = "dirs-sys-0.3.7",
        build_file = Label("//third_party/remote:BUILD.dirs-sys-0.3.7.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__dirs_sys_next__0_1_2",
        url = "https://crates.io/api/v1/crates/dirs-sys-next/0.1.2/download",
        type = "tar.gz",
        sha256 = "4ebda144c4fe02d1f7ea1a7d9641b6fc6b580adcfa024ae48797ecdeb6825b4d",
        strip_prefix = "dirs-sys-next-0.1.2",
        build_file = Label("//third_party/remote:BUILD.dirs-sys-next-0.1.2.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__discard__1_0_4",
        url = "https://crates.io/api/v1/crates/discard/1.0.4/download",
        type = "tar.gz",
        sha256 = "212d0f5754cb6769937f4501cc0e67f4f4483c8d2c3e1e922ee9edbe4ab4c7c0",
        strip_prefix = "discard-1.0.4",
        build_file = Label("//third_party/remote:BUILD.discard-1.0.4.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__either__1_6_1",
        url = "https://crates.io/api/v1/crates/either/1.6.1/download",
        type = "tar.gz",
        sha256 = "e78d4f1cc4ae33bbfc157ed5d5a5ef3bc29227303d595861deb238fcec4e9457",
        strip_prefix = "either-1.6.1",
        build_file = Label("//third_party/remote:BUILD.either-1.6.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__env_logger__0_8_4",
        url = "https://crates.io/api/v1/crates/env_logger/0.8.4/download",
        type = "tar.gz",
        sha256 = "a19187fea3ac7e84da7dacf48de0c45d63c6a76f9490dae389aead16c243fce3",
        strip_prefix = "env_logger-0.8.4",
        build_file = Label("//third_party/remote:BUILD.env_logger-0.8.4.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__env_logger__0_9_0",
        url = "https://crates.io/api/v1/crates/env_logger/0.9.0/download",
        type = "tar.gz",
        sha256 = "0b2cf0344971ee6c64c31be0d530793fba457d322dfec2810c453d0ef228f9c3",
        strip_prefix = "env_logger-0.9.0",
        build_file = Label("//third_party/remote:BUILD.env_logger-0.9.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__fake_simd__0_1_2",
        url = "https://crates.io/api/v1/crates/fake-simd/0.1.2/download",
        type = "tar.gz",
        sha256 = "e88a8acf291dafb59c2d96e8f59828f3838bb1a70398823ade51a84de6a6deed",
        strip_prefix = "fake-simd-0.1.2",
        build_file = Label("//third_party/remote:BUILD.fake-simd-0.1.2.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__fast_async_mutex__0_6_7",
        url = "https://crates.io/api/v1/crates/fast-async-mutex/0.6.7/download",
        type = "tar.gz",
        sha256 = "b877ceff2e3d2922823bab7960826ce198181f9c25f75d67b913fde9237e2da6",
        strip_prefix = "fast-async-mutex-0.6.7",
        build_file = Label("//third_party/remote:BUILD.fast-async-mutex-0.6.7.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__fastrand__1_7_0",
        url = "https://crates.io/api/v1/crates/fastrand/1.7.0/download",
        type = "tar.gz",
        sha256 = "c3fcf0cee53519c866c09b5de1f6c56ff9d647101f81c1964fa632e148896cdf",
        strip_prefix = "fastrand-1.7.0",
        build_file = Label("//third_party/remote:BUILD.fastrand-1.7.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__filetime__0_2_16",
        url = "https://crates.io/api/v1/crates/filetime/0.2.16/download",
        type = "tar.gz",
        sha256 = "c0408e2626025178a6a7f7ffc05a25bc47103229f19c113755de7bf63816290c",
        strip_prefix = "filetime-0.2.16",
        build_file = Label("//third_party/remote:BUILD.filetime-0.2.16.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__fixed_buffer__0_2_3",
        url = "https://crates.io/api/v1/crates/fixed-buffer/0.2.3/download",
        type = "tar.gz",
        sha256 = "ee27d08e4f11444330d000b8620aec3eae623d5bedd3667f65a2ee871e93dfdb",
        strip_prefix = "fixed-buffer-0.2.3",
        build_file = Label("//third_party/remote:BUILD.fixed-buffer-0.2.3.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__fixedbitset__0_4_1",
        url = "https://crates.io/api/v1/crates/fixedbitset/0.4.1/download",
        type = "tar.gz",
        sha256 = "279fb028e20b3c4c320317955b77c5e0c9701f05a1d309905d6fc702cdc5053e",
        strip_prefix = "fixedbitset-0.4.1",
        build_file = Label("//third_party/remote:BUILD.fixedbitset-0.4.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__fnv__1_0_7",
        url = "https://crates.io/api/v1/crates/fnv/1.0.7/download",
        type = "tar.gz",
        sha256 = "3f9eec918d3f24069decb9af1554cad7c880e2da24a9afd88aca000531ab82c1",
        strip_prefix = "fnv-1.0.7",
        build_file = Label("//third_party/remote:BUILD.fnv-1.0.7.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__foreign_types__0_3_2",
        url = "https://crates.io/api/v1/crates/foreign-types/0.3.2/download",
        type = "tar.gz",
        sha256 = "f6f339eb8adc052cd2ca78910fda869aefa38d22d5cb648e6485e4d3fc06f3b1",
        strip_prefix = "foreign-types-0.3.2",
        build_file = Label("//third_party/remote:BUILD.foreign-types-0.3.2.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__foreign_types_shared__0_1_1",
        url = "https://crates.io/api/v1/crates/foreign-types-shared/0.1.1/download",
        type = "tar.gz",
        sha256 = "00b0228411908ca8685dba7fc2cdd70ec9990a6e753e89b6ac91a84c40fbaf4b",
        strip_prefix = "foreign-types-shared-0.1.1",
        build_file = Label("//third_party/remote:BUILD.foreign-types-shared-0.1.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__futures__0_3_21",
        url = "https://crates.io/api/v1/crates/futures/0.3.21/download",
        type = "tar.gz",
        sha256 = "f73fe65f54d1e12b726f517d3e2135ca3125a437b6d998caf1962961f7172d9e",
        strip_prefix = "futures-0.3.21",
        build_file = Label("//third_party/remote:BUILD.futures-0.3.21.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__futures_channel__0_3_21",
        url = "https://crates.io/api/v1/crates/futures-channel/0.3.21/download",
        type = "tar.gz",
        sha256 = "c3083ce4b914124575708913bca19bfe887522d6e2e6d0952943f5eac4a74010",
        strip_prefix = "futures-channel-0.3.21",
        build_file = Label("//third_party/remote:BUILD.futures-channel-0.3.21.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__futures_core__0_3_21",
        url = "https://crates.io/api/v1/crates/futures-core/0.3.21/download",
        type = "tar.gz",
        sha256 = "0c09fd04b7e4073ac7156a9539b57a484a8ea920f79c7c675d05d289ab6110d3",
        strip_prefix = "futures-core-0.3.21",
        build_file = Label("//third_party/remote:BUILD.futures-core-0.3.21.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__futures_executor__0_3_21",
        url = "https://crates.io/api/v1/crates/futures-executor/0.3.21/download",
        type = "tar.gz",
        sha256 = "9420b90cfa29e327d0429f19be13e7ddb68fa1cccb09d65e5706b8c7a749b8a6",
        strip_prefix = "futures-executor-0.3.21",
        build_file = Label("//third_party/remote:BUILD.futures-executor-0.3.21.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__futures_io__0_3_21",
        url = "https://crates.io/api/v1/crates/futures-io/0.3.21/download",
        type = "tar.gz",
        sha256 = "fc4045962a5a5e935ee2fdedaa4e08284547402885ab326734432bed5d12966b",
        strip_prefix = "futures-io-0.3.21",
        build_file = Label("//third_party/remote:BUILD.futures-io-0.3.21.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__futures_macro__0_3_21",
        url = "https://crates.io/api/v1/crates/futures-macro/0.3.21/download",
        type = "tar.gz",
        sha256 = "33c1e13800337f4d4d7a316bf45a567dbcb6ffe087f16424852d97e97a91f512",
        strip_prefix = "futures-macro-0.3.21",
        build_file = Label("//third_party/remote:BUILD.futures-macro-0.3.21.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__futures_sink__0_3_21",
        url = "https://crates.io/api/v1/crates/futures-sink/0.3.21/download",
        type = "tar.gz",
        sha256 = "21163e139fa306126e6eedaf49ecdb4588f939600f0b1e770f4205ee4b7fa868",
        strip_prefix = "futures-sink-0.3.21",
        build_file = Label("//third_party/remote:BUILD.futures-sink-0.3.21.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__futures_task__0_3_21",
        url = "https://crates.io/api/v1/crates/futures-task/0.3.21/download",
        type = "tar.gz",
        sha256 = "57c66a976bf5909d801bbef33416c41372779507e7a6b3a5e25e4749c58f776a",
        strip_prefix = "futures-task-0.3.21",
        build_file = Label("//third_party/remote:BUILD.futures-task-0.3.21.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__futures_util__0_3_21",
        url = "https://crates.io/api/v1/crates/futures-util/0.3.21/download",
        type = "tar.gz",
        sha256 = "d8b7abd5d659d9b90c8cba917f6ec750a74e2dc23902ef9cd4cc8c8b22e6036a",
        strip_prefix = "futures-util-0.3.21",
        build_file = Label("//third_party/remote:BUILD.futures-util-0.3.21.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__generic_array__0_12_4",
        url = "https://crates.io/api/v1/crates/generic-array/0.12.4/download",
        type = "tar.gz",
        sha256 = "ffdf9f34f1447443d37393cc6c2b8313aebddcd96906caf34e54c68d8e57d7bd",
        strip_prefix = "generic-array-0.12.4",
        build_file = Label("//third_party/remote:BUILD.generic-array-0.12.4.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__generic_array__0_14_5",
        url = "https://crates.io/api/v1/crates/generic-array/0.14.5/download",
        type = "tar.gz",
        sha256 = "fd48d33ec7f05fbfa152300fdad764757cbded343c1aa1cff2fbaf4134851803",
        strip_prefix = "generic-array-0.14.5",
        build_file = Label("//third_party/remote:BUILD.generic-array-0.14.5.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__getopts__0_2_21",
        url = "https://crates.io/api/v1/crates/getopts/0.2.21/download",
        type = "tar.gz",
        sha256 = "14dbbfd5c71d70241ecf9e6f13737f7b5ce823821063188d7e46c41d371eebd5",
        strip_prefix = "getopts-0.2.21",
        build_file = Label("//third_party/remote:BUILD.getopts-0.2.21.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__getrandom__0_2_6",
        url = "https://crates.io/api/v1/crates/getrandom/0.2.6/download",
        type = "tar.gz",
        sha256 = "9be70c98951c83b8d2f8f60d7065fa6d5146873094452a1008da8c2f1e4205ad",
        strip_prefix = "getrandom-0.2.6",
        build_file = Label("//third_party/remote:BUILD.getrandom-0.2.6.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__globset__0_4_8",
        url = "https://crates.io/api/v1/crates/globset/0.4.8/download",
        type = "tar.gz",
        sha256 = "10463d9ff00a2a068db14231982f5132edebad0d7660cd956a1c30292dbcbfbd",
        strip_prefix = "globset-0.4.8",
        build_file = Label("//third_party/remote:BUILD.globset-0.4.8.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__h2__0_3_13",
        url = "https://crates.io/api/v1/crates/h2/0.3.13/download",
        type = "tar.gz",
        sha256 = "37a82c6d637fc9515a4694bbf1cb2457b79d81ce52b3108bdeea58b07dd34a57",
        strip_prefix = "h2-0.3.13",
        build_file = Label("//third_party/remote:BUILD.h2-0.3.13.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__hashbrown__0_11_2",
        url = "https://crates.io/api/v1/crates/hashbrown/0.11.2/download",
        type = "tar.gz",
        sha256 = "ab5ef0d4909ef3724cc8cce6ccc8572c5c817592e9285f5464f8e86f8bd3726e",
        strip_prefix = "hashbrown-0.11.2",
        build_file = Label("//third_party/remote:BUILD.hashbrown-0.11.2.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__heck__0_3_3",
        url = "https://crates.io/api/v1/crates/heck/0.3.3/download",
        type = "tar.gz",
        sha256 = "6d621efb26863f0e9924c6ac577e8275e5e6b77455db64ffa6c65c904e9e132c",
        strip_prefix = "heck-0.3.3",
        build_file = Label("//third_party/remote:BUILD.heck-0.3.3.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__heck__0_4_0",
        url = "https://crates.io/api/v1/crates/heck/0.4.0/download",
        type = "tar.gz",
        sha256 = "2540771e65fc8cb83cd6e8a237f70c319bd5c29f78ed1084ba5d50eeac86f7f9",
        strip_prefix = "heck-0.4.0",
        build_file = Label("//third_party/remote:BUILD.heck-0.4.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__hermit_abi__0_1_19",
        url = "https://crates.io/api/v1/crates/hermit-abi/0.1.19/download",
        type = "tar.gz",
        sha256 = "62b467343b94ba476dcb2500d242dadbb39557df889310ac77c5d99100aaac33",
        strip_prefix = "hermit-abi-0.1.19",
        build_file = Label("//third_party/remote:BUILD.hermit-abi-0.1.19.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__hex__0_4_3",
        url = "https://crates.io/api/v1/crates/hex/0.4.3/download",
        type = "tar.gz",
        sha256 = "7f24254aa9a54b5c858eaee2f5bccdb46aaf0e486a595ed5fd8f86ba55232a70",
        strip_prefix = "hex-0.4.3",
        build_file = Label("//third_party/remote:BUILD.hex-0.4.3.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__hmac__0_10_1",
        url = "https://crates.io/api/v1/crates/hmac/0.10.1/download",
        type = "tar.gz",
        sha256 = "c1441c6b1e930e2817404b5046f1f989899143a12bf92de603b69f4e0aee1e15",
        strip_prefix = "hmac-0.10.1",
        build_file = Label("//third_party/remote:BUILD.hmac-0.10.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__http__0_2_6",
        url = "https://crates.io/api/v1/crates/http/0.2.6/download",
        type = "tar.gz",
        sha256 = "31f4c6746584866f0feabcc69893c5b51beef3831656a968ed7ae254cdc4fd03",
        strip_prefix = "http-0.2.6",
        build_file = Label("//third_party/remote:BUILD.http-0.2.6.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__http_body__0_4_4",
        url = "https://crates.io/api/v1/crates/http-body/0.4.4/download",
        type = "tar.gz",
        sha256 = "1ff4f84919677303da5f147645dbea6b1881f368d03ac84e1dc09031ebd7b2c6",
        strip_prefix = "http-body-0.4.4",
        build_file = Label("//third_party/remote:BUILD.http-body-0.4.4.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__http_range_header__0_3_0",
        url = "https://crates.io/api/v1/crates/http-range-header/0.3.0/download",
        type = "tar.gz",
        sha256 = "0bfe8eed0a9285ef776bb792479ea3834e8b94e13d615c2f66d03dd50a435a29",
        strip_prefix = "http-range-header-0.3.0",
        build_file = Label("//third_party/remote:BUILD.http-range-header-0.3.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__httparse__1_7_1",
        url = "https://crates.io/api/v1/crates/httparse/1.7.1/download",
        type = "tar.gz",
        sha256 = "496ce29bb5a52785b44e0f7ca2847ae0bb839c9bd28f69acac9b99d461c0c04c",
        strip_prefix = "httparse-1.7.1",
        build_file = Label("//third_party/remote:BUILD.httparse-1.7.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__httpdate__1_0_2",
        url = "https://crates.io/api/v1/crates/httpdate/1.0.2/download",
        type = "tar.gz",
        sha256 = "c4a1e36c821dbe04574f602848a19f742f4fb3c98d40449f11bcad18d6b17421",
        strip_prefix = "httpdate-1.0.2",
        build_file = Label("//third_party/remote:BUILD.httpdate-1.0.2.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__humantime__2_1_0",
        url = "https://crates.io/api/v1/crates/humantime/2.1.0/download",
        type = "tar.gz",
        sha256 = "9a3a5bfb195931eeb336b2a7b4d761daec841b97f947d34394601737a7bba5e4",
        strip_prefix = "humantime-2.1.0",
        build_file = Label("//third_party/remote:BUILD.humantime-2.1.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__hyper__0_14_18",
        url = "https://crates.io/api/v1/crates/hyper/0.14.18/download",
        type = "tar.gz",
        sha256 = "b26ae0a80afebe130861d90abf98e3814a4f28a4c6ffeb5ab8ebb2be311e0ef2",
        strip_prefix = "hyper-0.14.18",
        build_file = Label("//third_party/remote:BUILD.hyper-0.14.18.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__hyper_timeout__0_4_1",
        url = "https://crates.io/api/v1/crates/hyper-timeout/0.4.1/download",
        type = "tar.gz",
        sha256 = "bbb958482e8c7be4bc3cf272a766a2b0bf1a6755e7a6ae777f017a31d11b13b1",
        strip_prefix = "hyper-timeout-0.4.1",
        build_file = Label("//third_party/remote:BUILD.hyper-timeout-0.4.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__hyper_tls__0_5_0",
        url = "https://crates.io/api/v1/crates/hyper-tls/0.5.0/download",
        type = "tar.gz",
        sha256 = "d6183ddfa99b85da61a140bea0efc93fdf56ceaa041b37d553518030827f9905",
        strip_prefix = "hyper-tls-0.5.0",
        build_file = Label("//third_party/remote:BUILD.hyper-tls-0.5.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__ignore__0_4_18",
        url = "https://crates.io/api/v1/crates/ignore/0.4.18/download",
        type = "tar.gz",
        sha256 = "713f1b139373f96a2e0ce3ac931cd01ee973c3c5dd7c40c0c2efe96ad2b6751d",
        strip_prefix = "ignore-0.4.18",
        build_file = Label("//third_party/remote:BUILD.ignore-0.4.18.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__indexmap__1_8_1",
        url = "https://crates.io/api/v1/crates/indexmap/1.8.1/download",
        type = "tar.gz",
        sha256 = "0f647032dfaa1f8b6dc29bd3edb7bbef4861b8b8007ebb118d6db284fd59f6ee",
        strip_prefix = "indexmap-1.8.1",
        build_file = Label("//third_party/remote:BUILD.indexmap-1.8.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__instant__0_1_12",
        url = "https://crates.io/api/v1/crates/instant/0.1.12/download",
        type = "tar.gz",
        sha256 = "7a5bbe824c507c5da5956355e86a746d82e0e1464f65d862cc5e71da70e94b2c",
        strip_prefix = "instant-0.1.12",
        build_file = Label("//third_party/remote:BUILD.instant-0.1.12.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__itertools__0_10_3",
        url = "https://crates.io/api/v1/crates/itertools/0.10.3/download",
        type = "tar.gz",
        sha256 = "a9a9d19fa1e79b6215ff29b9d6880b706147f16e9b1dbb1e4e5947b5b02bc5e3",
        strip_prefix = "itertools-0.10.3",
        build_file = Label("//third_party/remote:BUILD.itertools-0.10.3.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__itertools__0_9_0",
        url = "https://crates.io/api/v1/crates/itertools/0.9.0/download",
        type = "tar.gz",
        sha256 = "284f18f85651fe11e8a991b2adb42cb078325c996ed026d994719efcfca1d54b",
        strip_prefix = "itertools-0.9.0",
        build_file = Label("//third_party/remote:BUILD.itertools-0.9.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__itoa__1_0_1",
        url = "https://crates.io/api/v1/crates/itoa/1.0.1/download",
        type = "tar.gz",
        sha256 = "1aab8fc367588b89dcee83ab0fd66b72b50b72fa1904d7095045ace2b0c81c35",
        strip_prefix = "itoa-1.0.1",
        build_file = Label("//third_party/remote:BUILD.itoa-1.0.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__json5__0_3_0",
        url = "https://crates.io/api/v1/crates/json5/0.3.0/download",
        type = "tar.gz",
        sha256 = "3d993b17585f39e5e3bd98ff52bbd9e2a6d6b3f5b09d8abcec9d1873fb04cf3f",
        strip_prefix = "json5-0.3.0",
        build_file = Label("//third_party/remote:BUILD.json5-0.3.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__lazy_init__0_5_0",
        url = "https://crates.io/api/v1/crates/lazy-init/0.5.0/download",
        type = "tar.gz",
        sha256 = "23517540be87a91d06324e6bf6286ba8214171123ee8862ae9a5e7d938d71815",
        strip_prefix = "lazy-init-0.5.0",
        build_file = Label("//third_party/remote:BUILD.lazy-init-0.5.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__lazy_static__1_4_0",
        url = "https://crates.io/api/v1/crates/lazy_static/1.4.0/download",
        type = "tar.gz",
        sha256 = "e2abad23fbc42b3700f2f279844dc832adb2b2eb069b2df918f455c4e18cc646",
        strip_prefix = "lazy_static-1.4.0",
        build_file = Label("//third_party/remote:BUILD.lazy_static-1.4.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__libc__0_2_124",
        url = "https://crates.io/api/v1/crates/libc/0.2.124/download",
        type = "tar.gz",
        sha256 = "21a41fed9d98f27ab1c6d161da622a4fa35e8a54a8adc24bbf3ddd0ef70b0e50",
        strip_prefix = "libc-0.2.124",
        build_file = Label("//third_party/remote:BUILD.libc-0.2.124.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__lock_api__0_4_7",
        url = "https://crates.io/api/v1/crates/lock_api/0.4.7/download",
        type = "tar.gz",
        sha256 = "327fa5b6a6940e4699ec49a9beae1ea4845c6bab9314e4f84ac68742139d8c53",
        strip_prefix = "lock_api-0.4.7",
        build_file = Label("//third_party/remote:BUILD.lock_api-0.4.7.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__log__0_4_16",
        url = "https://crates.io/api/v1/crates/log/0.4.16/download",
        type = "tar.gz",
        sha256 = "6389c490849ff5bc16be905ae24bc913a9c8892e19b2341dbc175e14c341c2b8",
        strip_prefix = "log-0.4.16",
        build_file = Label("//third_party/remote:BUILD.log-0.4.16.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__lru__0_6_6",
        url = "https://crates.io/api/v1/crates/lru/0.6.6/download",
        type = "tar.gz",
        sha256 = "7ea2d928b485416e8908cff2d97d621db22b27f7b3b6729e438bcf42c671ba91",
        strip_prefix = "lru-0.6.6",
        build_file = Label("//third_party/remote:BUILD.lru-0.6.6.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__lz4_flex__0_9_2",
        url = "https://crates.io/api/v1/crates/lz4_flex/0.9.2/download",
        type = "tar.gz",
        sha256 = "42c51df9d8d4842336c835df1d85ed447c4813baa237d033d95128bf5552ad8a",
        strip_prefix = "lz4_flex-0.9.2",
        build_file = Label("//third_party/remote:BUILD.lz4_flex-0.9.2.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__maplit__1_0_2",
        url = "https://crates.io/api/v1/crates/maplit/1.0.2/download",
        type = "tar.gz",
        sha256 = "3e2e65a1a2e43cfcb47a895c4c8b10d1f4a61097f9f254f183aee60cad9c651d",
        strip_prefix = "maplit-1.0.2",
        build_file = Label("//third_party/remote:BUILD.maplit-1.0.2.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__matchit__0_5_0",
        url = "https://crates.io/api/v1/crates/matchit/0.5.0/download",
        type = "tar.gz",
        sha256 = "73cbba799671b762df5a175adf59ce145165747bb891505c43d09aefbbf38beb",
        strip_prefix = "matchit-0.5.0",
        build_file = Label("//third_party/remote:BUILD.matchit-0.5.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__md5__0_7_0",
        url = "https://crates.io/api/v1/crates/md5/0.7.0/download",
        type = "tar.gz",
        sha256 = "490cc448043f947bae3cbee9c203358d62dbee0db12107a74be5c30ccfd09771",
        strip_prefix = "md5-0.7.0",
        build_file = Label("//third_party/remote:BUILD.md5-0.7.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__memchr__2_4_1",
        url = "https://crates.io/api/v1/crates/memchr/2.4.1/download",
        type = "tar.gz",
        sha256 = "308cc39be01b73d0d18f82a0e7b2a3df85245f84af96fdddc5d202d27e47b86a",
        strip_prefix = "memchr-2.4.1",
        build_file = Label("//third_party/remote:BUILD.memchr-2.4.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__memoffset__0_6_5",
        url = "https://crates.io/api/v1/crates/memoffset/0.6.5/download",
        type = "tar.gz",
        sha256 = "5aa361d4faea93603064a027415f07bd8e1d5c88c9fbf68bf56a285428fd79ce",
        strip_prefix = "memoffset-0.6.5",
        build_file = Label("//third_party/remote:BUILD.memoffset-0.6.5.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__mime__0_3_16",
        url = "https://crates.io/api/v1/crates/mime/0.3.16/download",
        type = "tar.gz",
        sha256 = "2a60c7ce501c71e03a9c9c0d35b861413ae925bd979cc7a4e30d060069aaac8d",
        strip_prefix = "mime-0.3.16",
        build_file = Label("//third_party/remote:BUILD.mime-0.3.16.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__mio__0_8_2",
        url = "https://crates.io/api/v1/crates/mio/0.8.2/download",
        type = "tar.gz",
        sha256 = "52da4364ffb0e4fe33a9841a98a3f3014fb964045ce4f7a45a398243c8d6b0c9",
        strip_prefix = "mio-0.8.2",
        build_file = Label("//third_party/remote:BUILD.mio-0.8.2.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__miow__0_3_7",
        url = "https://crates.io/api/v1/crates/miow/0.3.7/download",
        type = "tar.gz",
        sha256 = "b9f1c5b025cda876f66ef43a113f91ebc9f4ccef34843000e0adf6ebbab84e21",
        strip_prefix = "miow-0.3.7",
        build_file = Label("//third_party/remote:BUILD.miow-0.3.7.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__mock_instant__0_2_1",
        url = "https://crates.io/api/v1/crates/mock_instant/0.2.1/download",
        type = "tar.gz",
        sha256 = "717e29a243b81f8130e31e24e04fb151b04a44b5a7d05370935f7d937e9de06d",
        strip_prefix = "mock_instant-0.2.1",
        build_file = Label("//third_party/remote:BUILD.mock_instant-0.2.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__multimap__0_8_3",
        url = "https://crates.io/api/v1/crates/multimap/0.8.3/download",
        type = "tar.gz",
        sha256 = "e5ce46fe64a9d73be07dcbe690a38ce1b293be448fd8ce1e6c1b8062c9f72c6a",
        strip_prefix = "multimap-0.8.3",
        build_file = Label("//third_party/remote:BUILD.multimap-0.8.3.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__native_tls__0_2_10",
        url = "https://crates.io/api/v1/crates/native-tls/0.2.10/download",
        type = "tar.gz",
        sha256 = "fd7e2f3618557f980e0b17e8856252eee3c97fa12c54dff0ca290fb6266ca4a9",
        strip_prefix = "native-tls-0.2.10",
        build_file = Label("//third_party/remote:BUILD.native-tls-0.2.10.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__nix__0_23_1",
        url = "https://crates.io/api/v1/crates/nix/0.23.1/download",
        type = "tar.gz",
        sha256 = "9f866317acbd3a240710c63f065ffb1e4fd466259045ccb504130b7f668f35c6",
        strip_prefix = "nix-0.23.1",
        build_file = Label("//third_party/remote:BUILD.nix-0.23.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__ntapi__0_3_7",
        url = "https://crates.io/api/v1/crates/ntapi/0.3.7/download",
        type = "tar.gz",
        sha256 = "c28774a7fd2fbb4f0babd8237ce554b73af68021b5f695a3cebd6c59bac0980f",
        strip_prefix = "ntapi-0.3.7",
        build_file = Label("//third_party/remote:BUILD.ntapi-0.3.7.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__num_integer__0_1_44",
        url = "https://crates.io/api/v1/crates/num-integer/0.1.44/download",
        type = "tar.gz",
        sha256 = "d2cc698a63b549a70bc047073d2949cce27cd1c7b0a4a862d08a8031bc2801db",
        strip_prefix = "num-integer-0.1.44",
        build_file = Label("//third_party/remote:BUILD.num-integer-0.1.44.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__num_traits__0_2_14",
        url = "https://crates.io/api/v1/crates/num-traits/0.2.14/download",
        type = "tar.gz",
        sha256 = "9a64b1ec5cda2586e284722486d802acf1f7dbdc623e2bfc57e65ca1cd099290",
        strip_prefix = "num-traits-0.2.14",
        build_file = Label("//third_party/remote:BUILD.num-traits-0.2.14.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__num_cpus__1_13_1",
        url = "https://crates.io/api/v1/crates/num_cpus/1.13.1/download",
        type = "tar.gz",
        sha256 = "19e64526ebdee182341572e50e9ad03965aa510cd94427a4549448f285e957a1",
        strip_prefix = "num_cpus-1.13.1",
        build_file = Label("//third_party/remote:BUILD.num_cpus-1.13.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__once_cell__1_10_0",
        url = "https://crates.io/api/v1/crates/once_cell/1.10.0/download",
        type = "tar.gz",
        sha256 = "87f3e037eac156d1775da914196f0f37741a274155e34a0b7e427c35d2a2ecb9",
        strip_prefix = "once_cell-1.10.0",
        build_file = Label("//third_party/remote:BUILD.once_cell-1.10.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__opaque_debug__0_2_3",
        url = "https://crates.io/api/v1/crates/opaque-debug/0.2.3/download",
        type = "tar.gz",
        sha256 = "2839e79665f131bdb5782e51f2c6c9599c133c6098982a54c794358bf432529c",
        strip_prefix = "opaque-debug-0.2.3",
        build_file = Label("//third_party/remote:BUILD.opaque-debug-0.2.3.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__opaque_debug__0_3_0",
        url = "https://crates.io/api/v1/crates/opaque-debug/0.3.0/download",
        type = "tar.gz",
        sha256 = "624a8340c38c1b80fd549087862da4ba43e08858af025b236e509b6649fc13d5",
        strip_prefix = "opaque-debug-0.3.0",
        build_file = Label("//third_party/remote:BUILD.opaque-debug-0.3.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__openssl__0_10_38",
        url = "https://crates.io/api/v1/crates/openssl/0.10.38/download",
        type = "tar.gz",
        sha256 = "0c7ae222234c30df141154f159066c5093ff73b63204dcda7121eb082fc56a95",
        strip_prefix = "openssl-0.10.38",
        build_file = Label("//third_party/remote:BUILD.openssl-0.10.38.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__openssl_probe__0_1_5",
        url = "https://crates.io/api/v1/crates/openssl-probe/0.1.5/download",
        type = "tar.gz",
        sha256 = "ff011a302c396a5197692431fc1948019154afc178baf7d8e37367442a4601cf",
        strip_prefix = "openssl-probe-0.1.5",
        build_file = Label("//third_party/remote:BUILD.openssl-probe-0.1.5.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__openssl_sys__0_9_72",
        url = "https://crates.io/api/v1/crates/openssl-sys/0.9.72/download",
        type = "tar.gz",
        sha256 = "7e46109c383602735fa0a2e48dd2b7c892b048e1bf69e5c3b1d804b7d9c203cb",
        strip_prefix = "openssl-sys-0.9.72",
        build_file = Label("//third_party/remote:BUILD.openssl-sys-0.9.72.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__os_str_bytes__6_0_0",
        url = "https://crates.io/api/v1/crates/os_str_bytes/6.0.0/download",
        type = "tar.gz",
        sha256 = "8e22443d1643a904602595ba1cd8f7d896afe56d26712531c5ff73a15b2fbf64",
        strip_prefix = "os_str_bytes-6.0.0",
        build_file = Label("//third_party/remote:BUILD.os_str_bytes-6.0.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__output_vt100__0_1_3",
        url = "https://crates.io/api/v1/crates/output_vt100/0.1.3/download",
        type = "tar.gz",
        sha256 = "628223faebab4e3e40667ee0b2336d34a5b960ff60ea743ddfdbcf7770bcfb66",
        strip_prefix = "output_vt100-0.1.3",
        build_file = Label("//third_party/remote:BUILD.output_vt100-0.1.3.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__parking_lot__0_12_0",
        url = "https://crates.io/api/v1/crates/parking_lot/0.12.0/download",
        type = "tar.gz",
        sha256 = "87f5ec2493a61ac0506c0f4199f99070cbe83857b0337006a30f3e6719b8ef58",
        strip_prefix = "parking_lot-0.12.0",
        build_file = Label("//third_party/remote:BUILD.parking_lot-0.12.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__parking_lot_core__0_9_2",
        url = "https://crates.io/api/v1/crates/parking_lot_core/0.9.2/download",
        type = "tar.gz",
        sha256 = "995f667a6c822200b0433ac218e05582f0e2efa1b922a3fd2fbaadc5f87bab37",
        strip_prefix = "parking_lot_core-0.9.2",
        build_file = Label("//third_party/remote:BUILD.parking_lot_core-0.9.2.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__percent_encoding__2_1_0",
        url = "https://crates.io/api/v1/crates/percent-encoding/2.1.0/download",
        type = "tar.gz",
        sha256 = "d4fd5641d01c8f18a23da7b6fe29298ff4b55afcccdf78973b24cf3175fee32e",
        strip_prefix = "percent-encoding-2.1.0",
        build_file = Label("//third_party/remote:BUILD.percent-encoding-2.1.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__pest__2_1_3",
        url = "https://crates.io/api/v1/crates/pest/2.1.3/download",
        type = "tar.gz",
        sha256 = "10f4872ae94d7b90ae48754df22fd42ad52ce740b8f370b03da4835417403e53",
        strip_prefix = "pest-2.1.3",
        build_file = Label("//third_party/remote:BUILD.pest-2.1.3.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__pest_derive__2_1_0",
        url = "https://crates.io/api/v1/crates/pest_derive/2.1.0/download",
        type = "tar.gz",
        sha256 = "833d1ae558dc601e9a60366421196a8d94bc0ac980476d0b67e1d0988d72b2d0",
        strip_prefix = "pest_derive-2.1.0",
        build_file = Label("//third_party/remote:BUILD.pest_derive-2.1.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__pest_generator__2_1_3",
        url = "https://crates.io/api/v1/crates/pest_generator/2.1.3/download",
        type = "tar.gz",
        sha256 = "99b8db626e31e5b81787b9783425769681b347011cc59471e33ea46d2ea0cf55",
        strip_prefix = "pest_generator-2.1.3",
        build_file = Label("//third_party/remote:BUILD.pest_generator-2.1.3.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__pest_meta__2_1_3",
        url = "https://crates.io/api/v1/crates/pest_meta/2.1.3/download",
        type = "tar.gz",
        sha256 = "54be6e404f5317079812fc8f9f5279de376d8856929e21c184ecf6bbd692a11d",
        strip_prefix = "pest_meta-2.1.3",
        build_file = Label("//third_party/remote:BUILD.pest_meta-2.1.3.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__petgraph__0_6_0",
        url = "https://crates.io/api/v1/crates/petgraph/0.6.0/download",
        type = "tar.gz",
        sha256 = "4a13a2fa9d0b63e5f22328828741e523766fff0ee9e779316902290dff3f824f",
        strip_prefix = "petgraph-0.6.0",
        build_file = Label("//third_party/remote:BUILD.petgraph-0.6.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__pin_project__1_0_10",
        url = "https://crates.io/api/v1/crates/pin-project/1.0.10/download",
        type = "tar.gz",
        sha256 = "58ad3879ad3baf4e44784bc6a718a8698867bb991f8ce24d1bcbe2cfb4c3a75e",
        strip_prefix = "pin-project-1.0.10",
        build_file = Label("//third_party/remote:BUILD.pin-project-1.0.10.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__pin_project_internal__1_0_10",
        url = "https://crates.io/api/v1/crates/pin-project-internal/1.0.10/download",
        type = "tar.gz",
        sha256 = "744b6f092ba29c3650faf274db506afd39944f48420f6c86b17cfe0ee1cb36bb",
        strip_prefix = "pin-project-internal-1.0.10",
        build_file = Label("//third_party/remote:BUILD.pin-project-internal-1.0.10.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__pin_project_lite__0_2_9",
        url = "https://crates.io/api/v1/crates/pin-project-lite/0.2.9/download",
        type = "tar.gz",
        sha256 = "e0a7ae3ac2f1173085d398531c705756c94a4c56843785df85a60c1a0afac116",
        strip_prefix = "pin-project-lite-0.2.9",
        build_file = Label("//third_party/remote:BUILD.pin-project-lite-0.2.9.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__pin_utils__0_1_0",
        url = "https://crates.io/api/v1/crates/pin-utils/0.1.0/download",
        type = "tar.gz",
        sha256 = "8b870d8c151b6f2fb93e84a13146138f05d02ed11c7e7c54f8826aaaf7c9f184",
        strip_prefix = "pin-utils-0.1.0",
        build_file = Label("//third_party/remote:BUILD.pin-utils-0.1.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__pkg_config__0_3_25",
        url = "https://crates.io/api/v1/crates/pkg-config/0.3.25/download",
        type = "tar.gz",
        sha256 = "1df8c4ec4b0627e53bdf214615ad287367e482558cf84b109250b37464dc03ae",
        strip_prefix = "pkg-config-0.3.25",
        build_file = Label("//third_party/remote:BUILD.pkg-config-0.3.25.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__ppv_lite86__0_2_16",
        url = "https://crates.io/api/v1/crates/ppv-lite86/0.2.16/download",
        type = "tar.gz",
        sha256 = "eb9f9e6e233e5c4a35559a617bf40a4ec447db2e84c20b55a6f83167b7e57872",
        strip_prefix = "ppv-lite86-0.2.16",
        build_file = Label("//third_party/remote:BUILD.ppv-lite86-0.2.16.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__pretty_assertions__0_7_2",
        url = "https://crates.io/api/v1/crates/pretty_assertions/0.7.2/download",
        type = "tar.gz",
        sha256 = "1cab0e7c02cf376875e9335e0ba1da535775beb5450d21e1dffca068818ed98b",
        strip_prefix = "pretty_assertions-0.7.2",
        build_file = Label("//third_party/remote:BUILD.pretty_assertions-0.7.2.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__prettyplease__0_1_10",
        url = "https://crates.io/api/v1/crates/prettyplease/0.1.10/download",
        type = "tar.gz",
        sha256 = "d9e07e3a46d0771a8a06b5f4441527802830b43e679ba12f44960f48dd4c6803",
        strip_prefix = "prettyplease-0.1.10",
        build_file = Label("//third_party/remote:BUILD.prettyplease-0.1.10.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__proc_macro_error__1_0_4",
        url = "https://crates.io/api/v1/crates/proc-macro-error/1.0.4/download",
        type = "tar.gz",
        sha256 = "da25490ff9892aab3fcf7c36f08cfb902dd3e71ca0f9f9517bea02a73a5ce38c",
        strip_prefix = "proc-macro-error-1.0.4",
        build_file = Label("//third_party/remote:BUILD.proc-macro-error-1.0.4.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__proc_macro_error_attr__1_0_4",
        url = "https://crates.io/api/v1/crates/proc-macro-error-attr/1.0.4/download",
        type = "tar.gz",
        sha256 = "a1be40180e52ecc98ad80b184934baf3d0d29f979574e439af5a55274b35f869",
        strip_prefix = "proc-macro-error-attr-1.0.4",
        build_file = Label("//third_party/remote:BUILD.proc-macro-error-attr-1.0.4.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__proc_macro_hack__0_5_19",
        url = "https://crates.io/api/v1/crates/proc-macro-hack/0.5.19/download",
        type = "tar.gz",
        sha256 = "dbf0c48bc1d91375ae5c3cd81e3722dff1abcf81a30960240640d223f59fe0e5",
        strip_prefix = "proc-macro-hack-0.5.19",
        build_file = Label("//third_party/remote:BUILD.proc-macro-hack-0.5.19.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__proc_macro2__1_0_37",
        url = "https://crates.io/api/v1/crates/proc-macro2/1.0.37/download",
        type = "tar.gz",
        sha256 = "ec757218438d5fda206afc041538b2f6d889286160d649a86a24d37e1235afd1",
        strip_prefix = "proc-macro2-1.0.37",
        build_file = Label("//third_party/remote:BUILD.proc-macro2-1.0.37.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__prost__0_10_1",
        url = "https://crates.io/api/v1/crates/prost/0.10.1/download",
        type = "tar.gz",
        sha256 = "a07b0857a71a8cb765763950499cae2413c3f9cede1133478c43600d9e146890",
        strip_prefix = "prost-0.10.1",
        build_file = Label("//third_party/remote:BUILD.prost-0.10.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__prost_build__0_10_1",
        url = "https://crates.io/api/v1/crates/prost-build/0.10.1/download",
        type = "tar.gz",
        sha256 = "120fbe7988713f39d780a58cf1a7ef0d7ef66c6d87e5aa3438940c05357929f4",
        strip_prefix = "prost-build-0.10.1",
        build_file = Label("//third_party/remote:BUILD.prost-build-0.10.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__prost_derive__0_10_1",
        url = "https://crates.io/api/v1/crates/prost-derive/0.10.1/download",
        type = "tar.gz",
        sha256 = "7b670f45da57fb8542ebdbb6105a925fe571b67f9e7ed9f47a06a84e72b4e7cc",
        strip_prefix = "prost-derive-0.10.1",
        build_file = Label("//third_party/remote:BUILD.prost-derive-0.10.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__prost_types__0_10_1",
        url = "https://crates.io/api/v1/crates/prost-types/0.10.1/download",
        type = "tar.gz",
        sha256 = "2d0a014229361011dc8e69c8a1ec6c2e8d0f2af7c91e3ea3f5b2170298461e68",
        strip_prefix = "prost-types-0.10.1",
        build_file = Label("//third_party/remote:BUILD.prost-types-0.10.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__quote__1_0_18",
        url = "https://crates.io/api/v1/crates/quote/1.0.18/download",
        type = "tar.gz",
        sha256 = "a1feb54ed693b93a84e14094943b84b7c4eae204c512b7ccb95ab0c66d278ad1",
        strip_prefix = "quote-1.0.18",
        build_file = Label("//third_party/remote:BUILD.quote-1.0.18.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__rand__0_8_5",
        url = "https://crates.io/api/v1/crates/rand/0.8.5/download",
        type = "tar.gz",
        sha256 = "34af8d1a0e25924bc5b7c43c079c942339d8f0a8b57c39049bef581b46327404",
        strip_prefix = "rand-0.8.5",
        build_file = Label("//third_party/remote:BUILD.rand-0.8.5.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__rand_chacha__0_3_1",
        url = "https://crates.io/api/v1/crates/rand_chacha/0.3.1/download",
        type = "tar.gz",
        sha256 = "e6c10a63a0fa32252be49d21e7709d4d4baf8d231c2dbce1eaa8141b9b127d88",
        strip_prefix = "rand_chacha-0.3.1",
        build_file = Label("//third_party/remote:BUILD.rand_chacha-0.3.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__rand_core__0_6_3",
        url = "https://crates.io/api/v1/crates/rand_core/0.6.3/download",
        type = "tar.gz",
        sha256 = "d34f1408f55294453790c48b2f1ebbb1c5b4b7563eb1f418bcfcfdbb06ebb4e7",
        strip_prefix = "rand_core-0.6.3",
        build_file = Label("//third_party/remote:BUILD.rand_core-0.6.3.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__redox_syscall__0_2_13",
        url = "https://crates.io/api/v1/crates/redox_syscall/0.2.13/download",
        type = "tar.gz",
        sha256 = "62f25bc4c7e55e0b0b7a1d43fb893f4fa1361d0abe38b9ce4f323c2adfe6ef42",
        strip_prefix = "redox_syscall-0.2.13",
        build_file = Label("//third_party/remote:BUILD.redox_syscall-0.2.13.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__redox_users__0_4_3",
        url = "https://crates.io/api/v1/crates/redox_users/0.4.3/download",
        type = "tar.gz",
        sha256 = "b033d837a7cf162d7993aded9304e30a83213c648b6e389db233191f891e5c2b",
        strip_prefix = "redox_users-0.4.3",
        build_file = Label("//third_party/remote:BUILD.redox_users-0.4.3.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__regex__1_5_5",
        url = "https://crates.io/api/v1/crates/regex/1.5.5/download",
        type = "tar.gz",
        sha256 = "1a11647b6b25ff05a515cb92c365cec08801e83423a235b51e231e1808747286",
        strip_prefix = "regex-1.5.5",
        build_file = Label("//third_party/remote:BUILD.regex-1.5.5.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__regex_syntax__0_6_25",
        url = "https://crates.io/api/v1/crates/regex-syntax/0.6.25/download",
        type = "tar.gz",
        sha256 = "f497285884f3fcff424ffc933e56d7cbca511def0c9831a7f9b5f6153e3cc89b",
        strip_prefix = "regex-syntax-0.6.25",
        build_file = Label("//third_party/remote:BUILD.regex-syntax-0.6.25.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__relative_path__1_7_0",
        url = "https://crates.io/api/v1/crates/relative-path/1.7.0/download",
        type = "tar.gz",
        sha256 = "b4e112eddc95bbf25365df3b5414354ad2fe7ee465eddb9965a515faf8c3b6d9",
        strip_prefix = "relative-path-1.7.0",
        build_file = Label("//third_party/remote:BUILD.relative-path-1.7.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__remove_dir_all__0_5_3",
        url = "https://crates.io/api/v1/crates/remove_dir_all/0.5.3/download",
        type = "tar.gz",
        sha256 = "3acd125665422973a33ac9d3dd2df85edad0f4ae9b00dafb1a05e43a9f5ef8e7",
        strip_prefix = "remove_dir_all-0.5.3",
        build_file = Label("//third_party/remote:BUILD.remove_dir_all-0.5.3.bazel"),
    )

    maybe(
        new_git_repository,
        name = "raze__rusoto_core__0_46_0",
        remote = "https://github.com/allada/rusoto.git",
        commit = "cc9acca00dbafa41a37d75faeaf2a4baba33d42e",
        build_file = Label("//third_party/remote:BUILD.rusoto_core-0.46.0.bazel"),
        init_submodules = True,
    )

    maybe(
        http_archive,
        name = "raze__rusoto_core__0_46_0",
        url = "https://crates.io/api/v1/crates/rusoto_core/0.46.0/download",
        type = "tar.gz",
        sha256 = "02aff20978970d47630f08de5f0d04799497818d16cafee5aec90c4b4d0806cf",
        strip_prefix = "rusoto_core-0.46.0",
        build_file = Label("//third_party/remote:BUILD.rusoto_core-0.46.0.bazel"),
    )

    maybe(
        new_git_repository,
        name = "raze__rusoto_credential__0_46_0",
        remote = "https://github.com/allada/rusoto.git",
        commit = "cc9acca00dbafa41a37d75faeaf2a4baba33d42e",
        build_file = Label("//third_party/remote:BUILD.rusoto_credential-0.46.0.bazel"),
        init_submodules = True,
    )

    maybe(
        new_git_repository,
        name = "raze__rusoto_mock__0_46_0",
        remote = "https://github.com/allada/rusoto.git",
        commit = "cc9acca00dbafa41a37d75faeaf2a4baba33d42e",
        build_file = Label("//third_party/remote:BUILD.rusoto_mock-0.46.0.bazel"),
        init_submodules = True,
    )

    maybe(
        http_archive,
        name = "raze__rusoto_s3__0_46_0",
        url = "https://crates.io/api/v1/crates/rusoto_s3/0.46.0/download",
        type = "tar.gz",
        sha256 = "abc3f56f14ccf91f880b9a9c2d0556d8523e8c155041c54db155b384a1dd1119",
        strip_prefix = "rusoto_s3-0.46.0",
        build_file = Label("//third_party/remote:BUILD.rusoto_s3-0.46.0.bazel"),
    )

    maybe(
        new_git_repository,
        name = "raze__rusoto_signature__0_46_0",
        remote = "https://github.com/allada/rusoto.git",
        commit = "cc9acca00dbafa41a37d75faeaf2a4baba33d42e",
        build_file = Label("//third_party/remote:BUILD.rusoto_signature-0.46.0.bazel"),
        init_submodules = True,
    )

    maybe(
        http_archive,
        name = "raze__rustc_workspace_hack__1_0_0",
        url = "https://crates.io/api/v1/crates/rustc-workspace-hack/1.0.0/download",
        type = "tar.gz",
        sha256 = "fc71d2faa173b74b232dedc235e3ee1696581bb132fc116fa3626d6151a1a8fb",
        strip_prefix = "rustc-workspace-hack-1.0.0",
        build_file = Label("//third_party/remote:BUILD.rustc-workspace-hack-1.0.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__rustc_version__0_2_3",
        url = "https://crates.io/api/v1/crates/rustc_version/0.2.3/download",
        type = "tar.gz",
        sha256 = "138e3e0acb6c9fb258b19b67cb8abd63c00679d2851805ea151465464fe9030a",
        strip_prefix = "rustc_version-0.2.3",
        build_file = Label("//third_party/remote:BUILD.rustc_version-0.2.3.bazel"),
    )

    maybe(
        new_git_repository,
        name = "raze__rustfmt_config_proc_macro__0_2_0",
        remote = "https://github.com/rust-lang/rustfmt",
        commit = "efa8f5521d3813cc897ba29ea0ef98c7aef66bb6",
        build_file = Label("//third_party/remote:BUILD.rustfmt-config_proc_macro-0.2.0.bazel"),
        init_submodules = True,
    )

    maybe(
        new_git_repository,
        name = "raze__rustfmt_nightly__1_4_38",
        remote = "https://github.com/rust-lang/rustfmt",
        commit = "efa8f5521d3813cc897ba29ea0ef98c7aef66bb6",
        build_file = Label("//third_party/remote:BUILD.rustfmt-nightly-1.4.38.bazel"),
        init_submodules = True,
    )

    maybe(
        http_archive,
        name = "raze__ryu__1_0_9",
        url = "https://crates.io/api/v1/crates/ryu/1.0.9/download",
        type = "tar.gz",
        sha256 = "73b4b750c782965c211b42f022f59af1fbceabdd026623714f104152f1ec149f",
        strip_prefix = "ryu-1.0.9",
        build_file = Label("//third_party/remote:BUILD.ryu-1.0.9.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__same_file__1_0_6",
        url = "https://crates.io/api/v1/crates/same-file/1.0.6/download",
        type = "tar.gz",
        sha256 = "93fc1dc3aaa9bfed95e02e6eadabb4baf7e3078b0bd1b4d7b6b0b68378900502",
        strip_prefix = "same-file-1.0.6",
        build_file = Label("//third_party/remote:BUILD.same-file-1.0.6.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__schannel__0_1_19",
        url = "https://crates.io/api/v1/crates/schannel/0.1.19/download",
        type = "tar.gz",
        sha256 = "8f05ba609c234e60bee0d547fe94a4c7e9da733d1c962cf6e59efa4cd9c8bc75",
        strip_prefix = "schannel-0.1.19",
        build_file = Label("//third_party/remote:BUILD.schannel-0.1.19.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__scopeguard__1_1_0",
        url = "https://crates.io/api/v1/crates/scopeguard/1.1.0/download",
        type = "tar.gz",
        sha256 = "d29ab0c6d3fc0ee92fe66e2d99f700eab17a8d57d1c1d3b748380fb20baa78cd",
        strip_prefix = "scopeguard-1.1.0",
        build_file = Label("//third_party/remote:BUILD.scopeguard-1.1.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__security_framework__2_6_1",
        url = "https://crates.io/api/v1/crates/security-framework/2.6.1/download",
        type = "tar.gz",
        sha256 = "2dc14f172faf8a0194a3aded622712b0de276821addc574fa54fc0a1167e10dc",
        strip_prefix = "security-framework-2.6.1",
        build_file = Label("//third_party/remote:BUILD.security-framework-2.6.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__security_framework_sys__2_6_1",
        url = "https://crates.io/api/v1/crates/security-framework-sys/2.6.1/download",
        type = "tar.gz",
        sha256 = "0160a13a177a45bfb43ce71c01580998474f556ad854dcbca936dd2841a5c556",
        strip_prefix = "security-framework-sys-2.6.1",
        build_file = Label("//third_party/remote:BUILD.security-framework-sys-2.6.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__semver__0_9_0",
        url = "https://crates.io/api/v1/crates/semver/0.9.0/download",
        type = "tar.gz",
        sha256 = "1d7eb9ef2c18661902cc47e535f9bc51b78acd254da71d375c2f6720d9a40403",
        strip_prefix = "semver-0.9.0",
        build_file = Label("//third_party/remote:BUILD.semver-0.9.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__semver__1_0_7",
        url = "https://crates.io/api/v1/crates/semver/1.0.7/download",
        type = "tar.gz",
        sha256 = "d65bd28f48be7196d222d95b9243287f48d27aca604e08497513019ff0502cc4",
        strip_prefix = "semver-1.0.7",
        build_file = Label("//third_party/remote:BUILD.semver-1.0.7.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__semver_parser__0_7_0",
        url = "https://crates.io/api/v1/crates/semver-parser/0.7.0/download",
        type = "tar.gz",
        sha256 = "388a1df253eca08550bef6c72392cfe7c30914bf41df5269b68cbd6ff8f570a3",
        strip_prefix = "semver-parser-0.7.0",
        build_file = Label("//third_party/remote:BUILD.semver-parser-0.7.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__serde__1_0_136",
        url = "https://crates.io/api/v1/crates/serde/1.0.136/download",
        type = "tar.gz",
        sha256 = "ce31e24b01e1e524df96f1c2fdd054405f8d7376249a5110886fb4b658484789",
        strip_prefix = "serde-1.0.136",
        build_file = Label("//third_party/remote:BUILD.serde-1.0.136.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__serde_derive__1_0_136",
        url = "https://crates.io/api/v1/crates/serde_derive/1.0.136/download",
        type = "tar.gz",
        sha256 = "08597e7152fcd306f41838ed3e37be9eaeed2b61c42e2117266a554fab4662f9",
        strip_prefix = "serde_derive-1.0.136",
        build_file = Label("//third_party/remote:BUILD.serde_derive-1.0.136.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__serde_json__1_0_79",
        url = "https://crates.io/api/v1/crates/serde_json/1.0.79/download",
        type = "tar.gz",
        sha256 = "8e8d9fa5c3b304765ce1fd9c4c8a3de2c8db365a5b91be52f186efc675681d95",
        strip_prefix = "serde_json-1.0.79",
        build_file = Label("//third_party/remote:BUILD.serde_json-1.0.79.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__sha_1__0_8_2",
        url = "https://crates.io/api/v1/crates/sha-1/0.8.2/download",
        type = "tar.gz",
        sha256 = "f7d94d0bede923b3cea61f3f1ff57ff8cdfd77b400fb8f9998949e0cf04163df",
        strip_prefix = "sha-1-0.8.2",
        build_file = Label("//third_party/remote:BUILD.sha-1-0.8.2.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__sha1__0_6_1",
        url = "https://crates.io/api/v1/crates/sha1/0.6.1/download",
        type = "tar.gz",
        sha256 = "c1da05c97445caa12d05e848c4a4fcbbea29e748ac28f7e80e9b010392063770",
        strip_prefix = "sha1-0.6.1",
        build_file = Label("//third_party/remote:BUILD.sha1-0.6.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__sha1_smol__1_0_0",
        url = "https://crates.io/api/v1/crates/sha1_smol/1.0.0/download",
        type = "tar.gz",
        sha256 = "ae1a47186c03a32177042e55dbc5fd5aee900b8e0069a8d70fba96a9375cd012",
        strip_prefix = "sha1_smol-1.0.0",
        build_file = Label("//third_party/remote:BUILD.sha1_smol-1.0.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__sha2__0_9_9",
        url = "https://crates.io/api/v1/crates/sha2/0.9.9/download",
        type = "tar.gz",
        sha256 = "4d58a1e1bf39749807d89cf2d98ac2dfa0ff1cb3faa38fbb64dd88ac8013d800",
        strip_prefix = "sha2-0.9.9",
        build_file = Label("//third_party/remote:BUILD.sha2-0.9.9.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__shlex__0_1_1",
        url = "https://crates.io/api/v1/crates/shlex/0.1.1/download",
        type = "tar.gz",
        sha256 = "7fdf1b9db47230893d76faad238fd6097fd6d6a9245cd7a4d90dbd639536bbd2",
        strip_prefix = "shlex-0.1.1",
        build_file = Label("//third_party/remote:BUILD.shlex-0.1.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__shlex__1_1_0",
        url = "https://crates.io/api/v1/crates/shlex/1.1.0/download",
        type = "tar.gz",
        sha256 = "43b2853a4d09f215c24cc5489c992ce46052d359b5109343cbafbf26bc62f8a3",
        strip_prefix = "shlex-1.1.0",
        build_file = Label("//third_party/remote:BUILD.shlex-1.1.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__signal_hook_registry__1_4_0",
        url = "https://crates.io/api/v1/crates/signal-hook-registry/1.4.0/download",
        type = "tar.gz",
        sha256 = "e51e73328dc4ac0c7ccbda3a494dfa03df1de2f46018127f60c693f2648455b0",
        strip_prefix = "signal-hook-registry-1.4.0",
        build_file = Label("//third_party/remote:BUILD.signal-hook-registry-1.4.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__slab__0_4_6",
        url = "https://crates.io/api/v1/crates/slab/0.4.6/download",
        type = "tar.gz",
        sha256 = "eb703cfe953bccee95685111adeedb76fabe4e97549a58d16f03ea7b9367bb32",
        strip_prefix = "slab-0.4.6",
        build_file = Label("//third_party/remote:BUILD.slab-0.4.6.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__smallvec__1_8_0",
        url = "https://crates.io/api/v1/crates/smallvec/1.8.0/download",
        type = "tar.gz",
        sha256 = "f2dd574626839106c320a323308629dcb1acfc96e32a8cba364ddc61ac23ee83",
        strip_prefix = "smallvec-1.8.0",
        build_file = Label("//third_party/remote:BUILD.smallvec-1.8.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__socket2__0_4_4",
        url = "https://crates.io/api/v1/crates/socket2/0.4.4/download",
        type = "tar.gz",
        sha256 = "66d72b759436ae32898a2af0a14218dbf55efde3feeb170eb623637db85ee1e0",
        strip_prefix = "socket2-0.4.4",
        build_file = Label("//third_party/remote:BUILD.socket2-0.4.4.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__standback__0_2_17",
        url = "https://crates.io/api/v1/crates/standback/0.2.17/download",
        type = "tar.gz",
        sha256 = "e113fb6f3de07a243d434a56ec6f186dfd51cb08448239fe7bcae73f87ff28ff",
        strip_prefix = "standback-0.2.17",
        build_file = Label("//third_party/remote:BUILD.standback-0.2.17.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__static_assertions__1_1_0",
        url = "https://crates.io/api/v1/crates/static_assertions/1.1.0/download",
        type = "tar.gz",
        sha256 = "a2eb9349b6444b326872e140eb1cf5e7c522154d69e7a0ffb0fb81c06b37543f",
        strip_prefix = "static_assertions-1.1.0",
        build_file = Label("//third_party/remote:BUILD.static_assertions-1.1.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__stdext__0_2_1",
        url = "https://crates.io/api/v1/crates/stdext/0.2.1/download",
        type = "tar.gz",
        sha256 = "4a61b4ae487cb43d0479907e74d36f8813e9940bd3b1adcbecc69fe8a0cee3ec",
        strip_prefix = "stdext-0.2.1",
        build_file = Label("//third_party/remote:BUILD.stdext-0.2.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__stdweb__0_4_20",
        url = "https://crates.io/api/v1/crates/stdweb/0.4.20/download",
        type = "tar.gz",
        sha256 = "d022496b16281348b52d0e30ae99e01a73d737b2f45d38fed4edf79f9325a1d5",
        strip_prefix = "stdweb-0.4.20",
        build_file = Label("//third_party/remote:BUILD.stdweb-0.4.20.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__stdweb_derive__0_5_3",
        url = "https://crates.io/api/v1/crates/stdweb-derive/0.5.3/download",
        type = "tar.gz",
        sha256 = "c87a60a40fccc84bef0652345bbbbbe20a605bf5d0ce81719fc476f5c03b50ef",
        strip_prefix = "stdweb-derive-0.5.3",
        build_file = Label("//third_party/remote:BUILD.stdweb-derive-0.5.3.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__stdweb_internal_macros__0_2_9",
        url = "https://crates.io/api/v1/crates/stdweb-internal-macros/0.2.9/download",
        type = "tar.gz",
        sha256 = "58fa5ff6ad0d98d1ffa8cb115892b6e69d67799f6763e162a1c9db421dc22e11",
        strip_prefix = "stdweb-internal-macros-0.2.9",
        build_file = Label("//third_party/remote:BUILD.stdweb-internal-macros-0.2.9.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__stdweb_internal_runtime__0_1_5",
        url = "https://crates.io/api/v1/crates/stdweb-internal-runtime/0.1.5/download",
        type = "tar.gz",
        sha256 = "213701ba3370744dcd1a12960caa4843b3d68b4d1c0a5d575e0d65b2ee9d16c0",
        strip_prefix = "stdweb-internal-runtime-0.1.5",
        build_file = Label("//third_party/remote:BUILD.stdweb-internal-runtime-0.1.5.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__strsim__0_10_0",
        url = "https://crates.io/api/v1/crates/strsim/0.10.0/download",
        type = "tar.gz",
        sha256 = "73473c0e59e6d5812c5dfe2a064a6444949f089e20eec9a2e5506596494e4623",
        strip_prefix = "strsim-0.10.0",
        build_file = Label("//third_party/remote:BUILD.strsim-0.10.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__strsim__0_8_0",
        url = "https://crates.io/api/v1/crates/strsim/0.8.0/download",
        type = "tar.gz",
        sha256 = "8ea5119cdb4c55b55d432abb513a0429384878c15dde60cc77b1c99de1a95a6a",
        strip_prefix = "strsim-0.8.0",
        build_file = Label("//third_party/remote:BUILD.strsim-0.8.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__structopt__0_3_26",
        url = "https://crates.io/api/v1/crates/structopt/0.3.26/download",
        type = "tar.gz",
        sha256 = "0c6b5c64445ba8094a6ab0c3cd2ad323e07171012d9c98b0b15651daf1787a10",
        strip_prefix = "structopt-0.3.26",
        build_file = Label("//third_party/remote:BUILD.structopt-0.3.26.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__structopt_derive__0_4_18",
        url = "https://crates.io/api/v1/crates/structopt-derive/0.4.18/download",
        type = "tar.gz",
        sha256 = "dcb5ae327f9cc13b68763b5749770cb9e048a99bd9dfdfa58d0cf05d5f64afe0",
        strip_prefix = "structopt-derive-0.4.18",
        build_file = Label("//third_party/remote:BUILD.structopt-derive-0.4.18.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__subtle__2_4_1",
        url = "https://crates.io/api/v1/crates/subtle/2.4.1/download",
        type = "tar.gz",
        sha256 = "6bdef32e8150c2a081110b42772ffe7d7c9032b606bc226c8260fd97e0976601",
        strip_prefix = "subtle-2.4.1",
        build_file = Label("//third_party/remote:BUILD.subtle-2.4.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__syn__1_0_91",
        url = "https://crates.io/api/v1/crates/syn/1.0.91/download",
        type = "tar.gz",
        sha256 = "b683b2b825c8eef438b77c36a06dc262294da3d5a5813fac20da149241dcd44d",
        strip_prefix = "syn-1.0.91",
        build_file = Label("//third_party/remote:BUILD.syn-1.0.91.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__sync_wrapper__0_1_1",
        url = "https://crates.io/api/v1/crates/sync_wrapper/0.1.1/download",
        type = "tar.gz",
        sha256 = "20518fe4a4c9acf048008599e464deb21beeae3d3578418951a189c235a7a9a8",
        strip_prefix = "sync_wrapper-0.1.1",
        build_file = Label("//third_party/remote:BUILD.sync_wrapper-0.1.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__tempfile__3_3_0",
        url = "https://crates.io/api/v1/crates/tempfile/3.3.0/download",
        type = "tar.gz",
        sha256 = "5cdb1ef4eaeeaddc8fbd371e5017057064af0911902ef36b39801f67cc6d79e4",
        strip_prefix = "tempfile-3.3.0",
        build_file = Label("//third_party/remote:BUILD.tempfile-3.3.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__term__0_6_1",
        url = "https://crates.io/api/v1/crates/term/0.6.1/download",
        type = "tar.gz",
        sha256 = "c0863a3345e70f61d613eab32ee046ccd1bcc5f9105fe402c61fcd0c13eeb8b5",
        strip_prefix = "term-0.6.1",
        build_file = Label("//third_party/remote:BUILD.term-0.6.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__termcolor__1_1_3",
        url = "https://crates.io/api/v1/crates/termcolor/1.1.3/download",
        type = "tar.gz",
        sha256 = "bab24d30b911b2376f3a13cc2cd443142f0c81dda04c118693e35b3835757755",
        strip_prefix = "termcolor-1.1.3",
        build_file = Label("//third_party/remote:BUILD.termcolor-1.1.3.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__textwrap__0_11_0",
        url = "https://crates.io/api/v1/crates/textwrap/0.11.0/download",
        type = "tar.gz",
        sha256 = "d326610f408c7a4eb6f51c37c330e496b08506c9457c9d34287ecc38809fb060",
        strip_prefix = "textwrap-0.11.0",
        build_file = Label("//third_party/remote:BUILD.textwrap-0.11.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__textwrap__0_15_0",
        url = "https://crates.io/api/v1/crates/textwrap/0.15.0/download",
        type = "tar.gz",
        sha256 = "b1141d4d61095b28419e22cb0bbf02755f5e54e0526f97f1e3d1d160e60885fb",
        strip_prefix = "textwrap-0.15.0",
        build_file = Label("//third_party/remote:BUILD.textwrap-0.15.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__thiserror__1_0_30",
        url = "https://crates.io/api/v1/crates/thiserror/1.0.30/download",
        type = "tar.gz",
        sha256 = "854babe52e4df1653706b98fcfc05843010039b406875930a70e4d9644e5c417",
        strip_prefix = "thiserror-1.0.30",
        build_file = Label("//third_party/remote:BUILD.thiserror-1.0.30.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__thiserror_impl__1_0_30",
        url = "https://crates.io/api/v1/crates/thiserror-impl/1.0.30/download",
        type = "tar.gz",
        sha256 = "aa32fd3f627f367fe16f893e2597ae3c05020f8bba2666a4e6ea73d377e5714b",
        strip_prefix = "thiserror-impl-1.0.30",
        build_file = Label("//third_party/remote:BUILD.thiserror-impl-1.0.30.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__thread_local__1_1_4",
        url = "https://crates.io/api/v1/crates/thread_local/1.1.4/download",
        type = "tar.gz",
        sha256 = "5516c27b78311c50bf42c071425c560ac799b11c30b31f87e3081965fe5e0180",
        strip_prefix = "thread_local-1.1.4",
        build_file = Label("//third_party/remote:BUILD.thread_local-1.1.4.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__time__0_1_43",
        url = "https://crates.io/api/v1/crates/time/0.1.43/download",
        type = "tar.gz",
        sha256 = "ca8a50ef2360fbd1eeb0ecd46795a87a19024eb4b53c5dc916ca1fd95fe62438",
        strip_prefix = "time-0.1.43",
        build_file = Label("//third_party/remote:BUILD.time-0.1.43.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__time__0_2_27",
        url = "https://crates.io/api/v1/crates/time/0.2.27/download",
        type = "tar.gz",
        sha256 = "4752a97f8eebd6854ff91f1c1824cd6160626ac4bd44287f7f4ea2035a02a242",
        strip_prefix = "time-0.2.27",
        build_file = Label("//third_party/remote:BUILD.time-0.2.27.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__time_macros__0_1_1",
        url = "https://crates.io/api/v1/crates/time-macros/0.1.1/download",
        type = "tar.gz",
        sha256 = "957e9c6e26f12cb6d0dd7fc776bb67a706312e7299aed74c8dd5b17ebb27e2f1",
        strip_prefix = "time-macros-0.1.1",
        build_file = Label("//third_party/remote:BUILD.time-macros-0.1.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__time_macros_impl__0_1_2",
        url = "https://crates.io/api/v1/crates/time-macros-impl/0.1.2/download",
        type = "tar.gz",
        sha256 = "fd3c141a1b43194f3f56a1411225df8646c55781d5f26db825b3d98507eb482f",
        strip_prefix = "time-macros-impl-0.1.2",
        build_file = Label("//third_party/remote:BUILD.time-macros-impl-0.1.2.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__tokio__1_18_0",
        url = "https://crates.io/api/v1/crates/tokio/1.18.0/download",
        type = "tar.gz",
        sha256 = "0f48b6d60512a392e34dbf7fd456249fd2de3c83669ab642e021903f4015185b",
        strip_prefix = "tokio-1.18.0",
        build_file = Label("//third_party/remote:BUILD.tokio-1.18.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__tokio_io_timeout__1_2_0",
        url = "https://crates.io/api/v1/crates/tokio-io-timeout/1.2.0/download",
        type = "tar.gz",
        sha256 = "30b74022ada614a1b4834de765f9bb43877f910cc8ce4be40e89042c9223a8bf",
        strip_prefix = "tokio-io-timeout-1.2.0",
        build_file = Label("//third_party/remote:BUILD.tokio-io-timeout-1.2.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__tokio_macros__1_7_0",
        url = "https://crates.io/api/v1/crates/tokio-macros/1.7.0/download",
        type = "tar.gz",
        sha256 = "b557f72f448c511a979e2564e55d74e6c4432fc96ff4f6241bc6bded342643b7",
        strip_prefix = "tokio-macros-1.7.0",
        build_file = Label("//third_party/remote:BUILD.tokio-macros-1.7.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__tokio_native_tls__0_3_0",
        url = "https://crates.io/api/v1/crates/tokio-native-tls/0.3.0/download",
        type = "tar.gz",
        sha256 = "f7d995660bd2b7f8c1568414c1126076c13fbb725c40112dc0120b78eb9b717b",
        strip_prefix = "tokio-native-tls-0.3.0",
        build_file = Label("//third_party/remote:BUILD.tokio-native-tls-0.3.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__tokio_stream__0_1_8",
        url = "https://crates.io/api/v1/crates/tokio-stream/0.1.8/download",
        type = "tar.gz",
        sha256 = "50145484efff8818b5ccd256697f36863f587da82cf8b409c53adf1e840798e3",
        strip_prefix = "tokio-stream-0.1.8",
        build_file = Label("//third_party/remote:BUILD.tokio-stream-0.1.8.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__tokio_util__0_6_9",
        url = "https://crates.io/api/v1/crates/tokio-util/0.6.9/download",
        type = "tar.gz",
        sha256 = "9e99e1983e5d376cd8eb4b66604d2e99e79f5bd988c3055891dcd8c9e2604cc0",
        strip_prefix = "tokio-util-0.6.9",
        build_file = Label("//third_party/remote:BUILD.tokio-util-0.6.9.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__tokio_util__0_7_1",
        url = "https://crates.io/api/v1/crates/tokio-util/0.7.1/download",
        type = "tar.gz",
        sha256 = "0edfdeb067411dba2044da6d1cb2df793dd35add7888d73c16e3381ded401764",
        strip_prefix = "tokio-util-0.7.1",
        build_file = Label("//third_party/remote:BUILD.tokio-util-0.7.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__toml__0_5_9",
        url = "https://crates.io/api/v1/crates/toml/0.5.9/download",
        type = "tar.gz",
        sha256 = "8d82e1a7758622a465f8cee077614c73484dac5b836c02ff6a40d5d1010324d7",
        strip_prefix = "toml-0.5.9",
        build_file = Label("//third_party/remote:BUILD.toml-0.5.9.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__tonic__0_7_1",
        url = "https://crates.io/api/v1/crates/tonic/0.7.1/download",
        type = "tar.gz",
        sha256 = "30fb54bf1e446f44d870d260d99957e7d11fb9d0a0f5bd1a662ad1411cc103f9",
        strip_prefix = "tonic-0.7.1",
        build_file = Label("//third_party/remote:BUILD.tonic-0.7.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__tonic_build__0_7_0",
        url = "https://crates.io/api/v1/crates/tonic-build/0.7.0/download",
        type = "tar.gz",
        sha256 = "4d17087af5c80e5d5fc8ba9878e60258065a0a757e35efe7a05b7904bece1943",
        strip_prefix = "tonic-build-0.7.0",
        build_file = Label("//third_party/remote:BUILD.tonic-build-0.7.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__tower__0_4_12",
        url = "https://crates.io/api/v1/crates/tower/0.4.12/download",
        type = "tar.gz",
        sha256 = "9a89fd63ad6adf737582df5db40d286574513c69a11dac5214dc3b5603d6713e",
        strip_prefix = "tower-0.4.12",
        build_file = Label("//third_party/remote:BUILD.tower-0.4.12.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__tower_http__0_3_0",
        url = "https://crates.io/api/v1/crates/tower-http/0.3.0/download",
        type = "tar.gz",
        sha256 = "79dd37121c38240c4b4fe6520332406218bbf876f2f690fe9e406020189366fd",
        strip_prefix = "tower-http-0.3.0",
        build_file = Label("//third_party/remote:BUILD.tower-http-0.3.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__tower_layer__0_3_1",
        url = "https://crates.io/api/v1/crates/tower-layer/0.3.1/download",
        type = "tar.gz",
        sha256 = "343bc9466d3fe6b0f960ef45960509f84480bf4fd96f92901afe7ff3df9d3a62",
        strip_prefix = "tower-layer-0.3.1",
        build_file = Label("//third_party/remote:BUILD.tower-layer-0.3.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__tower_service__0_3_1",
        url = "https://crates.io/api/v1/crates/tower-service/0.3.1/download",
        type = "tar.gz",
        sha256 = "360dfd1d6d30e05fda32ace2c8c70e9c0a9da713275777f5a4dbb8a1893930c6",
        strip_prefix = "tower-service-0.3.1",
        build_file = Label("//third_party/remote:BUILD.tower-service-0.3.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__tracing__0_1_34",
        url = "https://crates.io/api/v1/crates/tracing/0.1.34/download",
        type = "tar.gz",
        sha256 = "5d0ecdcb44a79f0fe9844f0c4f33a342cbcbb5117de8001e6ba0dc2351327d09",
        strip_prefix = "tracing-0.1.34",
        build_file = Label("//third_party/remote:BUILD.tracing-0.1.34.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__tracing_attributes__0_1_21",
        url = "https://crates.io/api/v1/crates/tracing-attributes/0.1.21/download",
        type = "tar.gz",
        sha256 = "cc6b8ad3567499f98a1db7a752b07a7c8c7c7c34c332ec00effb2b0027974b7c",
        strip_prefix = "tracing-attributes-0.1.21",
        build_file = Label("//third_party/remote:BUILD.tracing-attributes-0.1.21.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__tracing_core__0_1_26",
        url = "https://crates.io/api/v1/crates/tracing-core/0.1.26/download",
        type = "tar.gz",
        sha256 = "f54c8ca710e81886d498c2fd3331b56c93aa248d49de2222ad2742247c60072f",
        strip_prefix = "tracing-core-0.1.26",
        build_file = Label("//third_party/remote:BUILD.tracing-core-0.1.26.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__tracing_futures__0_2_5",
        url = "https://crates.io/api/v1/crates/tracing-futures/0.2.5/download",
        type = "tar.gz",
        sha256 = "97d095ae15e245a057c8e8451bab9b3ee1e1f68e9ba2b4fbc18d0ac5237835f2",
        strip_prefix = "tracing-futures-0.2.5",
        build_file = Label("//third_party/remote:BUILD.tracing-futures-0.2.5.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__try_lock__0_2_3",
        url = "https://crates.io/api/v1/crates/try-lock/0.2.3/download",
        type = "tar.gz",
        sha256 = "59547bce71d9c38b83d9c0e92b6066c4253371f15005def0c30d9657f50c7642",
        strip_prefix = "try-lock-0.2.3",
        build_file = Label("//third_party/remote:BUILD.try-lock-0.2.3.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__twox_hash__1_6_2",
        url = "https://crates.io/api/v1/crates/twox-hash/1.6.2/download",
        type = "tar.gz",
        sha256 = "4ee73e6e4924fe940354b8d4d98cad5231175d615cd855b758adc658c0aac6a0",
        strip_prefix = "twox-hash-1.6.2",
        build_file = Label("//third_party/remote:BUILD.twox-hash-1.6.2.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__typenum__1_15_0",
        url = "https://crates.io/api/v1/crates/typenum/1.15.0/download",
        type = "tar.gz",
        sha256 = "dcf81ac59edc17cc8697ff311e8f5ef2d99fcbd9817b34cec66f90b6c3dfd987",
        strip_prefix = "typenum-1.15.0",
        build_file = Label("//third_party/remote:BUILD.typenum-1.15.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__ucd_trie__0_1_3",
        url = "https://crates.io/api/v1/crates/ucd-trie/0.1.3/download",
        type = "tar.gz",
        sha256 = "56dee185309b50d1f11bfedef0fe6d036842e3fb77413abef29f8f8d1c5d4c1c",
        strip_prefix = "ucd-trie-0.1.3",
        build_file = Label("//third_party/remote:BUILD.ucd-trie-0.1.3.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__unicode_segmentation__1_9_0",
        url = "https://crates.io/api/v1/crates/unicode-segmentation/1.9.0/download",
        type = "tar.gz",
        sha256 = "7e8820f5d777f6224dc4be3632222971ac30164d4a258d595640799554ebfd99",
        strip_prefix = "unicode-segmentation-1.9.0",
        build_file = Label("//third_party/remote:BUILD.unicode-segmentation-1.9.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__unicode_width__0_1_9",
        url = "https://crates.io/api/v1/crates/unicode-width/0.1.9/download",
        type = "tar.gz",
        sha256 = "3ed742d4ea2bd1176e236172c8429aaf54486e7ac098db29ffe6529e0ce50973",
        strip_prefix = "unicode-width-0.1.9",
        build_file = Label("//third_party/remote:BUILD.unicode-width-0.1.9.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__unicode_xid__0_2_2",
        url = "https://crates.io/api/v1/crates/unicode-xid/0.2.2/download",
        type = "tar.gz",
        sha256 = "8ccb82d61f80a663efe1f787a51b16b5a51e3314d6ac365b08639f52387b33f3",
        strip_prefix = "unicode-xid-0.2.2",
        build_file = Label("//third_party/remote:BUILD.unicode-xid-0.2.2.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__unicode_categories__0_1_1",
        url = "https://crates.io/api/v1/crates/unicode_categories/0.1.1/download",
        type = "tar.gz",
        sha256 = "39ec24b3121d976906ece63c9daad25b85969647682eee313cb5779fdd69e14e",
        strip_prefix = "unicode_categories-0.1.1",
        build_file = Label("//third_party/remote:BUILD.unicode_categories-0.1.1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__uuid__0_8_2",
        url = "https://crates.io/api/v1/crates/uuid/0.8.2/download",
        type = "tar.gz",
        sha256 = "bc5cf98d8186244414c848017f0e2676b3fcb46807f6668a97dfe67359a3c4b7",
        strip_prefix = "uuid-0.8.2",
        build_file = Label("//third_party/remote:BUILD.uuid-0.8.2.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__vcpkg__0_2_15",
        url = "https://crates.io/api/v1/crates/vcpkg/0.2.15/download",
        type = "tar.gz",
        sha256 = "accd4ea62f7bb7a82fe23066fb0957d48ef677f6eeb8215f372f52e48bb32426",
        strip_prefix = "vcpkg-0.2.15",
        build_file = Label("//third_party/remote:BUILD.vcpkg-0.2.15.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__vec_map__0_8_2",
        url = "https://crates.io/api/v1/crates/vec_map/0.8.2/download",
        type = "tar.gz",
        sha256 = "f1bddf1187be692e79c5ffeab891132dfb0f236ed36a43c7ed39f1165ee20191",
        strip_prefix = "vec_map-0.8.2",
        build_file = Label("//third_party/remote:BUILD.vec_map-0.8.2.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__version_check__0_9_4",
        url = "https://crates.io/api/v1/crates/version_check/0.9.4/download",
        type = "tar.gz",
        sha256 = "49874b5167b65d7193b8aba1567f5c7d93d001cafc34600cee003eda787e483f",
        strip_prefix = "version_check-0.9.4",
        build_file = Label("//third_party/remote:BUILD.version_check-0.9.4.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__walkdir__2_3_2",
        url = "https://crates.io/api/v1/crates/walkdir/2.3.2/download",
        type = "tar.gz",
        sha256 = "808cf2735cd4b6866113f648b791c6adc5714537bc222d9347bb203386ffda56",
        strip_prefix = "walkdir-2.3.2",
        build_file = Label("//third_party/remote:BUILD.walkdir-2.3.2.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__want__0_3_0",
        url = "https://crates.io/api/v1/crates/want/0.3.0/download",
        type = "tar.gz",
        sha256 = "1ce8a968cb1cd110d136ff8b819a556d6fb6d919363c61534f6860c7eb172ba0",
        strip_prefix = "want-0.3.0",
        build_file = Label("//third_party/remote:BUILD.want-0.3.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__wasi__0_10_2_wasi_snapshot_preview1",
        url = "https://crates.io/api/v1/crates/wasi/0.10.2+wasi-snapshot-preview1/download",
        type = "tar.gz",
        sha256 = "fd6fbd9a79829dd1ad0cc20627bf1ed606756a7f77edff7b66b7064f9cb327c6",
        strip_prefix = "wasi-0.10.2+wasi-snapshot-preview1",
        build_file = Label("//third_party/remote:BUILD.wasi-0.10.2+wasi-snapshot-preview1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__wasi__0_11_0_wasi_snapshot_preview1",
        url = "https://crates.io/api/v1/crates/wasi/0.11.0+wasi-snapshot-preview1/download",
        type = "tar.gz",
        sha256 = "9c8d87e72b64a3b4db28d11ce29237c246188f4f51057d65a7eab63b7987e423",
        strip_prefix = "wasi-0.11.0+wasi-snapshot-preview1",
        build_file = Label("//third_party/remote:BUILD.wasi-0.11.0+wasi-snapshot-preview1.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__wasm_bindgen__0_2_80",
        url = "https://crates.io/api/v1/crates/wasm-bindgen/0.2.80/download",
        type = "tar.gz",
        sha256 = "27370197c907c55e3f1a9fbe26f44e937fe6451368324e009cba39e139dc08ad",
        strip_prefix = "wasm-bindgen-0.2.80",
        build_file = Label("//third_party/remote:BUILD.wasm-bindgen-0.2.80.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__wasm_bindgen_backend__0_2_80",
        url = "https://crates.io/api/v1/crates/wasm-bindgen-backend/0.2.80/download",
        type = "tar.gz",
        sha256 = "53e04185bfa3a779273da532f5025e33398409573f348985af9a1cbf3774d3f4",
        strip_prefix = "wasm-bindgen-backend-0.2.80",
        build_file = Label("//third_party/remote:BUILD.wasm-bindgen-backend-0.2.80.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__wasm_bindgen_macro__0_2_80",
        url = "https://crates.io/api/v1/crates/wasm-bindgen-macro/0.2.80/download",
        type = "tar.gz",
        sha256 = "17cae7ff784d7e83a2fe7611cfe766ecf034111b49deb850a3dc7699c08251f5",
        strip_prefix = "wasm-bindgen-macro-0.2.80",
        build_file = Label("//third_party/remote:BUILD.wasm-bindgen-macro-0.2.80.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__wasm_bindgen_macro_support__0_2_80",
        url = "https://crates.io/api/v1/crates/wasm-bindgen-macro-support/0.2.80/download",
        type = "tar.gz",
        sha256 = "99ec0dc7a4756fffc231aab1b9f2f578d23cd391390ab27f952ae0c9b3ece20b",
        strip_prefix = "wasm-bindgen-macro-support-0.2.80",
        build_file = Label("//third_party/remote:BUILD.wasm-bindgen-macro-support-0.2.80.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__wasm_bindgen_shared__0_2_80",
        url = "https://crates.io/api/v1/crates/wasm-bindgen-shared/0.2.80/download",
        type = "tar.gz",
        sha256 = "d554b7f530dee5964d9a9468d95c1f8b8acae4f282807e7d27d4b03099a46744",
        strip_prefix = "wasm-bindgen-shared-0.2.80",
        build_file = Label("//third_party/remote:BUILD.wasm-bindgen-shared-0.2.80.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__which__4_2_5",
        url = "https://crates.io/api/v1/crates/which/4.2.5/download",
        type = "tar.gz",
        sha256 = "5c4fb54e6113b6a8772ee41c3404fb0301ac79604489467e0a9ce1f3e97c24ae",
        strip_prefix = "which-4.2.5",
        build_file = Label("//third_party/remote:BUILD.which-4.2.5.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__winapi__0_3_9",
        url = "https://crates.io/api/v1/crates/winapi/0.3.9/download",
        type = "tar.gz",
        sha256 = "5c839a674fcd7a98952e593242ea400abe93992746761e38641405d28b00f419",
        strip_prefix = "winapi-0.3.9",
        build_file = Label("//third_party/remote:BUILD.winapi-0.3.9.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__winapi_i686_pc_windows_gnu__0_4_0",
        url = "https://crates.io/api/v1/crates/winapi-i686-pc-windows-gnu/0.4.0/download",
        type = "tar.gz",
        sha256 = "ac3b87c63620426dd9b991e5ce0329eff545bccbbb34f3be09ff6fb6ab51b7b6",
        strip_prefix = "winapi-i686-pc-windows-gnu-0.4.0",
        build_file = Label("//third_party/remote:BUILD.winapi-i686-pc-windows-gnu-0.4.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__winapi_util__0_1_5",
        url = "https://crates.io/api/v1/crates/winapi-util/0.1.5/download",
        type = "tar.gz",
        sha256 = "70ec6ce85bb158151cae5e5c87f95a8e97d2c0c4b001223f33a334e3ce5de178",
        strip_prefix = "winapi-util-0.1.5",
        build_file = Label("//third_party/remote:BUILD.winapi-util-0.1.5.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__winapi_x86_64_pc_windows_gnu__0_4_0",
        url = "https://crates.io/api/v1/crates/winapi-x86_64-pc-windows-gnu/0.4.0/download",
        type = "tar.gz",
        sha256 = "712e227841d057c1ee1cd2fb22fa7e5a5461ae8e48fa2ca79ec42cfc1931183f",
        strip_prefix = "winapi-x86_64-pc-windows-gnu-0.4.0",
        build_file = Label("//third_party/remote:BUILD.winapi-x86_64-pc-windows-gnu-0.4.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__windows_sys__0_34_0",
        url = "https://crates.io/api/v1/crates/windows-sys/0.34.0/download",
        type = "tar.gz",
        sha256 = "5acdd78cb4ba54c0045ac14f62d8f94a03d10047904ae2a40afa1e99d8f70825",
        strip_prefix = "windows-sys-0.34.0",
        build_file = Label("//third_party/remote:BUILD.windows-sys-0.34.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__windows_aarch64_msvc__0_34_0",
        url = "https://crates.io/api/v1/crates/windows_aarch64_msvc/0.34.0/download",
        type = "tar.gz",
        sha256 = "17cffbe740121affb56fad0fc0e421804adf0ae00891205213b5cecd30db881d",
        strip_prefix = "windows_aarch64_msvc-0.34.0",
        build_file = Label("//third_party/remote:BUILD.windows_aarch64_msvc-0.34.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__windows_i686_gnu__0_34_0",
        url = "https://crates.io/api/v1/crates/windows_i686_gnu/0.34.0/download",
        type = "tar.gz",
        sha256 = "2564fde759adb79129d9b4f54be42b32c89970c18ebf93124ca8870a498688ed",
        strip_prefix = "windows_i686_gnu-0.34.0",
        build_file = Label("//third_party/remote:BUILD.windows_i686_gnu-0.34.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__windows_i686_msvc__0_34_0",
        url = "https://crates.io/api/v1/crates/windows_i686_msvc/0.34.0/download",
        type = "tar.gz",
        sha256 = "9cd9d32ba70453522332c14d38814bceeb747d80b3958676007acadd7e166956",
        strip_prefix = "windows_i686_msvc-0.34.0",
        build_file = Label("//third_party/remote:BUILD.windows_i686_msvc-0.34.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__windows_x86_64_gnu__0_34_0",
        url = "https://crates.io/api/v1/crates/windows_x86_64_gnu/0.34.0/download",
        type = "tar.gz",
        sha256 = "cfce6deae227ee8d356d19effc141a509cc503dfd1f850622ec4b0f84428e1f4",
        strip_prefix = "windows_x86_64_gnu-0.34.0",
        build_file = Label("//third_party/remote:BUILD.windows_x86_64_gnu-0.34.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__windows_x86_64_msvc__0_34_0",
        url = "https://crates.io/api/v1/crates/windows_x86_64_msvc/0.34.0/download",
        type = "tar.gz",
        sha256 = "d19538ccc21819d01deaf88d6a17eae6596a12e9aafdbb97916fb49896d89de9",
        strip_prefix = "windows_x86_64_msvc-0.34.0",
        build_file = Label("//third_party/remote:BUILD.windows_x86_64_msvc-0.34.0.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__xml_rs__0_8_4",
        url = "https://crates.io/api/v1/crates/xml-rs/0.8.4/download",
        type = "tar.gz",
        sha256 = "d2d7d3948613f75c98fd9328cfdcc45acc4d360655289d0a7d4ec931392200a3",
        strip_prefix = "xml-rs-0.8.4",
        build_file = Label("//third_party/remote:BUILD.xml-rs-0.8.4.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__yansi_term__0_1_2",
        url = "https://crates.io/api/v1/crates/yansi-term/0.1.2/download",
        type = "tar.gz",
        sha256 = "fe5c30ade05e61656247b2e334a031dfd0cc466fadef865bdcdea8d537951bf1",
        strip_prefix = "yansi-term-0.1.2",
        build_file = Label("//third_party/remote:BUILD.yansi-term-0.1.2.bazel"),
    )

    maybe(
        http_archive,
        name = "raze__zeroize__1_5_4",
        url = "https://crates.io/api/v1/crates/zeroize/1.5.4/download",
        type = "tar.gz",
        sha256 = "7eb5728b8afd3f280a869ce1d4c554ffaed35f45c231fc41bfbd0381bef50317",
        strip_prefix = "zeroize-1.5.4",
        build_file = Label("//third_party/remote:BUILD.zeroize-1.5.4.bazel"),
    )
