load("@bazel_skylib//rules:native_binary.bzl", "native_binary")

filegroup(
    name = "tempo_bin",
    srcs = ["tempo/tempo"],
    visibility = ["//visibility:public"],
)

native_binary(
    name = "tempo",
    src = ":tempo_bin",
    visibility = ["//visibility:public"],
)
