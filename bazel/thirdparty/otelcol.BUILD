load("@bazel_skylib//rules:native_binary.bzl", "native_binary")

filegroup(
    name = "otelcol_bin",
    srcs = ["otelcol/otelcol-contrib"],
    visibility = ["//visibility:public"],
)

native_binary(
    name = "otelcol",
    src = ":otelcol_bin",
    visibility = ["//visibility:public"],
)
