load("@protobuf//bazel:cc_proto_library.bzl", "cc_proto_library")
load("@rules_proto//proto:defs.bzl", "proto_library")

proto_library(
    name = "common_proto",
    srcs = ["common.proto"],
    strip_import_prefix = "/proto",
    visibility = ["//visibility:public"],
)

cc_proto_library(
    name = "common_cc_proto",
    visibility = ["//visibility:public"],
    deps = [":common_proto"],
)
