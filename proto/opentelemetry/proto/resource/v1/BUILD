load("@protobuf//bazel:cc_proto_library.bzl", "cc_proto_library")
load("@rules_proto//proto:defs.bzl", "proto_library")

proto_library(
    name = "resource_proto",
    srcs = ["resource.proto"],
    strip_import_prefix = "/proto",
    visibility = ["//visibility:public"],
    deps = [
        "//proto/opentelemetry/proto/common/v1:common_proto",
    ],
)

cc_proto_library(
    name = "resource_cc_proto",
    visibility = ["//visibility:public"],
    deps = [":resource_proto"],
)
