load("@protobuf//bazel:cc_proto_library.bzl", "cc_proto_library")
load("@rules_proto//proto:defs.bzl", "proto_library")

proto_library(
    name = "trace_proto",
    srcs = ["trace.proto"],
    strip_import_prefix = "/proto",
    visibility = ["//visibility:public"],
    deps = [
        "//proto/opentelemetry/proto/common/v1:common_proto",
        "//proto/opentelemetry/proto/resource/v1:resource_proto",
    ],
)

cc_proto_library(
    name = "trace_cc_proto",
    visibility = ["//visibility:public"],
    deps = [":trace_proto"],
)
