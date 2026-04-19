load("@protobuf//bazel:cc_proto_library.bzl", "cc_proto_library")
load("@rules_proto//proto:defs.bzl", "proto_library")

proto_library(
    name = "trace_service_proto",
    srcs = ["trace_service.proto"],
    strip_import_prefix = "/proto",
    visibility = ["//visibility:public"],
    deps = [
        "//proto/opentelemetry/proto/trace/v1:trace_proto",
    ],
)

cc_proto_library(
    name = "trace_service_cc_proto",
    visibility = ["//visibility:public"],
    deps = [":trace_service_proto"],
)
