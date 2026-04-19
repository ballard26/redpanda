load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "logger",
    hdrs = [
        "logger.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "types",
    hdrs = [
        "fwd.h",
        "scope.h",
        "types.h",
        "well_known_attrs.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/container:chunked_vector",
        "//src/v/strings:static_str",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "trace",
    srcs = [
        "trace.cc",
    ],
    hdrs = [
        "trace.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":types",
        "//src/v/base",
        "//src/v/random:generators",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "span_manager",
    srcs = [
        "span_manager.cc",
    ],
    hdrs = [
        "span_exporter.h",
        "span_manager.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":logger",
        ":serialization",
        ":trace",
        ":types",
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/config",
        "//src/v/container:chunked_circular_buffer",
        "//src/v/container:chunked_hash_map",
        "//src/v/container:chunked_vector",
        "//src/v/metrics",
        "//src/v/random:generators",
        "//src/v/ssx:future_util",
        "//src/v/ssx:semaphore",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "serialization",
    srcs = [
        "serialization.cc",
    ],
    hdrs = [
        "serialization.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":types",
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/container:chunked_vector",
        "//src/v/serde/protobuf:wire_format",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "span_guard",
    srcs = [
        "span_guard.cc",
    ],
    hdrs = [
        "span_guard.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":span_manager",
        ":trace",
        ":types",
        "//src/v/base",
        "//src/v/strings:static_str",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "span_attrs",
    srcs = [
        "span_attrs.cc",
    ],
    hdrs = [
        "span_attrs.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":span_manager",
        ":trace",
        ":types",
        "//src/v/strings:static_str",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "otlp_exporter",
    srcs = [
        "otlp_exporter.cc",
    ],
    hdrs = [
        "otlp_exporter.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":logger",
        ":span_manager",
        "//src/v/base",
        "//src/v/http",
        "//src/v/net",
        "//src/v/utils:prefix_logger",
        "//src/v/utils:unresolved_address",
        "@fmt",
        "@seastar",
    ],
)
