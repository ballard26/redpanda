load("//bazel:build.bzl", "redpanda_cc_library")
load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_library(
    name = "test_span",
    hdrs = [
        "test_span.h",
    ],
    deps = [
        "//src/v/tracing:trace",
        "//src/v/tracing:types",
    ],
)

redpanda_cc_gtest(
    name = "span_manager_test",
    timeout = "short",
    srcs = [
        "span_manager_test.cc",
    ],
    deps = [
        ":test_span",
        "//src/v/bytes:iobuf",
        "//src/v/test_utils:gtest",
        "//src/v/tracing:span_manager",
        "//src/v/tracing:trace",
        "//src/v/tracing:types",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "serialization_test",
    timeout = "short",
    srcs = [
        "serialization_test.cc",
    ],
    deps = [
        ":test_span",
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/serde/protobuf:wire_format",
        "//src/v/test_utils:gtest",
        "//src/v/tracing:serialization",
        "//src/v/tracing:trace",
        "//src/v/tracing:types",
        "@googletest//:gtest",
        "@seastar",
    ],
)
