load("//bazel:build.bzl", "redpanda_cc_library")
load("//bazel:test.bzl", "redpanda_cc_bench", "redpanda_cc_btest", "redpanda_cc_gtest")

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
    name = "span_guard_test",
    timeout = "short",
    srcs = [
        "span_guard_test.cc",
    ],
    deps = [
        "//src/v/tracing:span_guard",
        "//src/v/tracing:span_manager",
        "//src/v/tracing:trace",
        "//src/v/tracing:types",
        "//src/v/test_utils:gtest",
        "@boost//:range",
        "@googletest//:gtest",
        "@seastar",
    ],
)

redpanda_cc_gtest(
    name = "span_attrs_test",
    timeout = "short",
    srcs = [
        "span_attrs_test.cc",
    ],
    deps = [
        "//src/v/tracing:span_attrs",
        "//src/v/tracing:span_guard",
        "//src/v/tracing:span_manager",
        "//src/v/tracing:trace",
        "//src/v/tracing:types",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
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
        "//src/v/test_utils:metrics",
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

redpanda_cc_btest(
    name = "otlp_exporter_test",
    timeout = "short",
    srcs = [
        "otlp_exporter_test.cc",
    ],
    cpu = 1,
    deps = [
        ":test_span",
        "//proto/opentelemetry/proto/collector/trace/v1:trace_service_cc_proto",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/http/tests:utils",
        "//src/v/test_utils:seastar_boost",
        "//src/v/tracing:otlp_exporter",
        "//src/v/tracing:serialization",
        "//src/v/tracing:trace",
        "//src/v/tracing:types",
        "@boost//:test",
        "@seastar",
    ],
)

redpanda_cc_bench(
    name = "tracing_rpbench",
    srcs = [
        "tracing_rpbench.cc",
    ],
    deps = [
        "//src/v/tracing:span_attrs",
        "//src/v/tracing:span_guard",
        "//src/v/tracing:span_manager",
        "//src/v/tracing:trace",
        "//src/v/tracing:types",
        "@seastar",
        "@seastar//:benchmark",
    ],
)

redpanda_cc_bench(
    name = "serialization_rpbench",
    srcs = [
        "serialization_rpbench.cc",
    ],
    deps = [
        ":test_span",
        "//src/v/bytes:iobuf",
        "//src/v/tracing:serialization",
        "//src/v/tracing:trace",
        "//src/v/tracing:types",
        "@seastar",
        "@seastar//:benchmark",
    ],
)
