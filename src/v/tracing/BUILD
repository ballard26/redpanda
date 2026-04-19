load("//bazel:build.bzl", "redpanda_cc_library")

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
