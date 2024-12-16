load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "crash_tracker",
    srcs = [
        "logger.cc",
    ],
    hdrs = [
        "logger.h",
    ],
    implementation_deps = [],
    include_prefix = "crash_tracker",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "@seastar",
    ],
)
