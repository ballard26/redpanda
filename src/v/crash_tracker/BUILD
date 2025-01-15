load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "crash_tracker",
    srcs = [
        "logger.cc",
    ],
    hdrs = [
        "logger.h",
        "types.h",
    ],
    implementation_deps = [
        "//src/v/model",
        "//src/v/serde",
        "//src/v/serde:bytes",
        "//src/v/serde:enum",
        "//src/v/serde:sstring",
    ],
    include_prefix = "crash_tracker",
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/base",
        "//src/v/model",
        "//src/v/serde",
        "@seastar",
    ],
)
