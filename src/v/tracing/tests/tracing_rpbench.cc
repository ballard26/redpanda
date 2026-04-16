/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "tracing/span_attrs.h"
#include "tracing/span_guard.h"
#include "tracing/span_manager.h"
#include "tracing/trace.h"
#include "tracing/types.h"

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/testing/perf_tests.hh>

using namespace tracing;

namespace {
constexpr size_t N = 10000;

// -- trace_*_call wrappers (formerly scoped-guard path) --

ss::future<size_t> bench_root_call() {
    for (size_t i = 0; i < N; ++i) {
        co_await trace_root_span("produce", {.scope = scope_id::kafka}, [] { return ss::now(); });
    }
    co_return N;
}

ss::future<size_t> bench_root_and_child_call() {
    for (size_t i = 0; i < N; ++i) {
        co_await trace_root_span("produce", {.scope = scope_id::kafka}, [] {
            return trace_span("replicate", {.scope = scope_id::raft}, [] { return ss::now(); });
        });
    }
    co_return N;
}

ss::future<size_t> bench_call_with_attr() {
    for (size_t i = 0; i < N; ++i) {
        co_await trace_root_span("produce", {.scope = scope_id::kafka}, [] {
            if (auto sa = try_get_span_attrs()) {
                sa->attr(static_str{"topic"}, ss::sstring("test-topic"));
            }
            return ss::now();
        });
    }
    co_return N;
}

ss::future<size_t> bench_call_with_event() {
    for (size_t i = 0; i < N; ++i) {
        co_await trace_root_span("produce", {.scope = scope_id::kafka}, [] {
            if (auto sa = try_get_span_attrs()) {
                sa->event(ss::sstring("batch_received"));
            }
            return ss::now();
        });
    }
    co_return N;
}

// -- Coroutine guard benchmark bodies --

ss::future<size_t> co_bench_root_span() {
    for (size_t i = 0; i < N; ++i) {
        auto g = co_await tracing::coroutine::trace_root_span("produce", {.scope = scope_id::kafka});
        perf_tests::do_not_optimize(g);
    }
    co_return N;
}

ss::future<size_t> co_bench_root_and_child() {
    for (size_t i = 0; i < N; ++i) {
        auto root = co_await tracing::coroutine::trace_root_span("produce", {.scope = scope_id::kafka});
        auto child = co_await tracing::coroutine::trace_span("replicate", {.scope = scope_id::raft});
        perf_tests::do_not_optimize(root);
        perf_tests::do_not_optimize(child);
    }
    co_return N;
}

ss::future<size_t> co_bench_root_with_attr() {
    for (size_t i = 0; i < N; ++i) {
        auto g = co_await tracing::coroutine::trace_root_span("produce", {.scope = scope_id::kafka});
        if (auto sa = try_get_span_attrs()) {
            sa->attr(static_str{"topic"}, ss::sstring("test-topic"));
        }
        perf_tests::do_not_optimize(g);
    }
    co_return N;
}

ss::future<size_t> co_bench_root_with_event() {
    for (size_t i = 0; i < N; ++i) {
        auto g = co_await tracing::coroutine::trace_root_span("produce", {.scope = scope_id::kafka});
        if (auto sa = try_get_span_attrs()) {
            sa->event(ss::sstring("batch_received"));
        }
        perf_tests::do_not_optimize(g);
    }
    co_return N;
}

} // namespace

struct tracing_off {
    tracing_off() { mgr.start().get(); }
    ~tracing_off() { mgr.stop().get(); }
    ss::sharded<span_manager> mgr;
};

struct tracing_on {
    tracing_on() {
        mgr.start().get();
        mgr.local().update_config(
          {.enabled = true, .sampling = {.default_rate = 1.0}});
    }
    ~tracing_on() { mgr.stop().get(); }
    ss::sharded<span_manager> mgr;
};

// -- Wrapper call: OFF --

PERF_TEST_CN(tracing_off, call_root) {
    perf_tests::start_measuring_time();
    auto n = co_await bench_root_call();
    perf_tests::stop_measuring_time();
    co_return n;
}

PERF_TEST_CN(tracing_off, call_root_and_child) {
    perf_tests::start_measuring_time();
    auto n = co_await bench_root_and_child_call();
    perf_tests::stop_measuring_time();
    co_return n;
}

PERF_TEST_CN(tracing_off, call_with_attr) {
    perf_tests::start_measuring_time();
    auto n = co_await bench_call_with_attr();
    perf_tests::stop_measuring_time();
    co_return n;
}

PERF_TEST_CN(tracing_off, call_with_event) {
    perf_tests::start_measuring_time();
    auto n = co_await bench_call_with_event();
    perf_tests::stop_measuring_time();
    co_return n;
}

// -- Wrapper call: ON --

PERF_TEST_CN(tracing_on, call_root) {
    perf_tests::start_measuring_time();
    auto n = co_await bench_root_call();
    perf_tests::stop_measuring_time();
    co_return n;
}

PERF_TEST_CN(tracing_on, call_root_and_child) {
    perf_tests::start_measuring_time();
    auto n = co_await bench_root_and_child_call();
    perf_tests::stop_measuring_time();
    co_return n;
}

PERF_TEST_CN(tracing_on, call_with_attr) {
    perf_tests::start_measuring_time();
    auto n = co_await bench_call_with_attr();
    perf_tests::stop_measuring_time();
    co_return n;
}

PERF_TEST_CN(tracing_on, call_with_event) {
    perf_tests::start_measuring_time();
    auto n = co_await bench_call_with_event();
    perf_tests::stop_measuring_time();
    co_return n;
}

// -- Coroutine guard: OFF --

PERF_TEST_CN(tracing_off, co_root_span) {
    perf_tests::start_measuring_time();
    auto n = co_await co_bench_root_span();
    perf_tests::stop_measuring_time();
    co_return n;
}

PERF_TEST_CN(tracing_off, co_root_and_child) {
    perf_tests::start_measuring_time();
    auto n = co_await co_bench_root_and_child();
    perf_tests::stop_measuring_time();
    co_return n;
}

PERF_TEST_CN(tracing_off, co_span_with_attr) {
    perf_tests::start_measuring_time();
    auto n = co_await co_bench_root_with_attr();
    perf_tests::stop_measuring_time();
    co_return n;
}

PERF_TEST_CN(tracing_off, co_span_with_event) {
    perf_tests::start_measuring_time();
    auto n = co_await co_bench_root_with_event();
    perf_tests::stop_measuring_time();
    co_return n;
}

// -- Coroutine guard: ON --

PERF_TEST_CN(tracing_on, co_root_span) {
    perf_tests::start_measuring_time();
    auto n = co_await co_bench_root_span();
    perf_tests::stop_measuring_time();
    co_return n;
}

PERF_TEST_CN(tracing_on, co_root_and_child) {
    perf_tests::start_measuring_time();
    auto n = co_await co_bench_root_and_child();
    perf_tests::stop_measuring_time();
    co_return n;
}

PERF_TEST_CN(tracing_on, co_span_with_attr) {
    perf_tests::start_measuring_time();
    auto n = co_await co_bench_root_with_attr();
    perf_tests::stop_measuring_time();
    co_return n;
}

PERF_TEST_CN(tracing_on, co_span_with_event) {
    perf_tests::start_measuring_time();
    auto n = co_await co_bench_root_with_event();
    perf_tests::stop_measuring_time();
    co_return n;
}
