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

#include "test_utils/test.h"
#include "tracing/span_attrs.h"
#include "tracing/span_guard.h"
#include "tracing/span_manager.h"
#include "tracing/trace.h"
#include "tracing/types.h"

#include <seastar/core/sharded.hh>

#include <gtest/gtest.h>

using namespace tracing;

namespace {

class TracingFixture : public seastar_test {
protected:
    ss::future<> SetUpAsync() override {
        co_await _collector.start();
        co_await _collector.invoke_on_all([](span_manager& sm) {
            sm.update_config(
              {.enabled = true, .sampling = {.default_rate = 1.0}});
        });
    }

    ss::future<> TearDownAsync() override { co_await _collector.stop(); }

    ss::sharded<span_manager> _collector;
};

} // namespace

TEST_F_CORO(TracingFixture, AddAttributes) {
    auto root = co_await coroutine::trace_root_span(
      "with_attrs", {.scope = scope_id::kafka});
    auto sa = try_get_span_attrs();
    ASSERT_TRUE_CORO(sa.has_value());

    sa->attr(static_str{"key1"}, ss::sstring("value1"))
      .attr(static_str{"key2"}, int64_t{42})
      .attr(static_str{"key3"}, true)
      .attr(static_str{"key4"}, 3.14);

    auto* ctx = current_trace();
    ASSERT_EQ_CORO(ctx->current_span.attributes.size(), 4);
}

TEST_F_CORO(TracingFixture, NulloptWithoutContext) {
    auto sa = try_get_span_attrs();
    ASSERT_FALSE_CORO(sa.has_value());
}

TEST_F_CORO(TracingFixture, AddEvents) {
    auto root = co_await coroutine::trace_root_span(
      "with_events", {.scope = scope_id::kafka});
    auto sa = try_get_span_attrs();
    ASSERT_TRUE_CORO(sa.has_value());

    sa->event(ss::sstring("event1")).event(ss::sstring("event2"));

    auto* ctx = current_trace();
    ASSERT_EQ_CORO(ctx->current_span.events.size(), 2);
}

TEST_F_CORO(TracingFixture, AttributeLimitDropsExcess) {
    span_manager::config cfg;
    cfg.enabled = true;
    cfg.sampling.default_rate = 1.0;
    cfg.limits.max_attributes_per_span = 2;
    _collector.local().update_config(std::move(cfg));

    auto root = co_await coroutine::trace_root_span(
      "attr_limit", {.scope = scope_id::kafka});
    auto sa = try_get_span_attrs();
    ASSERT_TRUE_CORO(sa.has_value());

    sa->attr(static_str{"a"}, int64_t{1})
      .attr(static_str{"b"}, int64_t{2})
      .attr(static_str{"c"}, int64_t{3});

    auto* ctx = current_trace();
    ASSERT_EQ_CORO(ctx->current_span.attributes.size(), 2);
    ASSERT_EQ_CORO(ctx->current_span.dropped_attributes_count, 1);

    auto status = _collector.local().get_status();
    ASSERT_EQ_CORO(status.attributes_dropped, 1);
}

TEST_F_CORO(TracingFixture, EventLimitDropsExcess) {
    span_manager::config cfg;
    cfg.enabled = true;
    cfg.sampling.default_rate = 1.0;
    cfg.limits.max_events_per_span = 2;
    _collector.local().update_config(std::move(cfg));

    auto root = co_await coroutine::trace_root_span(
      "event_limit", {.scope = scope_id::kafka});
    auto sa = try_get_span_attrs();
    ASSERT_TRUE_CORO(sa.has_value());

    sa->event(ss::sstring("e1"))
      .event(ss::sstring("e2"))
      .event(ss::sstring("e3"));

    auto* ctx = current_trace();
    ASSERT_EQ_CORO(ctx->current_span.events.size(), 2);
    ASSERT_EQ_CORO(ctx->current_span.dropped_events_count, 1);

    auto status = _collector.local().get_status();
    ASSERT_EQ_CORO(status.events_dropped, 1);
}

TEST_CORO(TracingShutdown, NulloptWithoutManager) {
    ss::sharded<span_manager> mgr;
    co_await mgr.start();
    co_await mgr.invoke_on_all([](span_manager& sm) {
        sm.update_config({.enabled = true, .sampling = {.default_rate = 1.0}});
    });

    auto root = co_await coroutine::trace_root_span(
      "attrs_no_manager", {.scope = scope_id::kafka});
    ASSERT_TRUE_CORO(static_cast<bool>(root));

    co_await mgr.stop();

    auto sa = try_get_span_attrs();
    ASSERT_FALSE_CORO(sa.has_value());
}
