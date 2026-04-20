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
#include "tracing/span_exporter.h"
#include "tracing/span_guard.h"
#include "tracing/span_manager.h"
#include "tracing/trace.h"
#include "tracing/types.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sleep.hh>

#include <boost/range/irange.hpp>
#include <gtest/gtest.h>

#include <chrono>
#include <vector>

using namespace tracing;
using namespace std::chrono_literals;

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

TEST_F(TracingFixture, DefaultCoroutineGuardIsNoop) {
    coroutine::scoped_span_guard g;
    EXPECT_FALSE(static_cast<bool>(g));
}

TEST_F(TracingFixture, NoTraceContextWithoutActiveSpan) {
    EXPECT_EQ(current_trace(), nullptr);
}

TEST_F_CORO(TracingFixture, RootSpanSetsContext) {
    auto guard = co_await coroutine::trace_root_span("test_root", {.scope = scope_id::kafka});
    ASSERT_TRUE_CORO(static_cast<bool>(guard));

    auto* ctx = current_trace();
    ASSERT_NE_CORO(ctx, nullptr);
    ASSERT_NE_CORO(ctx->current_span.trace_id, trace_id_t{});
    ASSERT_NE_CORO(ctx->current_span.span_id, span_id_t{});
    ASSERT_EQ_CORO(ctx->current_span.parent_span_id, span_id_t{});
    ASSERT_EQ_CORO(ctx->current_span.scope, scope_id::kafka);
    ASSERT_EQ_CORO(ctx->depth, 0);
}

TEST_F_CORO(TracingFixture, ChildSpanInheritsTraceId) {
    auto root = co_await coroutine::trace_root_span("root", {.scope = scope_id::kafka});
    auto* root_ctx = current_trace();
    ASSERT_NE_CORO(root_ctx, nullptr);
    auto root_trace_id = root_ctx->current_span.trace_id;
    auto root_span_id = root_ctx->current_span.span_id;

    {
        auto child = co_await coroutine::trace_span("child", {.scope = scope_id::raft});
        ASSERT_TRUE_CORO(static_cast<bool>(child));

        auto* ctx = current_trace();
        ASSERT_NE_CORO(ctx, nullptr);
        ASSERT_TRUE_CORO(ctx->current_span.trace_id == root_trace_id);
        ASSERT_TRUE_CORO(ctx->current_span.span_id != root_span_id);
        ASSERT_TRUE_CORO(ctx->current_span.parent_span_id == root_span_id);
        ASSERT_EQ_CORO(
          static_cast<int>(ctx->current_span.scope),
          static_cast<int>(scope_id::raft));
        ASSERT_EQ_CORO(ctx->depth, 1);
    }

    auto* ctx = current_trace();
    ASSERT_NE_CORO(ctx, nullptr);
    ASSERT_TRUE_CORO(ctx->current_span.span_id == root_span_id);
}

TEST_F_CORO(TracingFixture, NestedChildSpans) {
    auto root = co_await coroutine::trace_root_span("root", {.scope = scope_id::kafka});
    auto root_span_id = current_trace()->current_span.span_id;

    {
        auto child1 = co_await coroutine::trace_span("child1", {.scope = scope_id::raft});
        auto child1_span_id = current_trace()->current_span.span_id;
        ASSERT_EQ_CORO(current_trace()->depth, 1);

        {
            auto child2 = co_await coroutine::trace_span("child2", {.scope = scope_id::storage});
            ASSERT_EQ_CORO(current_trace()->depth, 2);
            ASSERT_EQ_CORO(
              current_trace()->current_span.parent_span_id, child1_span_id);
        }

        ASSERT_EQ_CORO(current_trace()->current_span.span_id, child1_span_id);
    }

    ASSERT_EQ_CORO(current_trace()->current_span.span_id, root_span_id);
}

TEST_F_CORO(TracingFixture, TraceSpanNoopWithoutParent) {
    auto guard = co_await coroutine::trace_span("should_be_noop", {.scope = scope_id::raft});
    ASSERT_FALSE_CORO(static_cast<bool>(guard));
    ASSERT_EQ_CORO(current_trace(), nullptr);
}

TEST_F_CORO(TracingFixture, SpanGuardRecordsOnDestruction) {
    {
        auto root = co_await coroutine::trace_root_span("recorded_span", {.scope = scope_id::kafka});
        ASSERT_TRUE_CORO(static_cast<bool>(root));
    }
    ASSERT_EQ_CORO(current_trace(), nullptr);
}

TEST_F_CORO(TracingFixture, ContextSurvivesCoAwait) {
    auto root = co_await coroutine::trace_root_span("across_await", {.scope = scope_id::kafka});
    auto trace_id = current_trace()->current_span.trace_id;

    co_await ss::sleep(1ms);

    auto* ctx = current_trace();
    ASSERT_NE_CORO(ctx, nullptr);
    ASSERT_EQ_CORO(ctx->current_span.trace_id, trace_id);
}

TEST_F_CORO(TracingFixture, ConcurrentBranchesOwnContext) {
    auto root = co_await coroutine::trace_root_span("root", {.scope = scope_id::kafka});
    auto root_trace_id = current_trace()->current_span.trace_id;

    // Each parallel branch should get its own trace_context with the
    // same trace_id but a different span_id.
    chunked_vector<span_id_t> child_span_ids;
    co_await ss::parallel_for_each(
      boost::irange(0, 3), [&](int) -> ss::future<> {
          auto child = co_await coroutine::trace_span("branch", {.scope = scope_id::raft});
          ASSERT_TRUE_CORO(static_cast<bool>(child));
          auto* ctx = current_trace();
          ASSERT_NE_CORO(ctx, nullptr);
          ASSERT_TRUE_CORO(ctx->current_span.trace_id == root_trace_id);
          child_span_ids.push_back(ctx->current_span.span_id);
      });

    ASSERT_EQ_CORO(child_span_ids.size(), 3);
    ASSERT_NE_CORO(child_span_ids[0], child_span_ids[1]);
    ASSERT_NE_CORO(child_span_ids[0], child_span_ids[2]);
    ASSERT_NE_CORO(child_span_ids[1], child_span_ids[2]);
}

TEST_F(TracingFixture, DepthLimitEnforced) {
    span_manager::config cfg;
    cfg.enabled = true;
    cfg.sampling.default_rate = 1.0;
    cfg.limits.max_span_depth = 3;
    _collector.local().update_config(std::move(cfg));

    EXPECT_TRUE(_collector.local().try_start_span(0, 0));
    _collector.local().record_span(span{});
    EXPECT_TRUE(_collector.local().try_start_span(2, 0));
    _collector.local().record_span(span{});
    EXPECT_FALSE(_collector.local().try_start_span(3, 0));
}

TEST_F(TracingFixture, ChildrenLimitEnforced) {
    span_manager::config cfg;
    cfg.enabled = true;
    cfg.sampling.default_rate = 1.0;
    cfg.limits.max_children_per_span = 2;
    _collector.local().update_config(std::move(cfg));

    EXPECT_TRUE(_collector.local().try_start_span(0, 0));
    _collector.local().record_span(span{});
    EXPECT_TRUE(_collector.local().try_start_span(0, 1));
    _collector.local().record_span(span{});
    EXPECT_FALSE(_collector.local().try_start_span(0, 2));
}

// -- set_span_error / set_span_ok --

TEST_F(TracingFixture, SetSpanErrorNoopWithoutContext) {
    set_span_error("should not crash");
    EXPECT_EQ(current_trace(), nullptr);
}

TEST_F_CORO(TracingFixture, SetSpanErrorSetsStatus) {
    auto guard = co_await coroutine::trace_root_span(
      "errored", {.scope = scope_id::kafka});
    set_span_error("boom");

    auto* ctx = current_trace();
    ASSERT_NE_CORO(ctx, nullptr);
    ASSERT_EQ_CORO(ctx->current_span.status.code, status_code::error);
    ASSERT_EQ_CORO(ctx->current_span.status.message, "boom");
}

TEST_F_CORO(TracingFixture, SetSpanOkSetsStatus) {
    auto guard = co_await coroutine::trace_root_span(
      "succeeded", {.scope = scope_id::kafka});
    set_span_ok();

    auto* ctx = current_trace();
    ASSERT_NE_CORO(ctx, nullptr);
    ASSERT_EQ_CORO(ctx->current_span.status.code, status_code::ok);
    ASSERT_TRUE_CORO(ctx->current_span.status.message.empty());
}

// -- trace_root_span / trace_span wrappers --

TEST_F_CORO(TracingFixture, TraceRootCallCoversAsyncWork) {
    trace_id_t tid{};
    co_await trace_root_span("root_call", {.scope = scope_id::kafka}, [&]() -> ss::future<> {
          auto* ctx = current_trace();
          ASSERT_NE_CORO(ctx, nullptr);
          ASSERT_EQ_CORO(ctx->depth, 0);
          tid = ctx->current_span.trace_id;
          co_await ss::sleep(1ms);
          // Context must survive the await inside the wrapper
          auto* ctx2 = current_trace();
          ASSERT_NE_CORO(ctx2, nullptr);
          ASSERT_EQ_CORO(ctx2->current_span.trace_id, tid);
      });

    // After the wrapped call, context is restored
    ASSERT_EQ_CORO(current_trace(), nullptr);
    ASSERT_NE_CORO(tid, trace_id_t{});
}

TEST_F_CORO(TracingFixture, TraceCallNestsUnderRoot) {
    co_await trace_root_span("outer", {.scope = scope_id::kafka}, [&]() -> ss::future<> {
        auto* outer = current_trace();
        ASSERT_NE_CORO(outer, nullptr);
        auto outer_span_id = outer->current_span.span_id;

        co_await trace_span("inner", {.scope = scope_id::raft}, [&]() -> ss::future<> {
            auto* inner = current_trace();
            EXPECT_NE(inner, nullptr);
            EXPECT_EQ(inner->depth, 1);
            EXPECT_EQ(inner->current_span.parent_span_id, outer_span_id);
            return ss::now();
        });

        // Child span recorded, outer context restored
        auto* after = current_trace();
        ASSERT_NE_CORO(after, nullptr);
        ASSERT_EQ_CORO(after->current_span.span_id, outer_span_id);
    });
}

TEST_F_CORO(TracingFixture, TraceCallPassthroughWithoutParent) {
    bool ran = false;
    co_await trace_span("orphan", {.scope = scope_id::raft}, [&]() -> ss::future<> {
        ran = true;
        EXPECT_EQ(current_trace(), nullptr);
        return ss::now();
    });
    ASSERT_TRUE_CORO(ran);
}

// -- Cross-shard trace_ref --

TEST_F_CORO(TracingFixture, ExtractTraceRefEmptyWithoutContext) {
    auto ref = extract_trace_ref();
    ASSERT_FALSE_CORO(static_cast<bool>(ref));
    co_return;
}

TEST_F_CORO(TracingFixture, ExtractTraceRefCapturesCurrentSpan) {
    auto root = co_await coroutine::trace_root_span("captured", {.scope = scope_id::kafka});
    auto* ctx = current_trace();
    ASSERT_NE_CORO(ctx, nullptr);

    auto ref = extract_trace_ref();
    ASSERT_TRUE_CORO(static_cast<bool>(ref));
    ASSERT_TRUE_CORO(ref.trace_id == ctx->current_span.trace_id);
    ASSERT_TRUE_CORO(ref.span_id == ctx->current_span.span_id);
    ASSERT_EQ_CORO(ref.depth, ctx->depth);
}

TEST_F_CORO(TracingFixture, SpanFromRefCreatesChildOnSameShard) {
    auto root = co_await coroutine::trace_root_span("root", {.scope = scope_id::kafka});
    auto ref = extract_trace_ref();
    auto root_trace_id = ref.trace_id;
    auto root_span_id = ref.span_id;

    {
        auto child = co_await coroutine::trace_span_from_ref(ref, "from_ref", {.scope = scope_id::raft});
        ASSERT_TRUE_CORO(static_cast<bool>(child));

        auto* ctx = current_trace();
        ASSERT_NE_CORO(ctx, nullptr);
        ASSERT_TRUE_CORO(ctx->current_span.trace_id == root_trace_id);
        ASSERT_TRUE_CORO(ctx->current_span.parent_span_id == root_span_id);
        ASSERT_TRUE_CORO(ctx->current_span.span_id != root_span_id);
        ASSERT_EQ_CORO(ctx->depth, 1);
    }
}

TEST_F_CORO(TracingFixture, SpanFromRefNoopWithEmptyRef) {
    trace_ref empty{};
    auto guard = co_await coroutine::trace_span_from_ref(empty, "no_parent", {.scope = scope_id::raft});
    ASSERT_FALSE_CORO(static_cast<bool>(guard));
}

TEST_F_CORO(TracingFixture, SpanFromRefCrossShard) {
    auto root = co_await coroutine::trace_root_span("root", {.scope = scope_id::kafka});
    auto ref = extract_trace_ref();
    auto root_trace_id = ref.trace_id;
    auto root_span_id = ref.span_id;

    // Validate the cross-shard flow: submit to a different shard and
    // construct a child span there seeded only by the ref.
    auto target_shard = (ss::this_shard_id() + 1) % ss::smp::count;
    co_await _collector.invoke_on(
      target_shard, [ref, root_trace_id, root_span_id](span_manager&) {
          return trace_span_from_ref(ref, "remote_child", {.scope = scope_id::raft},
            [root_trace_id, root_span_id]() -> ss::future<> {
                auto* ctx = current_trace();
                EXPECT_NE(ctx, nullptr);
                if (ctx) {
                    EXPECT_TRUE(ctx->current_span.trace_id == root_trace_id);
                    EXPECT_TRUE(
                      ctx->current_span.parent_span_id == root_span_id);
                    EXPECT_EQ(ctx->depth, 1);
                }
                return ss::now();
            });
      });
}

TEST_F_CORO(TracingFixture, TraceSpanFromRefWrapperNoopWithEmptyRef) {
    trace_ref empty{};
    bool ran = false;
    co_await trace_span_from_ref(empty, "no_parent", {.scope = scope_id::raft}, [&]() -> ss::future<> {
          ran = true;
          EXPECT_EQ(current_trace(), nullptr);
          return ss::now();
      });
    ASSERT_TRUE_CORO(ran);
}

TEST_F_CORO(TracingFixture, TraceSpanFromRefWrapperCreatesChildOnSameShard) {
    auto root = co_await coroutine::trace_root_span("root", {.scope = scope_id::kafka});
    auto ref = extract_trace_ref();
    auto root_trace_id = ref.trace_id;
    auto root_span_id = ref.span_id;

    co_await trace_span_from_ref(ref, "child", {.scope = scope_id::raft},
      [root_trace_id, root_span_id]() -> ss::future<> {
          auto* ctx = current_trace();
          EXPECT_NE(ctx, nullptr);
          if (ctx) {
              EXPECT_TRUE(ctx->current_span.trace_id == root_trace_id);
              EXPECT_TRUE(ctx->current_span.parent_span_id == root_span_id);
              EXPECT_EQ(ctx->depth, 1);
          }
          return ss::now();
      });

    // Root context restored after the wrapper completes.
    auto* after = current_trace();
    ASSERT_NE_CORO(after, nullptr);
    ASSERT_TRUE_CORO(after->current_span.span_id == root_span_id);
}

TEST_F_CORO(TracingFixture, TraceSpanFromRefWrapperCoversAsyncWork) {
    auto root = co_await coroutine::trace_root_span("root", {.scope = scope_id::kafka});
    auto ref = extract_trace_ref();
    auto root_trace_id = ref.trace_id;

    // Async body — exercises the .finally path (not the available-future
    // short-circuit).
    co_await trace_span_from_ref(ref, "async_child", {.scope = scope_id::raft},
      [root_trace_id]() -> ss::future<> {
          co_await ss::sleep(1ms);
          auto* ctx = current_trace();
          EXPECT_NE(ctx, nullptr);
          if (ctx) {
              EXPECT_TRUE(ctx->current_span.trace_id == root_trace_id);
          }
      });

    // Parent span still active after the async child completes.
    ASSERT_NE_CORO(current_trace(), nullptr);
}

// -- Manager shutdown with active guards --

TEST_CORO(TracingShutdown, GuardDestructorSafeAfterManagerStop) {
    ss::sharded<span_manager> mgr;
    co_await mgr.start();
    co_await mgr.invoke_on_all([](span_manager& sm) {
        sm.update_config({.enabled = true, .sampling = {.default_rate = 1.0}});
    });

    auto guard = co_await coroutine::trace_root_span("outlives_stop", {.scope = scope_id::kafka});
    ASSERT_TRUE_CORO(static_cast<bool>(guard));

    co_await mgr.stop();

    // Guard destructor runs here — TLS is null, record_and_end no-ops.
    // This must not crash.
}

TEST_CORO(TracingShutdown, GuardDestructorSafeAfterManagerDestroyed) {
    // The guard must not outlive the coroutine that co_await-ed it
    // (the awaiter embeds a raw pointer to the coroutine promise).
    // But the manager CAN be destroyed while the guard is alive in
    // the same coroutine scope — the TLS null-out makes it safe.
    {
        ss::sharded<span_manager> mgr;
        co_await mgr.start();
        co_await mgr.invoke_on_all([](span_manager& sm) {
            sm.update_config(
              {.enabled = true, .sampling = {.default_rate = 1.0}});
        });

        auto guard = co_await coroutine::trace_root_span("outlives_destroy", {.scope = scope_id::kafka});
        ASSERT_TRUE_CORO(static_cast<bool>(guard));

        co_await mgr.stop();
        // mgr destroyed here, guard destructor runs after — TLS is
        // null so record_and_end no-ops. No crash.
    }
}

TEST_CORO(TracingShutdown, RootSpanNoopWithoutManager) {
    ss::sharded<span_manager> mgr;
    co_await mgr.start();
    co_await mgr.invoke_on_all([](span_manager& sm) {
        sm.update_config({.enabled = true, .sampling = {.default_rate = 1.0}});
    });
    co_await mgr.stop();

    auto guard = co_await coroutine::trace_root_span("no_manager", {.scope = scope_id::kafka});
    ASSERT_FALSE_CORO(static_cast<bool>(guard));
}

TEST_CORO(TracingShutdown, ChildSpanNoopWithoutManager) {
    ss::sharded<span_manager> mgr;
    co_await mgr.start();
    co_await mgr.invoke_on_all([](span_manager& sm) {
        sm.update_config({.enabled = true, .sampling = {.default_rate = 1.0}});
    });

    auto root = co_await coroutine::trace_root_span("root", {.scope = scope_id::kafka});
    ASSERT_TRUE_CORO(static_cast<bool>(root));

    co_await mgr.stop();

    auto child = co_await coroutine::trace_span("no_manager", {.scope = scope_id::raft});
    ASSERT_FALSE_CORO(static_cast<bool>(child));
}

