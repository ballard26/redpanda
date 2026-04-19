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

#include "bytes/iobuf.h"
#include "test_utils/metrics.h"
#include "test_utils/test.h"
#include "tracing/span_exporter.h"
#include "tracing/span_manager.h"
#include "tracing/tests/test_span.h"
#include "tracing/trace.h"
#include "tracing/types.h"

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

#include <gtest/gtest.h>

using namespace tracing;

namespace {

class capturing_exporter : public span_exporter {
public:
    ss::future<> export_spans(iobuf request) override {
        ++flush_count;
        last_request_size = request.size_bytes();
        co_return;
    }

    size_t flush_count = 0;
    size_t last_request_size = 0;
};

span make_test_span(scope_id scope = scope_id::kafka) {
    return tracing::testing::make_test_span(scope);
}

class CollectorFixture : public seastar_test {
protected:
    ss::future<> SetUpAsync() override {
        co_await _collector.start();
        auto exp = std::make_unique<capturing_exporter>();
        _exporter = exp.get();
        co_await _collector.local().set_exporter(std::move(exp));
        co_await _collector.invoke_on_all([](span_manager& sm) {
            sm.update_config(
              {.enabled = true, .sampling = {.default_rate = 1.0}});
        });
    }
    ss::future<> TearDownAsync() override { co_await _collector.stop(); }

    span_manager& local() { return _collector.local(); }

    capturing_exporter* _exporter = nullptr;
    ss::sharded<span_manager> _collector;
};

} // namespace

TEST_F(CollectorFixture, RecordAndFlush) {
    local().record_span(make_test_span());
    local().record_span(make_test_span());
    local().flush().get();

    EXPECT_EQ(_exporter->flush_count, 1);
    EXPECT_GT(_exporter->last_request_size, 0);
}

TEST_F(CollectorFixture, FlushClearsBuffer) {
    local().record_span(make_test_span());
    local().flush().get();
    EXPECT_EQ(_exporter->flush_count, 1);
    EXPECT_GT(_exporter->last_request_size, 0);

    local().flush().get();
    // Second flush should be a no-op (buffer was drained)
    EXPECT_EQ(_exporter->flush_count, 1);
}

TEST_F(CollectorFixture, RingBufferDropsOldest) {
    span_manager::config cfg;
    cfg.enabled = true;
    cfg.sampling.default_rate = 1.0;
    cfg.buffer_size = 3;
    local().update_config(std::move(cfg));

    local().record_span(make_test_span(scope_id::kafka));
    local().record_span(make_test_span(scope_id::raft));
    local().record_span(make_test_span(scope_id::storage));
    local().record_span(make_test_span(scope_id::cluster));
    local().flush().get();

    EXPECT_EQ(_exporter->flush_count, 1);
    EXPECT_GT(_exporter->last_request_size, 0);
    // Ring buffer holds 3, so 1 was dropped
    auto status = local().get_status();
    EXPECT_EQ(status.spans_dropped, 1);
    EXPECT_EQ(status.spans_flushed, 3);
}

TEST_F(CollectorFixture, DropCounterIncrements) {
    span_manager::config cfg;
    cfg.enabled = true;
    cfg.sampling.default_rate = 1.0;
    cfg.buffer_size = 2;
    local().update_config(std::move(cfg));

    local().record_span(make_test_span());
    local().record_span(make_test_span());
    local().record_span(make_test_span()); // drops oldest

    auto status = local().get_status();
    EXPECT_EQ(status.spans_dropped, 1);
    EXPECT_EQ(status.spans_recorded, 3);
}

TEST_F(CollectorFixture, ActiveSpanLimit) {
    span_manager::config cfg;
    cfg.enabled = true;
    cfg.sampling.default_rate = 1.0;
    cfg.limits.max_active_spans_per_shard = 2;
    local().update_config(std::move(cfg));

    EXPECT_TRUE(local().try_start_span(0, 0));
    EXPECT_TRUE(local().try_start_span(0, 0));
    EXPECT_FALSE(local().try_start_span(0, 0));

    // record_span decrements active count
    local().record_span(make_test_span());
    EXPECT_TRUE(local().try_start_span(0, 0));

    local().record_span(make_test_span());
    local().record_span(make_test_span());
}

TEST_F(CollectorFixture, DepthLimit) {
    span_manager::config cfg;
    cfg.enabled = true;
    cfg.sampling.default_rate = 1.0;
    cfg.limits.max_span_depth = 4;
    local().update_config(std::move(cfg));

    EXPECT_TRUE(local().try_start_span(0, 0));
    local().record_span(make_test_span());
    EXPECT_TRUE(local().try_start_span(3, 0));
    local().record_span(make_test_span());
    EXPECT_FALSE(local().try_start_span(4, 0));
}

TEST_F(CollectorFixture, ChildrenLimit) {
    span_manager::config cfg;
    cfg.enabled = true;
    cfg.sampling.default_rate = 1.0;
    cfg.limits.max_children_per_span = 3;
    local().update_config(std::move(cfg));

    EXPECT_TRUE(local().try_start_span(0, 2));
    local().record_span(make_test_span());
    EXPECT_FALSE(local().try_start_span(0, 3));
}

// -- Sampler tests --

TEST_F(CollectorFixture, SamplerDisabledRejectAll) {
    local().update_config({.enabled = false});
    EXPECT_FALSE(local().try_start_root_span(scope_id::kafka, "test"));
}

TEST_F(CollectorFixture, SamplerZeroRateRejectAll) {
    local().update_config({.enabled = true, .sampling = {.default_rate = 0.0}});
    EXPECT_FALSE(local().try_start_root_span(scope_id::kafka, "test"));
}

TEST_F(CollectorFixture, SamplerFullRateAcceptAll) {
    local().update_config({.enabled = true, .sampling = {.default_rate = 1.0}});
    // All should pass
    int accepted = 0;
    for (int i = 0; i < 100; ++i) {
        if (local().try_start_root_span(scope_id::kafka, "test")) {
            ++accepted;
            local().record_span(make_test_span());
        }
    }
    EXPECT_EQ(accepted, 100);
}

TEST_F(CollectorFixture, SamplerPartialRate) {
    local().update_config({.enabled = true, .sampling = {.default_rate = 0.5}});
    int accepted = 0;
    for (int i = 0; i < 1000; ++i) {
        if (local().try_start_root_span(scope_id::kafka, "test")) {
            ++accepted;
            local().record_span(make_test_span());
        }
    }
    // With 50% rate over 1000 trials, expect roughly 400-600
    EXPECT_GT(accepted, 300);
    EXPECT_LT(accepted, 700);
}

TEST_F(CollectorFixture, SamplerScopeOverride) {
    span_manager::config cfg;
    cfg.enabled = true;
    cfg.sampling.default_rate = 0.0;
    cfg.sampling.overrides.push_back({.scope = scope_id::storage, .rate = 1.0});
    local().update_config(std::move(cfg));

    EXPECT_FALSE(local().try_start_root_span(scope_id::kafka, "test"));
    EXPECT_TRUE(local().try_start_root_span(scope_id::storage, "test"));
    local().record_span(make_test_span());
}

TEST_F(CollectorFixture, SamplerNameOverride) {
    span_manager::config cfg;
    cfg.enabled = true;
    cfg.sampling.default_rate = 0.0;
    cfg.sampling.overrides.push_back(
      {.scope = scope_id::kafka, .name = "produce", .rate = 1.0});
    local().update_config(std::move(cfg));

    EXPECT_FALSE(local().try_start_root_span(scope_id::kafka, "fetch"));
    EXPECT_TRUE(local().try_start_root_span(scope_id::kafka, "produce"));
    local().record_span(make_test_span());
}

TEST_F(CollectorFixture, SamplerNameOverrideTakesPrecedence) {
    span_manager::config cfg;
    cfg.enabled = true;
    cfg.sampling.default_rate = 0.0;
    // Scope-level: storage at 0% (inherits default)
    // Name-level: storage/compaction at 100%
    cfg.sampling.overrides.push_back({.scope = scope_id::storage, .rate = 0.0});
    cfg.sampling.overrides.push_back(
      {.scope = scope_id::storage, .name = "compaction", .rate = 1.0});
    local().update_config(std::move(cfg));

    EXPECT_FALSE(local().try_start_root_span(scope_id::storage, "append"));
    EXPECT_TRUE(local().try_start_root_span(scope_id::storage, "compaction"));
    local().record_span(make_test_span());
}

// -- Metrics tests --

TEST_F(CollectorFixture, MetricsReflectCounters) {
    local().record_span(make_test_span());
    local().record_span(make_test_span());

    auto recorded = test_utils::find_metric_value<uint64_t>(
      "tracing_spans_recorded_total");
    ASSERT_TRUE(recorded.has_value());
    EXPECT_EQ(*recorded, 2);

    local().flush().get();

    auto flushed = test_utils::find_metric_value<uint64_t>(
      "tracing_spans_flushed_total");
    ASSERT_TRUE(flushed.has_value());
    EXPECT_EQ(*flushed, 2);
}

TEST_F(CollectorFixture, MetricsDroppedCounter) {
    span_manager::config cfg;
    cfg.enabled = true;
    cfg.sampling.default_rate = 1.0;
    cfg.buffer_size = 2;
    local().update_config(std::move(cfg));

    local().record_span(make_test_span());
    local().record_span(make_test_span());
    local().record_span(make_test_span()); // drops oldest

    auto dropped = test_utils::find_metric_value<uint64_t>(
      "tracing_spans_dropped_total");
    ASSERT_TRUE(dropped.has_value());
    EXPECT_EQ(*dropped, 1);
}

TEST_F(CollectorFixture, MetricsActiveSpansGauge) {
    EXPECT_TRUE(local().try_start_span(0, 0));
    EXPECT_TRUE(local().try_start_span(0, 0));

    auto active = test_utils::find_metric_value<double>("tracing_active_spans");
    ASSERT_TRUE(active.has_value());
    EXPECT_EQ(*active, 2.0);

    local().record_span(make_test_span());
    active = test_utils::find_metric_value<double>("tracing_active_spans");
    ASSERT_TRUE(active.has_value());
    EXPECT_EQ(*active, 1.0);
}

// Exporter that throws on every export to exercise the flush error path.
class throwing_exporter : public span_exporter {
public:
    ss::future<> export_spans(iobuf) override {
        throw std::runtime_error("simulated export failure");
    }
};

TEST_F(CollectorFixture, FlushErrorIncrementsCounter) {
    _collector.local()
      .set_exporter(std::make_unique<throwing_exporter>())
      .get();

    local().record_span(make_test_span());
    local().flush().get();

    EXPECT_EQ(local().get_status().flush_errors, 1);
}
