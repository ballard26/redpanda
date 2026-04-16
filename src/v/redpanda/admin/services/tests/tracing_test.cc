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

#include "proto/redpanda/core/admin/v2/tracing.proto.h"
#include "redpanda/admin/services/tracing.h"
#include "test_utils/test.h"
#include "tracing/span_manager.h"

#include <seastar/core/sharded.hh>

#include <gtest/gtest.h>

using namespace tracing;

namespace {

class TracingServiceFixture : public seastar_test {
protected:
    ss::future<> SetUpAsync() override { co_await _span_manager.start(); }
    ss::future<> TearDownAsync() override { co_await _span_manager.stop(); }

    ss::sharded<span_manager> _span_manager;
};

} // namespace

TEST_F(TracingServiceFixture, GetConfigReturnsDefaults) {
    admin::tracing_service_impl svc(_span_manager);

    auto resp = svc
                  .get_tracing_config(
                    serde::pb::rpc::context{},
                    proto::admin::get_tracing_config_request{})
                  .get();

    EXPECT_FALSE(resp.get_config().get_enabled());
    EXPECT_DOUBLE_EQ(resp.get_config().get_sampling().get_default_rate(), 0.0);
}

TEST_F_CORO(TracingServiceFixture, UpdateConfigPropagates) {
    admin::tracing_service_impl svc(_span_manager);

    proto::admin::update_tracing_config_request req;
    auto& cfg = req.get_config();
    cfg.set_enabled(true);
    cfg.get_sampling().set_default_rate(0.5);
    cfg.set_buffer_size(2048);
    cfg.set_flush_interval_ms(1000);

    co_await svc.update_tracing_config(
      serde::pb::rpc::context{}, std::move(req));

    auto resp = co_await svc.get_tracing_config(
      serde::pb::rpc::context{}, proto::admin::get_tracing_config_request{});

    ASSERT_TRUE_CORO(resp.get_config().get_enabled());
    ASSERT_EQ_CORO(resp.get_config().get_sampling().get_default_rate(), 0.5);
    ASSERT_EQ_CORO(resp.get_config().get_buffer_size(), 2048);
    ASSERT_EQ_CORO(resp.get_config().get_flush_interval_ms(), 1000);
}

TEST_F_CORO(TracingServiceFixture, UpdateConfigWithScopeOverride) {
    admin::tracing_service_impl svc(_span_manager);

    proto::admin::update_tracing_config_request req;
    auto& cfg = req.get_config();
    cfg.set_enabled(true);
    cfg.get_sampling().set_default_rate(0.0);

    proto::admin::sampling_override ovr;
    ovr.set_scope(ss::sstring{"kafka"});
    ovr.set_rate(1.0);
    cfg.get_sampling().get_overrides().push_back(std::move(ovr));

    co_await svc.update_tracing_config(
      serde::pb::rpc::context{}, std::move(req));

    // Kafka scope should sample, raft should not
    ASSERT_TRUE_CORO(
      _span_manager.local().try_start_root_span(scope_id::kafka, "test"));
    _span_manager.local().record_span(span{});

    ASSERT_FALSE_CORO(
      _span_manager.local().try_start_root_span(scope_id::raft, "test"));
}

TEST_F_CORO(TracingServiceFixture, GetStatusReturnsPerShardCounters) {
    admin::tracing_service_impl svc(_span_manager);

    // Enable tracing and record some spans
    proto::admin::update_tracing_config_request req;
    req.get_config().set_enabled(true);
    req.get_config().get_sampling().set_default_rate(1.0);
    co_await svc.update_tracing_config(
      serde::pb::rpc::context{}, std::move(req));

    _span_manager.local().try_start_root_span(scope_id::kafka, "test");
    _span_manager.local().record_span(span{});

    auto resp = co_await svc.get_tracing_status(
      serde::pb::rpc::context{}, proto::admin::get_tracing_status_request{});

    ASSERT_GT_CORO(resp.get_shards().size(), 0);
    // Shard 0 should have recorded at least one span
    ASSERT_EQ_CORO(resp.get_shards()[0].get_shard_id(), 0);
    ASSERT_EQ_CORO(resp.get_shards()[0].get_spans_recorded(), 1);
}
