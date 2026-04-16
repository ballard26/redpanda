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

#pragma once

#include "proto/redpanda/core/admin/v2/tracing.proto.h"

#include <seastar/core/sharded.hh>

namespace tracing {
class span_manager;
}

namespace admin {

class tracing_service_impl : public proto::admin::tracing_service {
public:
    explicit tracing_service_impl(
      ss::sharded<tracing::span_manager>& span_manager);

    ss::future<proto::admin::get_tracing_config_response> get_tracing_config(
      serde::pb::rpc::context,
      proto::admin::get_tracing_config_request) override;

    ss::future<proto::admin::update_tracing_config_response>
      update_tracing_config(
        serde::pb::rpc::context,
        proto::admin::update_tracing_config_request) override;

    ss::future<proto::admin::get_tracing_status_response> get_tracing_status(
      serde::pb::rpc::context,
      proto::admin::get_tracing_status_request) override;

private:
    ss::sharded<tracing::span_manager>& _span_manager;
};

} // namespace admin
