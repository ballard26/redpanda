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

#include "redpanda/admin/services/tracing.h"

#include "tracing/otlp_exporter.h"
#include "tracing/scope.h"
#include "tracing/span_manager.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/shard_id.hh>

namespace proto {
using namespace proto::admin;
}

namespace admin {

namespace {

// Convert scope name string (e.g., "kafka") to scope_id.
// The scope_registry names are "redpanda.kafka", so we match on the
// suffix after "redpanda.".
std::optional<tracing::scope_id>
scope_from_string(std::string_view name) noexcept {
    for (size_t i = 0; i < static_cast<size_t>(tracing::scope_id::num_scopes);
         ++i) {
        auto full_name = std::string_view{tracing::scope_registry[i].name};
        // Match either "redpanda.kafka" or just "kafka"
        if (full_name == name) {
            return static_cast<tracing::scope_id>(i);
        }
        constexpr std::string_view prefix = "redpanda.";
        if (
          full_name.starts_with(prefix)
          && full_name.substr(prefix.size()) == name) {
            return static_cast<tracing::scope_id>(i);
        }
    }
    return std::nullopt;
}

ss::sstring scope_to_string(tracing::scope_id id) {
    auto idx = static_cast<size_t>(id);
    if (idx < static_cast<size_t>(tracing::scope_id::num_scopes)) {
        return ss::sstring{std::string_view{tracing::scope_registry[idx].name}};
    }
    return "unknown";
}

// Convert proto TracingConfig to span_manager::config
tracing::span_manager::config
from_proto_config(const proto::admin::tracing_config& pc) {
    tracing::span_manager::config cfg;
    cfg.enabled = pc.get_enabled();
    cfg.buffer_size = pc.get_buffer_size() > 0 ? pc.get_buffer_size() : 4096;
    cfg.flush_interval = std::chrono::milliseconds(
      pc.get_flush_interval_ms() > 0 ? pc.get_flush_interval_ms() : 5000);

    const auto& ps = pc.get_sampling();
    cfg.sampling.default_rate = ps.get_default_rate();
    for (const auto& ovr : ps.get_overrides()) {
        auto scope = scope_from_string(ovr.get_scope());
        if (!scope) {
            continue;
        }
        tracing::sampling_override so;
        so.scope = *scope;
        so.name = ovr.get_name();
        if (ovr.has_rate()) {
            so.rate = ovr.get_rate();
        }
        cfg.sampling.overrides.push_back(std::move(so));
    }

    const auto& pl = pc.get_limits();
    if (pl.get_max_active_spans_per_shard() > 0) {
        cfg.limits.max_active_spans_per_shard
          = pl.get_max_active_spans_per_shard();
    }
    if (pl.get_max_span_depth() > 0) {
        cfg.limits.max_span_depth = pl.get_max_span_depth();
    }
    if (pl.get_max_children_per_span() > 0) {
        cfg.limits.max_children_per_span = pl.get_max_children_per_span();
    }
    if (pl.get_max_attributes_per_span() > 0) {
        cfg.limits.max_attributes_per_span = pl.get_max_attributes_per_span();
    }
    if (pl.get_max_events_per_span() > 0) {
        cfg.limits.max_events_per_span = pl.get_max_events_per_span();
    }
    if (pl.get_max_links_per_span() > 0) {
        cfg.limits.max_links_per_span = pl.get_max_links_per_span();
    }

    return cfg;
}

std::optional<tracing::exporter_config>
exporter_config_from_proto(const proto::admin::tracing_config& pc) {
    const auto& pe = pc.get_exporter();
    if (pe.get_endpoint_host().empty()) {
        return std::nullopt;
    }
    return tracing::exporter_config{
      .endpoint = net::unresolved_address(
        ss::sstring{pe.get_endpoint_host()},
        pe.get_endpoint_port() > 0
          ? static_cast<uint16_t>(pe.get_endpoint_port())
          : uint16_t{4318}),
      .path = pe.get_path().empty() ? ss::sstring{"/v1/traces"}
                                    : ss::sstring{pe.get_path()},
      .auth_header = ss::sstring{pe.get_auth_header()},
      .timeout = std::chrono::milliseconds(
        pe.get_timeout_ms() > 0 ? pe.get_timeout_ms() : 2000),
    };
}

// Convert span_manager::config to proto TracingConfig
proto::admin::tracing_config
to_proto_config(const tracing::span_manager::config& cfg) {
    proto::admin::tracing_config pc;
    pc.set_enabled(cfg.enabled);
    pc.set_buffer_size(static_cast<uint32_t>(cfg.buffer_size));
    pc.set_flush_interval_ms(static_cast<uint32_t>(cfg.flush_interval.count()));

    proto::admin::sampling_config ps;
    ps.set_default_rate(cfg.sampling.default_rate);
    for (const auto& ovr : cfg.sampling.overrides) {
        proto::admin::sampling_override po;
        po.set_scope(scope_to_string(ovr.scope));
        po.set_name(ss::sstring{ovr.name});
        if (ovr.rate) {
            po.set_rate(*ovr.rate);
        }
        ps.get_overrides().push_back(std::move(po));
    }
    pc.set_sampling(std::move(ps));

    proto::admin::span_limits pl;
    pl.set_max_active_spans_per_shard(cfg.limits.max_active_spans_per_shard);
    pl.set_max_span_depth(cfg.limits.max_span_depth);
    pl.set_max_children_per_span(cfg.limits.max_children_per_span);
    pl.set_max_attributes_per_span(cfg.limits.max_attributes_per_span);
    pl.set_max_events_per_span(cfg.limits.max_events_per_span);
    pl.set_max_links_per_span(cfg.limits.max_links_per_span);
    pc.set_limits(std::move(pl));

    return pc;
}

} // namespace

tracing_service_impl::tracing_service_impl(
  ss::sharded<tracing::span_manager>& span_manager)
  : _span_manager(span_manager) {}

ss::future<proto::admin::get_tracing_config_response>
tracing_service_impl::get_tracing_config(
  serde::pb::rpc::context, proto::admin::get_tracing_config_request) {
    proto::admin::get_tracing_config_response resp;
    resp.set_config(to_proto_config(_span_manager.local().current_config()));
    co_return resp;
}

ss::future<proto::admin::update_tracing_config_response>
tracing_service_impl::update_tracing_config(
  serde::pb::rpc::context, proto::admin::update_tracing_config_request req) {
    auto proto_cfg = std::move(req.get_config());

    // Parse exporter config before dispatching (shard 0 only)
    auto exp_cfg = exporter_config_from_proto(proto_cfg);

    // Dispatch span_manager config to all shards
    auto proto_buf = co_await proto_cfg.to_proto();
    co_await _span_manager.invoke_on_all(
      [&proto_buf](tracing::span_manager& sm) -> ss::future<> {
          auto pc = co_await proto::admin::tracing_config::from_proto(
            proto_buf.copy());
          sm.update_config(from_proto_config(pc));
      });

    if (exp_cfg) {
        co_await _span_manager.invoke_on(
          0, [&exp_cfg](tracing::span_manager& sm) {
              return sm.set_exporter(
                std::make_unique<tracing::otlp_exporter>(*exp_cfg));
          });
    }

    co_return proto::admin::update_tracing_config_response{};
}

ss::future<proto::admin::get_tracing_status_response>
tracing_service_impl::get_tracing_status(
  serde::pb::rpc::context, proto::admin::get_tracing_status_request) {
    auto per_shard = co_await _span_manager.map(
      [](tracing::span_manager& sm) { return sm.get_status(); });

    proto::admin::get_tracing_status_response resp;
    for (uint32_t i = 0; i < per_shard.size(); ++i) {
        const auto& s = per_shard[i];
        proto::admin::shard_status ss;
        ss.set_shard_id(i);
        ss.set_spans_recorded(s.spans_recorded);
        ss.set_spans_dropped(s.spans_dropped);
        ss.set_spans_flushed(s.spans_flushed);
        ss.set_active_spans(s.active_spans);
        ss.set_spans_limited_depth(s.spans_limited_depth);
        ss.set_spans_limited_children(s.spans_limited_children);
        ss.set_spans_limited_active(s.spans_limited_active);
        ss.set_spans_sampled_out(s.spans_sampled_out);
        ss.set_attributes_dropped(s.attributes_dropped);
        ss.set_events_dropped(s.events_dropped);
        ss.set_flush_errors(s.flush_errors);
        resp.get_shards().push_back(std::move(ss));
    }
    co_return resp;
}

} // namespace admin
