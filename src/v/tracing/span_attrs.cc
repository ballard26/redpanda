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

#include "tracing/span_manager.h"
#include "tracing/trace.h"

namespace tracing {

namespace detail {

void on_attribute_dropped_safe() noexcept {
    if (auto* m = local_span_manager()) {
        m->on_attribute_dropped();
    }
}

void on_event_dropped_safe() noexcept {
    if (auto* m = local_span_manager()) {
        m->on_event_dropped();
    }
}

} // namespace detail

span_attrs& span_attrs::event(ss::sstring name) noexcept {
    if (_span.events.size() >= _max_events) {
        ++_span.dropped_events_count;
        detail::on_event_dropped_safe();
        return *this;
    }
    _span.events.emplace_back(trace_now_ns(), std::move(name));
    return *this;
}

std::optional<span_attrs> try_get_span_attrs() noexcept {
    auto* ctx = current_trace();
    if (!ctx || ctx->recorded) [[likely]] {
        return std::nullopt;
    }
    auto* mgr = local_span_manager();
    if (!mgr) {
        return std::nullopt;
    }
    const auto& limits = mgr->limits();
    return span_attrs(
      ctx->shared_from_this(),
      limits.max_attributes_per_span,
      limits.max_events_per_span);
}

} // namespace tracing
