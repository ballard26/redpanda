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

#include "strings/static_str.h"
#include "tracing/trace.h"
#include "tracing/types.h"

#include <seastar/core/sstring.hh>

#include <cstdint>
#include <optional>
#include <type_traits>

namespace tracing {

namespace detail {
/// Out-of-line helpers so the inline template below doesn't need
/// span_manager's full type in this header.
void on_attribute_dropped_safe() noexcept;
void on_event_dropped_safe() noexcept;
} // namespace detail

/// Fluent attribute/event builder for the current span. Constructed
/// only after confirming tracing is active, so no null checks needed
/// on methods.
///
/// Holds an owning reference to the trace_context so the span stays
/// alive for the lifetime of span_attrs even if the owning span_guard
/// goes out of scope first. The span_manager is looked up via TLS for
/// metric increments and safely skipped if the shard has stopped.
class span_attrs {
    ss::lw_shared_ptr<ss::task_context> _ctx;
    span& _span;
    uint32_t _max_attributes;
    uint32_t _max_events;

public:
    span_attrs(
      ss::lw_shared_ptr<ss::task_context> ctx,
      uint32_t max_attributes,
      uint32_t max_events) noexcept
      : _ctx(std::move(ctx))
      , _span(static_cast<trace_context*>(_ctx.get())->current_span)
      , _max_attributes(max_attributes)
      , _max_events(max_events) {}

    template<typename V>
    requires std::constructible_from<attribute_value, V>
    span_attrs& attr(static_str key, V&& value) noexcept {
        if (_span.attributes.size() >= _max_attributes) {
            ++_span.dropped_attributes_count;
            detail::on_attribute_dropped_safe();
            return *this;
        }
        _span.attributes.emplace_back(key, std::forward<V>(value));
        return *this;
    }

    span_attrs& event(ss::sstring name) noexcept;
};

/// Returns a span_attrs builder if tracing is active, nullopt otherwise.
/// Use:
///   if (auto sa = try_get_span_attrs()) {
///       sa->attr("key", value).event("something happened");
///   }
std::optional<span_attrs> try_get_span_attrs() noexcept;

} // namespace tracing
