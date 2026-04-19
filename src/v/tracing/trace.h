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

#include "base/seastarx.h"
#include "tracing/types.h"

#include <seastar/core/task.hh>

#include <chrono>

namespace tracing {

class span_manager;

/// Per-span context that propagates through the Seastar task chain.
/// Each span_guard creates a new trace_context and installs it as
/// the current task context, so concurrent branches each see their
/// own context.
struct trace_context : seastar::task_context {
    span current_span;
    bool recorded = false;
    uint16_t depth = 0;
    uint16_t child_count = 0;
};

/// Returns the shard-local span_manager, or nullptr before start().
span_manager* local_span_manager() noexcept;

/// High-resolution wall-clock time in nanoseconds since Unix epoch.
/// Not lowres_system_clock: its ~10ms granularity makes short spans
/// appear as zero-duration to OTLP receivers.
inline uint64_t trace_now_ns() noexcept {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

trace_id_t generate_trace_id() noexcept;
span_id_t generate_span_id() noexcept;

/// Returns the current trace context, or nullptr if not tracing.
/// Cost: one TLS load + null check.
inline trace_context* current_trace() noexcept {
    auto* ctx = ss::current_task_context();
    if (!ctx) [[likely]] {
        return nullptr;
    }
    return static_cast<trace_context*>(ctx);
}

/// Snapshot the current span identity as a \ref trace_ref, suitable
/// for passing across shards (or out of process). Returns an empty
/// ref when not tracing.
inline trace_ref extract_trace_ref() noexcept {
    auto* ctx = current_trace();
    if (!ctx) [[likely]] {
        return {};
    }
    return trace_ref{
      .trace_id = ctx->current_span.trace_id,
      .span_id = ctx->current_span.span_id,
      .depth = ctx->depth,
    };
}

} // namespace tracing
