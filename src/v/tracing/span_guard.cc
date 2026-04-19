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

#include "tracing/span_guard.h"

#include "tracing/span_manager.h"
#include "tracing/trace.h"

namespace tracing::detail {

namespace {

// Can't use make_lw_shared<trace_context>() because trace_context
// inherits enable_lw_shared_from_this indirectly (via task_context),
// so the accessor selects the no-esft path and allocates a separate
// counter. Bare new + shared_from_this() uses the esft counter
// embedded in task_context. Virtual destructor handles cleanup.
ss::lw_shared_ptr<ss::task_context> make_trace_context() {
    return (new trace_context())->shared_from_this();
}

} // namespace

ss::lw_shared_ptr<ss::task_context> make_child_trace_context(
  trace_context* parent, static_str name, span_opts opts) noexcept {
    auto* collector = local_span_manager();
    if (
      !collector
      || !collector->try_start_span(parent->depth, parent->child_count)) {
        return {};
    }
    ++parent->child_count;

    auto ctx_ptr = make_trace_context();
    auto* ctx = static_cast<trace_context*>(ctx_ptr.get());
    auto& s = ctx->current_span;
    s.trace_id = parent->current_span.trace_id;
    s.span_id = generate_span_id();
    s.parent_span_id = parent->current_span.span_id;
    s.name = name;
    s.scope = opts.scope;
    s.kind = opts.kind;
    s.start_time_unix_nano = trace_now_ns();
    ctx->depth = parent->depth + 1;
    return ctx_ptr;
}

ss::lw_shared_ptr<ss::task_context>
make_root_trace_context(static_str name, span_opts opts) noexcept {
    auto* mgr = local_span_manager();
    if (!mgr || !mgr->try_start_root_span(opts.scope, name)) {
        return {};
    }

    auto ctx_ptr = make_trace_context();
    auto* ctx = static_cast<trace_context*>(ctx_ptr.get());
    auto& s = ctx->current_span;
    s.trace_id = generate_trace_id();
    s.span_id = generate_span_id();
    s.name = name;
    s.scope = opts.scope;
    s.kind = opts.kind;
    s.start_time_unix_nano = trace_now_ns();
    ctx->depth = 0;
    return ctx_ptr;
}

void record_and_end_span(ss::lw_shared_ptr<ss::task_context> ctx_ptr) noexcept {
    if (!ctx_ptr) {
        return;
    }
    auto* ctx = static_cast<trace_context*>(ctx_ptr.get());
    if (ctx->recorded) {
        return;
    }
    ctx->recorded = true;
    auto* collector = local_span_manager();
    if (!collector) {
        return;
    }
    ctx->current_span.end_time_unix_nano = trace_now_ns();
    collector->record_span(std::move(ctx->current_span));
}

} // namespace tracing::detail
