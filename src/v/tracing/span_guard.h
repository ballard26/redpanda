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
#include "strings/static_str.h"
#include "tracing/scope.h"
#include "tracing/trace.h"
#include "tracing/types.h"

#include <seastar/core/future.hh>
#include <seastar/core/task.hh>
#include <seastar/core/with_context.hh>

#include <coroutine>
#include <utility>

namespace tracing {

/// Options bag for span creation, forwarded through the factory
/// helpers. Keeps the factory signatures stable as we add new
/// optional fields (links, start-time override, etc.).
struct span_opts {
    scope_id scope = scope_id::unknown;
    span_kind kind = span_kind::internal;
};

namespace detail {
/// Helpers shared with header-inline templates below.
ss::lw_shared_ptr<ss::task_context>
make_root_trace_context(static_str name, span_opts opts) noexcept;

ss::lw_shared_ptr<ss::task_context> make_child_trace_context(
  trace_context* parent, static_str name, span_opts opts) noexcept;

ss::lw_shared_ptr<ss::task_context> make_child_trace_context_from_ref(
  const trace_ref& ref, static_str name, span_opts opts) noexcept;

void record_and_end_span(ss::lw_shared_ptr<ss::task_context> ctx) noexcept;

/// Run \c func under \c ctx (empty ctx = passthrough) and record the
/// span when the returned future resolves. Shared body of
/// \c trace_span and \c trace_root_span.
template<typename Func, typename... Args>
auto with_traced_context(
  ss::lw_shared_ptr<ss::task_context> ctx,
  Func&& func,
  Args&&... args) noexcept {
    if (!ctx) [[likely]] {
        return ss::futurize_invoke(
          std::forward<Func>(func), std::forward<Args>(args)...);
    }
    return ss::with_context(
             ctx, std::forward<Func>(func), std::forward<Args>(args)...)
      .finally([ctx = std::move(ctx)]() mutable noexcept {
          record_and_end_span(std::move(ctx));
      });
}
} // namespace detail

namespace coroutine {

/// RAII handle returned by co_await-ing \c trace_span or
/// \c trace_root_span. Holds the task context for the lifetime of the
/// span so attributes added on the current coroutine always refer to a
/// live span. Records the span on destruction and restores the caller's
/// context.
///
/// Must not outlive the coroutine that co_await-ed it — the raw
/// task pointer embedded in the guard points to the coroutine promise.
class scoped_span_guard {
    ss::lw_shared_ptr<ss::task_context> _ctx;
    ss::lw_shared_ptr<ss::task_context> _prev;
    ss::task* _task = nullptr;

public:
    scoped_span_guard() noexcept = default;
    scoped_span_guard(
      ss::lw_shared_ptr<ss::task_context> ctx,
      ss::lw_shared_ptr<ss::task_context> prev,
      ss::task* task) noexcept
      : _ctx(std::move(ctx))
      , _prev(std::move(prev))
      , _task(task) {}

    scoped_span_guard(scoped_span_guard&&) noexcept = default;
    scoped_span_guard& operator=(scoped_span_guard&&) = delete;
    scoped_span_guard(const scoped_span_guard&) = delete;
    scoped_span_guard& operator=(const scoped_span_guard&) = delete;

    ~scoped_span_guard() noexcept {
        if (!_ctx) {
            return;
        }
        ss::set_current_task_context(_prev.get());
        if (_task) {
            _task->set_context(std::move(_prev));
        }
        detail::record_and_end_span(std::move(_ctx));
    }

    explicit operator bool() const noexcept { return static_cast<bool>(_ctx); }
};

/// Awaitable that installs the trace context in TLS and in the
/// enclosing coroutine's task context field, without actually
/// suspending. Returns a \ref scoped_span_guard whose destructor
/// restores both and records the span.
class scoped_span_guard_awaitable {
    ss::lw_shared_ptr<ss::task_context> _ctx;
    ss::lw_shared_ptr<ss::task_context> _prev;
    ss::task* _task = nullptr;

public:
    explicit scoped_span_guard_awaitable(
      ss::lw_shared_ptr<ss::task_context> ctx) noexcept
      : _ctx(std::move(ctx)) {}

    bool await_ready() const noexcept { return false; }

    template<typename Promise>
    bool await_suspend(std::coroutine_handle<Promise> h) noexcept {
        if (!_ctx) {
            return false;
        }
        // Own a ref to the parent context so the guard can safely
        // restore it even if a child coroutine completes after its
        // enclosing parent has released its own reference.
        if (auto* p = ss::current_task_context()) {
            _prev = p->shared_from_this();
        }
        ss::set_current_task_context(_ctx.get());
        h.promise().set_context(_ctx);
        _task = static_cast<ss::task*>(&h.promise());
        return false;
    }

    scoped_span_guard await_resume() noexcept {
        return scoped_span_guard{std::move(_ctx), std::move(_prev), _task};
    }
};

/// Create a child span in a coroutine. Must be co_await-ed.
/// No-op if no parent trace context exists.
[[nodiscard]] inline scoped_span_guard_awaitable
trace_span(static_str name, span_opts opts) noexcept {
    auto* parent = current_trace();
    if (!parent) [[likely]] {
        return scoped_span_guard_awaitable{{}};
    }
    return scoped_span_guard_awaitable{
      detail::make_child_trace_context(parent, name, opts)};
}

/// Create a root span in a coroutine. Must be co_await-ed.
/// No-op if tracing is disabled or sampling says no.
[[nodiscard]] inline scoped_span_guard_awaitable
trace_root_span(static_str name, span_opts opts) noexcept {
    return scoped_span_guard_awaitable{
      detail::make_root_trace_context(name, opts)};
}

/// Create a child span in a coroutine, seeded from a \ref trace_ref
/// typically produced on another shard by \c extract_trace_ref.
/// Must be co_await-ed. No-op if the ref is empty or tracing is off.
[[nodiscard]] inline scoped_span_guard_awaitable trace_span_from_ref(
  const trace_ref& ref, static_str name, span_opts opts) noexcept {
    if (!ref) [[likely]] {
        return scoped_span_guard_awaitable{{}};
    }
    return scoped_span_guard_awaitable{
      detail::make_child_trace_context_from_ref(ref, name, opts)};
}

} // namespace coroutine

/// Execute \c func under a root span for \c .then()-style callers.
/// The span covers \c func's full async chain; it is recorded when
/// the returned future resolves.
///
/// When tracing is off or sampling denies, \c func is invoked
/// directly with no context manipulation.
template<typename Func, typename... Args>
[[nodiscard]] auto trace_root_span(
  static_str name, span_opts opts, Func&& func, Args&&... args) noexcept {
    return detail::with_traced_context(
      detail::make_root_trace_context(name, opts),
      std::forward<Func>(func),
      std::forward<Args>(args)...);
}

/// Execute \c func under a child of the current span. No-op passthrough
/// when no parent context exists.
template<typename Func, typename... Args>
[[nodiscard]] auto trace_span(
  static_str name, span_opts opts, Func&& func, Args&&... args) noexcept {
    auto* parent = current_trace();
    ss::lw_shared_ptr<ss::task_context> ctx;
    if (parent) {
        ctx = detail::make_child_trace_context(parent, name, opts);
    }
    return detail::with_traced_context(
      std::move(ctx), std::forward<Func>(func), std::forward<Args>(args)...);
}

/// Execute \c func under a child span seeded from a \ref trace_ref,
/// typically produced on another shard by \c extract_trace_ref.
/// No-op passthrough when the ref is empty or tracing is off.
template<typename Func, typename... Args>
[[nodiscard]] auto trace_span_from_ref(
  const trace_ref& ref,
  static_str name,
  span_opts opts,
  Func&& func,
  Args&&... args) noexcept {
    ss::lw_shared_ptr<ss::task_context> ctx;
    if (ref) {
        ctx = detail::make_child_trace_context_from_ref(ref, name, opts);
    }
    return detail::with_traced_context(
      std::move(ctx), std::forward<Func>(func), std::forward<Args>(args)...);
}

} // namespace tracing
