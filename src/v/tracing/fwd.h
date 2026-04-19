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

#include <array>
#include <cstdint>

namespace tracing {

using trace_id_t = std::array<uint8_t, 16>;
using span_id_t = std::array<uint8_t, 8>;

enum class scope_id : uint8_t;
enum class span_kind : uint8_t;
enum class status_code : uint8_t;

struct key_value;
struct span_event;
struct span_link;
struct span_status;
struct span;
struct trace_context;
struct trace_ref;
struct instrumentation_scope;

namespace coroutine {
class scoped_span_guard;
} // namespace coroutine
class span_attrs;
class span_exporter;
class span_manager;

} // namespace tracing
