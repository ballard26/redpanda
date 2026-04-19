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

#include "tracing/trace.h"

#include "random/generators.h"

#include <cstring>

namespace tracing {

namespace {

thread_local span_manager* tl_manager = nullptr;
thread_local random_generators::rng tl_rng;

} // namespace

span_manager* local_span_manager() noexcept { return tl_manager; }

void set_local_span_manager(span_manager* m) noexcept { tl_manager = m; }

trace_id_t generate_trace_id() noexcept {
    trace_id_t id{};
    auto v0 = tl_rng.get_int<uint64_t>();
    auto v1 = tl_rng.get_int<uint64_t>();
    std::memcpy(id.data(), &v0, 8);
    std::memcpy(id.data() + 8, &v1, 8);
    return id;
}

span_id_t generate_span_id() noexcept {
    span_id_t id{};
    auto v = tl_rng.get_int<uint64_t>();
    std::memcpy(id.data(), &v, 8);
    return id;
}

} // namespace tracing
