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
#include "container/chunked_vector.h"
#include "strings/static_str.h"
#include "tracing/fwd.h"
#include "tracing/scope.h"

#include <seastar/core/sstring.hh>

#include <variant>

namespace tracing {

enum class span_kind : uint8_t {
    unspecified = 0,
    internal = 1,
    server = 2,
    client = 3,
    producer = 4,
    consumer = 5,
};

enum class status_code : uint8_t {
    unset = 0,
    ok = 1,
    error = 2,
};

using attribute_value = std::variant<ss::sstring, bool, int64_t, double>;

struct key_value {
    static_str key{""};
    attribute_value value;
};

struct span_event {
    uint64_t time_unix_nano;
    ss::sstring name;
    chunked_vector<key_value> attributes;
};

struct span_link {
    trace_id_t trace_id;
    span_id_t span_id;
    chunked_vector<key_value> attributes;
};

struct span_status {
    status_code code = status_code::unset;
    ss::sstring message;
};

struct span {
    trace_id_t trace_id{};
    span_id_t span_id{};
    span_id_t parent_span_id{};

    static_str name{"<unset>"};
    span_kind kind = span_kind::unspecified;
    scope_id scope = scope_id::unknown;
    uint64_t start_time_unix_nano = 0;
    uint64_t end_time_unix_nano = 0;

    chunked_vector<key_value> attributes;
    chunked_vector<span_event> events;
    chunked_vector<span_link> links;

    span_status status;

    uint32_t dropped_attributes_count = 0;
    uint32_t dropped_events_count = 0;
    uint32_t dropped_links_count = 0;
};

} // namespace tracing
