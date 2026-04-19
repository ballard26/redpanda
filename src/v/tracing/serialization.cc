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

#include "tracing/serialization.h"

#include "bytes/iobuf.h"
#include "serde/protobuf/wire_format.h"

#include <seastar/coroutine/maybe_yield.hh>

#include <algorithm>
#include <string_view>
#include <variant>

namespace tracing {

namespace pb = serde::pb;

// Proto3 convention: skip fields with default/zero values.

namespace {

// -- Low-level helpers --

void write_tag(int32_t field, pb::wire_type wt, iobuf* out) {
    pb::tag::write({.wire_type = wt, .field_number = field}, out);
}

void write_bytes_field(
  int32_t field, const uint8_t* data, size_t len, iobuf* out) {
    if (len == 0) {
        return;
    }
    // Proto3 skip-default: treat all-zero bytes as unset. Matters for
    // parent_span_id on root spans — OTLP receivers require the field
    // to be absent, not 8 zero bytes, to recognize a root span.
    if (std::all_of(data, data + len, [](uint8_t b) { return b == 0; })) {
        return;
    }
    write_tag(field, pb::wire_type::length, out);
    pb::write_length(static_cast<int32_t>(len), out);
    out->append(data, len);
}

void write_string_field(int32_t field, std::string_view s, iobuf* out) {
    if (s.empty()) {
        return;
    }
    write_bytes_field(
      field, reinterpret_cast<const uint8_t*>(s.data()), s.size(), out);
}

void write_uint32_field(int32_t field, uint32_t v, iobuf* out) {
    if (v == 0) {
        return;
    }
    write_tag(field, pb::wire_type::varint, out);
    pb::write_varint<uint32_t>(v, out);
}

void write_enum_field(int32_t field, int32_t v, iobuf* out) {
    if (v == 0) {
        return;
    }
    write_tag(field, pb::wire_type::varint, out);
    pb::write_varint<int32_t, pb::zigzag::no>(v, out);
}

void write_fixed64_field(int32_t field, uint64_t v, iobuf* out) {
    if (v == 0) {
        return;
    }
    write_tag(field, pb::wire_type::i64, out);
    out->append(reinterpret_cast<const char*>(&v), sizeof(v));
}

void write_submessage(int32_t field, iobuf msg, iobuf* out) {
    if (msg.empty()) {
        return;
    }
    write_tag(field, pb::wire_type::length, out);
    pb::write_length(static_cast<int32_t>(msg.size_bytes()), out);
    out->append(std::move(msg));
}

// -- AnyValue (opentelemetry.proto.common.v1.AnyValue) --
// Field numbers: string_value=1, bool_value=2, int_value=3, double_value=4

iobuf serialize_any_value(const attribute_value& v) {
    iobuf out;
    std::visit(
      [&out](const auto& val) {
          using T = std::decay_t<decltype(val)>;
          if constexpr (std::is_same_v<T, ss::sstring>) {
              write_string_field(1, val, &out);
          } else if constexpr (std::is_same_v<T, bool>) {
              if (val) {
                  write_tag(2, pb::wire_type::varint, &out);
                  pb::write_varint<uint64_t>(1, &out);
              }
          } else if constexpr (std::is_same_v<T, int64_t>) {
              write_tag(3, pb::wire_type::varint, &out);
              pb::write_varint<int64_t, pb::zigzag::no>(val, &out);
          } else if constexpr (std::is_same_v<T, double>) {
              write_tag(4, pb::wire_type::i64, &out);
              out.append(reinterpret_cast<const char*>(&val), sizeof(val));
          }
      },
      v);
    return out;
}

// -- KeyValue (opentelemetry.proto.common.v1.KeyValue) --
// key=1, value=2

iobuf serialize_key_value(const key_value& kv) {
    iobuf out;
    write_string_field(1, kv.key, &out);
    write_submessage(2, serialize_any_value(kv.value), &out);
    return out;
}

// -- Status (opentelemetry.proto.trace.v1.Status) --
// message=2, code=3

iobuf serialize_status(const span_status& st) {
    iobuf out;
    write_string_field(2, st.message, &out);
    write_enum_field(3, static_cast<int32_t>(st.code), &out);
    return out;
}

// -- Event (opentelemetry.proto.trace.v1.Span.Event) --
// time_unix_nano=1, name=2, attributes=3, dropped_attributes_count=4

iobuf serialize_span_event(const span_event& ev) {
    iobuf out;
    write_fixed64_field(1, ev.time_unix_nano, &out);
    write_string_field(2, ev.name, &out);
    for (const auto& attr : ev.attributes) {
        write_submessage(3, serialize_key_value(attr), &out);
    }
    return out;
}

// -- Link (opentelemetry.proto.trace.v1.Span.Link) --
// trace_id=1, span_id=2, attributes=4

iobuf serialize_span_link(const span_link& link) {
    iobuf out;
    write_bytes_field(1, link.trace_id.data(), link.trace_id.size(), &out);
    write_bytes_field(2, link.span_id.data(), link.span_id.size(), &out);
    for (const auto& attr : link.attributes) {
        write_submessage(4, serialize_key_value(attr), &out);
    }
    return out;
}

// -- Span (opentelemetry.proto.trace.v1.Span) --
// trace_id=1, span_id=2, parent_span_id=4, name=5, kind=6,
// start_time_unix_nano=7, end_time_unix_nano=8, attributes=9,
// dropped_attributes_count=10, events=11, dropped_events_count=12,
// links=13, dropped_links_count=14, status=15

iobuf serialize_span(const span& s) {
    iobuf out;
    write_bytes_field(1, s.trace_id.data(), s.trace_id.size(), &out);
    write_bytes_field(2, s.span_id.data(), s.span_id.size(), &out);
    write_bytes_field(
      4, s.parent_span_id.data(), s.parent_span_id.size(), &out);
    write_string_field(5, s.name, &out);
    write_enum_field(6, static_cast<int32_t>(s.kind), &out);
    write_fixed64_field(7, s.start_time_unix_nano, &out);
    write_fixed64_field(8, s.end_time_unix_nano, &out);
    for (const auto& attr : s.attributes) {
        write_submessage(9, serialize_key_value(attr), &out);
    }
    write_uint32_field(10, s.dropped_attributes_count, &out);
    for (const auto& ev : s.events) {
        write_submessage(11, serialize_span_event(ev), &out);
    }
    write_uint32_field(12, s.dropped_events_count, &out);
    for (const auto& link : s.links) {
        write_submessage(13, serialize_span_link(link), &out);
    }
    write_uint32_field(14, s.dropped_links_count, &out);
    write_submessage(15, serialize_status(s.status), &out);
    return out;
}

// -- InstrumentationScope --
// name=1, version=2

iobuf serialize_scope(const instrumentation_scope& scope) {
    iobuf out;
    write_string_field(1, scope.name, &out);
    write_string_field(2, scope.version, &out);
    return out;
}

// -- Resource --
// attributes=1
iobuf serialize_resource(const resource& res) {
    iobuf out;
    for (const auto& attr : res.attributes) {
        write_submessage(1, serialize_key_value(attr), &out);
    }
    return out;
}

} // namespace

ss::future<iobuf>
serialize_resource_spans(const resource& res, chunked_vector<span> spans) {
    std::array<chunked_vector<span>, static_cast<size_t>(scope_id::num_scopes)>
      buckets;
    for (auto& s : spans) {
        auto idx = static_cast<size_t>(s.scope);
        if (idx < buckets.size()) {
            buckets[idx].push_back(std::move(s));
        }
    }

    iobuf rs_buf;
    write_submessage(1, serialize_resource(res), &rs_buf);
    for (size_t i = 0; i < buckets.size(); ++i) {
        if (buckets[i].empty()) {
            continue;
        }
        const auto& scope_info = scope_registry[i];
        iobuf scope_buf;
        write_submessage(1, serialize_scope(scope_info), &scope_buf);
        for (const auto& s : buckets[i]) {
            write_submessage(2, serialize_span(s), &scope_buf);
            co_await ss::coroutine::maybe_yield();
        }
        write_submessage(2, std::move(scope_buf), &rs_buf);
    }
    co_return rs_buf;
}

iobuf wrap_export_request(std::vector<iobuf> per_shard_resource_spans) {
    iobuf out;
    for (auto& rs_buf : per_shard_resource_spans) {
        if (rs_buf.empty()) {
            continue;
        }
        write_submessage(1, std::move(rs_buf), &out);
    }
    return out;
}

// -- testing namespace: thin forwarders for unit tests --

namespace testing {

iobuf serialize_span(const span& s) { return tracing::serialize_span(s); }

iobuf serialize_key_value(const key_value& kv) {
    return tracing::serialize_key_value(kv);
}

iobuf serialize_span_event(const span_event& ev) {
    return tracing::serialize_span_event(ev);
}

iobuf serialize_span_link(const span_link& link) {
    return tracing::serialize_span_link(link);
}

} // namespace testing

} // namespace tracing
