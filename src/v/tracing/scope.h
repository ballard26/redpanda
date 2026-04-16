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

#include <array>
#include <cstddef>
#include <cstdint>

namespace tracing {

enum class scope_id : uint8_t {
    unknown = 0,
    kafka = 1,
    raft = 2,
    storage = 3,
    cluster = 4,
    rpc = 5,
    cloud_topics = 6,
    tiered_storage = 7,
    num_scopes,
};

struct instrumentation_scope {
    static_str name{""};
    static_str version{""};
};

inline const std::
  array<instrumentation_scope, static_cast<size_t>(scope_id::num_scopes)>
    scope_registry = {{
      {.name = "redpanda.unknown"},
      {.name = "redpanda.kafka"},
      {.name = "redpanda.raft"},
      {.name = "redpanda.storage"},
      {.name = "redpanda.cluster"},
      {.name = "redpanda.rpc"},
      {.name = "redpanda.cloud_topics"},
      {.name = "redpanda.tiered_storage"},
    }};

} // namespace tracing
