/*
 * Copyright 2025 Redpanda Data, Inc.
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
#include "model/timestamp.h"
#include "serde/envelope.h"
#include "serde/rw/enum.h"
#include "serde/rw/envelope.h"
#include "serde/rw/sstring.h"

namespace crash_tracker {

enum class crash_type {
    unknown,
    startup_exception,
    segfault,
    abort,
    illegal_instruction
};

struct crash_description
  : serde::
      envelope<crash_description, serde::version<0>, serde::compat_version<0>> {
    crash_type type;
    model::timestamp crash_time;
    ss::sstring crash_message;
    ss::sstring stacktrace;

    /// Extension to the crash_message. It can be used to add further
    /// information about the crash that is useful for debugging but is too
    /// verbose for telemetry.
    /// Eg. top-N allocations
    ss::sstring addition_info;

    auto serde_fields() {
        return std::tie(
          type, crash_time, crash_message, stacktrace, addition_info);
    }
};

} // namespace crash_tracker
