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

#include "crash_tracker/service.h"

#include "crash_tracker/limiter.h"
#include "crash_tracker/recorder.h"

#include <seastar/core/coroutine.hh>

namespace crash_tracker {

service::service() noexcept
  : _limiter(get_recorder()) {}

} // namespace crash_tracker
