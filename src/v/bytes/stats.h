#pragma once

#include <cstddef>

struct iobuf_stats {
    size_t copied_bytes{0};
    size_t needed_foreign_frees{0};
};

inline iobuf_stats& get_iobuf_stats() {
    static thread_local iobuf_stats stats{};
    return stats;
}
