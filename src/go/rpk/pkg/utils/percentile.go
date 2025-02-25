// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package utils

import (
	"fmt"
	"math"
)

type Number interface {
	int | float64
}

// Uses the nearest-rank method to calculate the value of a given percentile.
// Expects `sortedData` to be sorted in ascending order.
// Expects `percent` to be a value in [0, 100].
func Percentile[V Number](sortedData []V, percent float64) (V, error) {
	if len(sortedData) == 0 {
		return 0, fmt.Errorf("empty input")
	}
	if percent > 100 || percent < 0 {
		return 0, fmt.Errorf("percent must be a value in [0, 100]")
	}

	i := int64(math.Ceil((percent/100.0)*float64(len(sortedData))) - 1)
	i = max(i, 0)

	return sortedData[i], nil
}
