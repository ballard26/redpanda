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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPercentile(t *testing.T) {
	tests := []struct {
		name     string
		data     []int
		percent  float64
		output   int
		hasError bool
	}{
		{"test0", []int{11, 12, 13, 14}, 0.0, 11, false},
		{"test1", []int{11, 12, 13, 14}, 25.0, 11, false},
		{"test2", []int{11, 12, 13, 14}, 45.0, 12, false},
		{"test4", []int{11, 12, 13, 14}, 100.0, 14, false},
		{"empty data", []int{}, 0.0, 0, true},
		{"positive out of bounds percent", []int{}, 1000.0, 0, true},
		{"negative out of bounds percent", []int{}, -1000.0, 0, true},
	}

	for _, test := range tests {
		res, err := Percentile(test.data, test.percent)
		require.Equal(t, test.output, res, "output: %v != %v in test %v", res, test.output, test.name)
		require.Equal(t, test.hasError, err != nil, "unexpected error value in test %v", test.name)
	}
}
