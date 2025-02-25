// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package topic

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseTimeRange(t *testing.T) {
	baseTime := time.Now().UTC()
	tests := []struct {
		name     string
		rs       string
		tr       timeRange
		hasError bool
	}{
		{name: "no lhs", rs: ":-24h", hasError: true},
		{name: "no rhs", rs: "-24h:", hasError: true},
		{name: "end:end", rs: "end:end", hasError: true},
		{name: "just :", rs: ":", hasError: true},
		{name: "single date", rs: "2022-03-24", hasError: true},
		{name: "same date", rs: "2022-03-24:2022-03-24", hasError: true},
		{name: "out of order date", rs: "2022-03-25:2022-03-24", hasError: true},
		{name: "past end", rs: "24h:end", hasError: true},
		{name: "nonsense", rs: "-24h:-48h", hasError: true},
		{
			name: "relative to end",
			rs:   "-24h:end",
			tr:   timeRange{baseTime.Add(-24 * time.Hour), baseTime},
		},
		{
			name: "two negative durations",
			rs:   "-48h:-24h",
			tr:   timeRange{baseTime.Add(-48 * time.Hour), baseTime.Add(-24 * time.Hour)},
		},
		{
			name: "date til end",
			rs:   "2022-03-24:end",
			tr:   timeRange{time.Unix(1648080000, 0).UTC(), baseTime},
		},
		{
			name: "date to date",
			rs:   "2022-03-24:2022-03-25",
			tr:   timeRange{time.Unix(1648080000, 0).UTC(), time.Unix(1648080000, 0).Add(24 * time.Hour).UTC()},
		},
	}

	for _, test := range tests {
		tr, err := parseTimeRange(test.rs, baseTime)
		if test.hasError {
			require.Equal(t, test.hasError, err != nil, "test(%v)", test.name)
		} else {
			require.Equal(t, test.tr.Start, tr.Start, "test(%v)", test.name)
			require.Equal(t, test.tr.End, tr.End, "test(%v)", test.name)
			require.Equal(t, test.hasError, err != nil, "test(%v)", test.name)
		}
	}
}

func TestEmptyCollector(t *testing.T) {
	cfg := collectorConfig{
		TopicPartitions: []topicPartition{},
		ConsumeRanges:   offsetRanges{},
		TimeRange:       timeRange{time.Now().Add(-24 * time.Hour), time.Now()},
	}
	c, err := newCollector(cfg)
	require.NoError(t, err)
	res := c.results()

	_, err = res.summarizeGlobal([]float64{1})
	require.NoError(t, err)
	_, err = res.summarizeTopics([]float64{1})
	require.NoError(t, err)
}
