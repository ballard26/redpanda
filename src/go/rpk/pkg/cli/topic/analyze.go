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
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/kafka"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/utils"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func newAnalyzeCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		re                     bool
		timeout                time.Duration
		minBatchesPerPartition int
		all                    bool
		timeRange              string
	)
	cmd := &cobra.Command{
		Use:   "analyze [TOPIC]",
		Short: "Analyze a topic",
		Long:  helpAnalyze,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, topics []string) {
			ctx, cancel := context.WithTimeout(cmd.Context(), timeout)
			defer cancel()

			f := p.Formatter

			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			adm, err := kafka.NewAdmin(fs, p)
			out.MaybeDie(err, "failed to initialize admin kafka client: %v", err)
			defer adm.Close()

			if re {
				topics, err = regexTopics(adm, topics)
				out.MaybeDie(err, "failed to filter topics by regex: %v", err)
				if len(topics) == 0 {
					out.Die("no topics were found that match the given regex")
				}
			}

			err = validateTopics(ctx, adm, topics)
			out.MaybeDie(err, "failed to validate topics: %v", err)

			tr, err := parseTimeRange(timeRange, time.Now())
			out.MaybeDie(err, "failed to parse offset: %v", err)

			offsets, err := getOffsetsForTimeRange(ctx, adm, tr, topics)
			out.MaybeDie(err, "failed to get topic offsets: %v", err)

			topicPartitions := []topicPartition{}
			offsets.StartOffsets.Each(func(o kadm.Offset) {
				topicPartitions = append(topicPartitions, topicPartition{o.Topic, o.Partition})
			})

			c, err := newCollector(collectorConfig{
				TopicPartitions: topicPartitions,
				ConsumeRanges:   offsets,
				TimeRange:       tr,
			})
			out.MaybeDie(err, "failed to create collector: %v", err)

			clientCreationFn := func(opts []kgo.Opt) (*kgo.Client, error) {
				return kafka.NewFranzClient(fs, p, opts...)
			}

			err = c.collect(ctx, clientCreationFn)
			out.MaybeDie(err, "failed to collect from topic(s): %v", err)
			res := c.results()

			printed, err := res.printRawResults(f, os.Stdout)
			out.MaybeDie(err, "failed to print results: %v", err)
			if !printed {
				printCfg := printConfig{
					Percentiles:        []float64{25, 50, 75, 99},
					PrintGlobalSummary: true,
					PrintTopicSummary:  all,
				}
				err = res.printSummaries(printCfg)
				out.MaybeDie(err, "failed to print results: %v", err)
			}
		},
	}

	p.InstallFormatFlag(cmd)
	cmd.Flags().BoolVarP(&re, "regex", "r", false, "Parse arguments as regex; describe any topic that matches any input topic expression")
	cmd.Flags().DurationVar(&timeout, "timeout", 10*time.Second, "Specifies how long the command should run before timing out")
	cmd.Flags().IntVar(&minBatchesPerPartition, "batches", 10, "Minimum number of batches to consume per partition")
	cmd.Flags().BoolVarP(&all, "print-all", "a", false, "Print all sections")
	cmd.Flags().StringVarP(&timeRange, "time-range", "t", "-24h:end", "Time range to consume from (-24h:end, -48h:-24h, 2022-02-14:1h)")

	return cmd
}

func validateTopics(ctx context.Context, adm *kadm.Client, topics []string) error {
	listed, err := adm.ListTopics(ctx, topics...)
	if err != nil {
		return err
	}

	for _, td := range listed {
		if td.Err != nil {
			return fmt.Errorf("unable to access topic %q: %v", td.Topic, td.Err.Error())
		}
	}

	return nil
}

type offsetRanges struct {
	StartOffsets kadm.Offsets
	EndOffsets   kadm.Offsets
}

// For each topic/partition if there is offsets in `tr` then the offsets (so, eo)
// will be returned where `so` is after `tr.Start` and `eo` is after `tr.End`.
func getOffsetsForTimeRange(ctx context.Context, adm *kadm.Client, tr timeRange, topics []string) (offsetRanges, error) {
	offsets := offsetRanges{}

	lstart, err := adm.ListOffsetsAfterMilli(ctx, tr.Start.UnixMilli(), topics...)
	if err != nil {
		return offsets, err
	}
	if lstart.Error() != nil {
		return offsets, fmt.Errorf("unable to list start offsets: %v", lstart.Error())
	}
	offsets.StartOffsets = lstart.Offsets()

	lend, err := adm.ListOffsetsAfterMilli(ctx, tr.End.UnixMilli(), topics...)
	if err != nil {
		return offsets, err
	}
	if lend.Error() != nil {
		return offsets, fmt.Errorf("unable to list end offsets: %v", lend.Error())
	}
	offsets.EndOffsets = lend.Offsets()

	// Ensure every start offset has an end offset and vice-versa.
	offsets.StartOffsets.KeepFunc(func(so kadm.Offset) bool {
		_, exists := offsets.EndOffsets.Lookup(so.Topic, so.Partition)
		return exists
	})
	offsets.EndOffsets.KeepFunc(func(so kadm.Offset) bool {
		_, exists := offsets.StartOffsets.Lookup(so.Topic, so.Partition)
		return exists
	})

	return offsets, nil
}

type timeRange struct {
	Start time.Time `json:"start" yaml:"start"`
	End   time.Time `json:"end" yaml:"end"`
}

func parseTimeRange(rangeStr string, currentTimestamp time.Time) (tr timeRange, err error) {
	length, startAt, end, fromTimestamp, err := parseTimestampBasedOffset(rangeStr, currentTimestamp)
	tr.Start = startAt
	if err != nil {
		return
	} else if end {
		err = errors.New("'end' is not a valid value for t1 in 't1:t2'")
		return
	} else if length == len(rangeStr) {
		err = errors.New("timerange must be of form 't1:t2'")
		return
	}

	relativeTimestamp := currentTimestamp
	if fromTimestamp {
		relativeTimestamp = tr.Start
	}

	if rangeStr[length] != ':' {
		err = errors.New("timerange must be of form 't1:t2'")
		return
	}

	rangeStr = rangeStr[length+1:]
	_, tr.End, end, _, err = parseTimestampBasedOffset(rangeStr, relativeTimestamp)

	if end {
		tr.End = currentTimestamp
	}

	if !tr.End.After(tr.Start) {
		err = errors.New("t1 has to be less than t2 in timerange t1:t2")
	}

	return
}

type topicPartition struct {
	Topic     string `json:"topic" yaml:"topic"`
	Partition int32  `json:"partition" yaml:"partition"`
}

type topicPartitionInfo struct {
	mux sync.Mutex // grab for all upates to topicPartitionInfo's fields

	StartTS        time.Time `json:"startTimestamp" yaml:"start_timestamp"`
	EndTS          time.Time `json:"endTimestamp" yaml:"end_timestamp"`
	BatchesRead    int       `json:"batchesRead" yaml:"batches_read"`
	BatchSizes     []int     `json:"batchSizes" yaml:"batch_sizes"`
	RecordsRead    int       `json:"recordsRead" yaml:"records_read"`
	LastOffsetRead int64     `json:"lastOffsetRead" yaml:"last_offset_read"`
	// Any members below are set at intiialization and constant thereafter.
	TargetStartOffset int64 `json:"targetStartOffset" yaml:"target_start_offset"`
	TargetEndOffset   int64 `json:"targetEndOffset" yaml:"target_end_offset"`
}

// Returns true if no samples were taken from the topic/partition.
func (tp *topicPartitionInfo) empty() bool {
	return tp.BatchesRead == 0
}

type tpInfoMap map[topicPartition]*topicPartitionInfo

type collectorConfig struct {
	TopicPartitions []topicPartition
	ConsumeRanges   offsetRanges
	TimeRange       timeRange
}

type collector struct {
	cfg collectorConfig

	tpInfoMux sync.Mutex // grab for any r/w's to tpInfo
	tpInfo    tpInfoMap

	tpsRemaining map[topicPartition]struct{}
}

func newCollector(cfg collectorConfig) (*collector, error) {
	tpInfo := make(map[topicPartition]*topicPartitionInfo)

	for _, tp := range cfg.TopicPartitions {
		so, _ := cfg.ConsumeRanges.StartOffsets.Lookup(tp.Topic, tp.Partition)
		eo, _ := cfg.ConsumeRanges.EndOffsets.Lookup(tp.Topic, tp.Partition)
		tpInfo[tp] = &topicPartitionInfo{
			TargetStartOffset: so.At,
			TargetEndOffset:   eo.At,
		}
	}

	// Only collect from topic/partitions if the starting offset is less than the end.
	cfg.ConsumeRanges.StartOffsets.KeepFunc(func(so kadm.Offset) bool {
		eo, _ := cfg.ConsumeRanges.EndOffsets.Lookup(so.Topic, so.Partition)
		return so.At < eo.At
	})

	c := &collector{
		cfg:          cfg,
		tpInfo:       tpInfo,
		tpsRemaining: make(map[topicPartition]struct{}),
	}

	return c, nil
}

func (c *collector) getClientOps() ([]kgo.Opt, error) {
	opts := []kgo.Opt{
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	}

	startOffsets := make(map[string]map[int32]kgo.Offset)
	c.cfg.ConsumeRanges.StartOffsets.Each(func(so kadm.Offset) {
		o := kgo.NewOffset().At(so.At)
		if _, exists := startOffsets[so.Topic]; !exists {
			startOffsets[so.Topic] = make(map[int32]kgo.Offset)
		}
		startOffsets[so.Topic][so.Partition] = o
	})

	opts = append(opts, kgo.ConsumePartitions(startOffsets))
	opts = append(opts, kgo.WithHooks(c))

	return opts, nil
}

func (c *collector) getTPInfo(topic string, partition int32) *topicPartitionInfo {
	c.tpInfoMux.Lock()
	defer c.tpInfoMux.Unlock()

	tp := topicPartition{topic, partition}
	return c.tpInfo[tp]
}

func (c *collector) OnFetchRecordBuffered(r *kgo.Record) {
	info := c.getTPInfo(r.Topic, r.Partition)
	info.mux.Lock()
	defer info.mux.Unlock()

	if info.StartTS.IsZero() {
		info.StartTS = r.Timestamp
	}

	if info.EndTS.Before(r.Timestamp) {
		info.EndTS = r.Timestamp
	}

	info.RecordsRead += 1
	info.LastOffsetRead = r.Offset
}

func (c *collector) OnFetchBatchRead(_ kgo.BrokerMetadata, topic string, partition int32, metrics kgo.FetchBatchMetrics) {
	info := c.getTPInfo(topic, partition)
	info.mux.Lock()
	defer info.mux.Unlock()

	info.BatchesRead += 1
	info.BatchSizes = append(info.BatchSizes, metrics.CompressedBytes)
}

var ( // Ensure collector correctly implements the hook interfaces.
	_ kgo.HookFetchBatchRead      = new(collector)
	_ kgo.HookFetchRecordBuffered = new(collector)
)

func (c *collector) checkForCompletedTPs() map[string][]int32 {
	ret := make(map[string][]int32)

	for tp := range c.tpsRemaining {
		eo, _ := c.cfg.ConsumeRanges.EndOffsets.Lookup(tp.Topic, tp.Partition)
		info := c.getTPInfo(tp.Topic, tp.Partition)
		info.mux.Lock()
		completed := info.LastOffsetRead >= (eo.At - 1)
		info.mux.Unlock()

		if completed {
			ret[tp.Topic] = append(ret[tp.Topic], tp.Partition)
			delete(c.tpsRemaining, tp)
		}
	}

	return ret
}

func (c *collector) isDone() bool {
	return len(c.tpsRemaining) == 0
}

func (c *collector) collect(ctx context.Context, clCreationFn func([]kgo.Opt) (*kgo.Client, error)) error {
	if len(c.cfg.ConsumeRanges.StartOffsets) == 0 {
		// Nothing to collect.
		return nil
	}

	opts, err := c.getClientOps()
	if err != nil {
		return err
	}

	// Creating and closing the kafka client in this method is important as the goroutines it
	// creates concurrently access `a.tpInfo`.
	cl, err := clCreationFn(opts)
	if err != nil {
		return err
	}
	defer cl.Close()

	for _, tp := range c.cfg.TopicPartitions {
		_, exists := c.cfg.ConsumeRanges.StartOffsets.Lookup(tp.Topic, tp.Partition)
		if exists {
			c.tpsRemaining[tp] = struct{}{}
		}
	}

	for {
		fetches := cl.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			return err
		}

		// Note that the fetched records are not being read here. Instead two client hooks are registered
		// "HookFetchBatchRead" and "HookFetchRecordBuffered".
		//
		// The "HookFetchBatchRead" is the only way to get the number of batches read and the compressed
		// batch size. This hook, however, is called for every internally read batch regardless of whether
		// its records end up being returned from "PollFetches" or not.
		//
		// Hence "HookFetchRecordBuffered" is used to ensure that all records have been read for every batch
		// "HookFetchBatchRead" is called for.

		completedTPs := c.checkForCompletedTPs()
		cl.PauseFetchPartitions(completedTPs)

		if c.isDone() {
			return nil
		}
	}
}

type collectResult struct {
	TopicPartitionInfo tpInfoMap `json:"topicPartitionInfo" yaml:"topic_partition_info"`
	TimeRange          timeRange `json:"timeRange" yaml:"time_range"`
}

func (c *collector) results() collectResult {
	return collectResult{c.tpInfo, c.cfg.TimeRange}
}

type percentileValue[V any] struct {
	percentile float64
	value      V
}

type topicSummary struct {
	Topic            string
	Partitions       int
	BatchesPerSecond []percentileValue[float64]
	BatchSizes       []percentileValue[int] // bytes
}

func (cr *collectResult) summarizeTopics(percentiles []float64) ([]topicSummary, error) {
	totalDuration := cr.TimeRange.End.Sub(cr.TimeRange.Start)
	tps := map[string]struct {
		Partitions []int
		BatchSizes []int
		BatchRates []float64
	}{}
	for tp, info := range cr.TopicPartitionInfo {
		e := tps[tp.Topic]
		e.Partitions = append(e.Partitions, int(tp.Partition))

		if info.empty() {
			tps[tp.Topic] = e
			continue
		}

		batchesPerS := float64(info.BatchesRead) / totalDuration.Seconds()
		e.BatchRates = append(e.BatchRates, batchesPerS)

		if !slices.IsSorted(info.BatchSizes) {
			slices.Sort(info.BatchSizes)
		}

		p50, err := utils.Percentile(info.BatchSizes, 50.0)
		if err != nil {
			return nil, err
		}
		// Note that we insert the p50 batch size for each partition here.
		// Another option could be inserting every sampled batch size then
		// take percentiles of that. However, since each partition may have
		// a different number of sampled batches care would need to be taken
		// to insert the same number of samples per partition to avoid bias.
		e.BatchSizes = append(e.BatchSizes, p50)

		tps[tp.Topic] = e
	}

	tss := []topicSummary{}
	for t, i := range tps {
		ts := topicSummary{Topic: t, Partitions: len(i.Partitions)}

		slices.Sort(i.BatchSizes)
		slices.Sort(i.BatchRates)
		for _, p := range percentiles {
			if len(i.BatchSizes) != 0 {
				pbs, err := utils.Percentile(i.BatchSizes, p)
				if err != nil {
					return nil, err
				}
				ts.BatchSizes = append(ts.BatchSizes, percentileValue[int]{p, pbs})
			} else {
				ts.BatchSizes = append(ts.BatchSizes, percentileValue[int]{p, 0})
			}

			if len(i.BatchRates) != 0 {
				pbr, err := utils.Percentile(i.BatchRates, p)
				if err != nil {
					return nil, err
				}
				ts.BatchesPerSecond = append(ts.BatchesPerSecond, percentileValue[float64]{p, pbr})
			} else {
				ts.BatchesPerSecond = append(ts.BatchesPerSecond, percentileValue[float64]{p, 0})
			}
		}

		tss = append(tss, ts)
	}

	return tss, nil
}

type globalSummary struct {
	Topics                int
	Partitions            int
	TotalBatchesPerSecond float64
	BatchesPerSecond      []percentileValue[float64]
	BatchSizes            []percentileValue[int] // bytes
}

func (cr *collectResult) summarizeGlobal(percentiles []float64) (globalSummary, error) {
	summary := globalSummary{}
	parts := 0
	totalBatches := 0
	totalDuration := cr.TimeRange.End.Sub(cr.TimeRange.Start)
	topics := map[string]struct{}{}
	batchSizes := []int{}
	batchRates := []float64{}

	for tp, info := range cr.TopicPartitionInfo {
		parts += 1
		topics[tp.Topic] = struct{}{}

		if info.empty() {
			continue
		}

		batchesPerS := float64(info.BatchesRead) / totalDuration.Seconds()
		batchRates = append(batchRates, batchesPerS)
		totalBatches += info.BatchesRead

		if !slices.IsSorted(info.BatchSizes) {
			slices.Sort(info.BatchSizes)
		}

		p50, err := utils.Percentile(info.BatchSizes, 50.0)
		if err != nil {
			return summary, err
		}
		batchSizes = append(batchSizes, p50)
	}

	summary.Partitions = parts
	summary.Topics = len(topics)
	summary.TotalBatchesPerSecond = float64(totalBatches) / totalDuration.Seconds()

	slices.Sort(batchSizes)
	slices.Sort(batchRates)
	for _, p := range percentiles {
		if len(batchRates) != 0 {
			pBatchRate, err := utils.Percentile(batchRates, p)
			if err != nil {
				return summary, err
			}
			summary.BatchesPerSecond = append(summary.BatchesPerSecond, percentileValue[float64]{p, pBatchRate})
		} else {
			summary.BatchesPerSecond = append(summary.BatchesPerSecond, percentileValue[float64]{p, 0})
		}

		if len(batchSizes) != 0 {
			pBatchSize, err := utils.Percentile(batchSizes, p)
			if err != nil {
				return summary, err
			}
			summary.BatchSizes = append(summary.BatchSizes, percentileValue[int]{p, pBatchSize})
		} else {
			summary.BatchSizes = append(summary.BatchSizes, percentileValue[int]{p, 0})
		}
	}

	return summary, nil
}

type printConfig struct {
	Percentiles        []float64
	PrintGlobalSummary bool
	PrintTopicSummary  bool
}

func (cr *collectResult) printSummaries(cfg printConfig) error {
	percentiles := cfg.Percentiles
	percentileNames := []string{}
	for _, p := range percentiles {
		percentileNames = append(percentileNames, fmt.Sprintf("P%v", p))
	}

	const (
		secSummary   = "summary"
		secBatchRate = "partition batch rate (1/s)"
		secBatchSize = "partition batch size (bytes)"
	)

	ts, err := cr.summarizeTopics(percentiles)
	if err != nil {
		return err
	}
	// Print the topic summaries in asc order by topic name
	slices.SortFunc(ts, func(a topicSummary, b topicSummary) int {
		return strings.Compare(a.Topic, b.Topic)
	})

	gs, err := cr.summarizeGlobal([]float64{50})
	if err != nil {
		return err
	}

	sections := out.NewMaybeHeaderSections(
		out.ConditionalSectionHeaders(map[string]bool{
			secSummary:   cfg.PrintGlobalSummary,
			secBatchRate: cfg.PrintTopicSummary,
			secBatchSize: cfg.PrintTopicSummary,
		})...,
	)

	sections.Add(secSummary, func() {
		tw := out.NewTabWriter()
		tw.PrintColumn("topics", gs.Topics)
		tw.PrintColumn("partitions", gs.Partitions)
		tw.PrintColumn("total batch rate (1/s)", gs.TotalBatchesPerSecond)
		tw.PrintColumn("p50 batch size (bytes)", gs.BatchSizes[0].value)
		tw.Flush()
	})

	sections.Add(secBatchRate, func() {
		tw := out.NewTable(append([]string{"TOPIC"}, percentileNames...)...)
		defer tw.Flush()
		for _, ts := range ts {
			vals := []interface{}{}
			for _, pv := range ts.BatchesPerSecond {
				vals = append(vals, pv.value)
			}

			tw.Print(append([]interface{}{ts.Topic}, vals...)...)
		}
	})

	sections.Add(secBatchSize, func() {
		tw := out.NewTable(append([]string{"TOPIC"}, percentileNames...)...)
		defer tw.Flush()
		for _, ts := range ts {
			vals := []interface{}{}
			for _, pv := range ts.BatchSizes {
				vals = append(vals, pv.value)
			}

			tw.Print(append([]interface{}{ts.Topic}, vals...)...)
		}
	})
	return nil
}

type kvTPInfo struct {
	TopicPartition topicPartition      `json:"topicPartition" yaml:"topic_partition"`
	Info           *topicPartitionInfo `json:"info" yaml:"info"`
}

type rawResults struct {
	TopicPartitionInfo []kvTPInfo `json:"topicPartitionInfo" yaml:"topic_partition_info"`
	TimeRange          timeRange  `json:"timeRange" yaml:"time_range"`
}

func (cr collectResult) printRawResults(f config.OutFormatter, w io.Writer) (bool, error) {
	// Json doesn't seem to like serializing the map types. So convert it to a slice here.
	kvs := []kvTPInfo{}
	for k, v := range cr.TopicPartitionInfo {
		kvs = append(kvs, kvTPInfo{k, v})
	}

	res := rawResults{kvs, cr.TimeRange}
	if isText, _, t, err := f.Format(res); !isText {
		if err != nil {
			return false, err
		}

		fmt.Fprintln(w, t)
		return true, nil
	}
	return false, nil
}

const helpAnalyze = `Analyze topics.

This command consumes records from the specified topics in order to determine
characteristics of the topic like batch rate and batch size.

Using the --format flag with either JSON or YAML prints all of the metadata collected.

TOPICS

Topics can either be listed individually or by regex via the --regex flag (-r).

For example,

    analyze foo bar            # describe topics foo and bar
    analyze -r '^f.*' '.*r$'   # describe any topic starting with f and any topics ending in r
    analyze -r '*'             # describe all topics
    analyze -r .               # describe any one-character topics

TIME RANGE

The --time-range flag allows for specifying what time range to consume from.
The time range should be of the following format:

    t1:t2    consume from timestamp t1 until timestamp t2

There are a few options for timestamps, with each option being evaluated
until one succeeds:

    13 digits             parsed as a unix millisecond
    9 digits              parsed as a unix second
    YYYY-MM-DD            parsed as a day, UTC
    YYYY-MM-DDTHH:MM:SSZ  parsed as RFC3339, UTC; fractional seconds optional (.MMM)
    end                   for t2 in @t1:t2, the current end of the partition
    -dur                  a negative duration from now or from a timestamp
    dur                   a positive duration from now or from a timestamp

Durations can be relative to the current time or relative to a timestamp.
If a duration is used for t1, that duration is relative to now.
If a duration is used for t2, if t1 is a timestamp, then t2 is relative to t1.
If a duration is used for t2, if t1 is a duration, then t2 is relative to now.

Durations are parsed simply:

    3ms    three milliseconds
    10s    ten seconds
    9m     nine minutes
    1h     one hour
    1m3ms  one minute and three milliseconds

For example,

    -t 2022-02-14:1h   consume 1h of time on Valentine's Day 2022
    -t -48h:-24h       consume from 2 days ago to 1 day ago
    -t -1m:end         consume from 1m ago until now
`
