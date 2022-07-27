// Copyright 2019 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tsdb

import (
	"fmt"
	"github.com/go-kit/kit/log"
	"github.com/prometheus/prometheus/pkg/labels"
)

var InvalidTimesError = fmt.Errorf("max time is lesser than min time")

type MetricSample struct {
	TimestampMs int64
	Value       float64
	Labels      labels.Labels
}

// CreateHead creates a TSDB writer head to write the sample data to.
func CreateHead(samples []*MetricSample, chunkRange int64, logger log.Logger) (*Head, error) {
	head, err := NewHead("default", nil, logger, nil, chunkRange, DefaultStripeSize, DefaultWaterMark)
	if err != nil {
		return nil, err
	}
	app := head.Appender()
	for _, sample := range samples {
		_, err = app.Add(sample.Labels, sample.TimestampMs, sample.Value)
		if err != nil {
			return nil, err
		}
	}
	err = app.Commit()
	if err != nil {
		return nil, err
	}
	return head, nil
}
