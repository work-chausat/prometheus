// Copyright 2018 The Prometheus Authors
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
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/util/testutil"
)

func BenchmarkHeadStripeSeriesCreate(b *testing.B) {
	// Put a series, select it. GC it and then access it.
	h, err := NewHead("default", nil, nil, nil, 1000, DefaultStripeSize, DefaultWaterMark)
	testutil.Ok(b, err)
	defer h.Close()

	for i := 0; i < b.N; i++ {
		h.getOrCreate(uint64(i), labels.FromStrings("a", "a"+strconv.Itoa(i),
			"b", "2916750b48b9dc11672a7c597814b0d4326f8de26a013c6292eee70e5e7fb43f"+strconv.Itoa(i),
			"c", "2916750b48b9dc1160d4326f8de26a013c6292eee70e5e7fb43fc"+strconv.Itoa(i),
			"d", "de26a013c6292eee70e5e7fb43fd"+strconv.Itoa(i),
			//"e", "2916750b48b9dc11672a7c597814b0d4326f8de26a013c6292eee70e5e7fb43fe"+strconv.Itoa(i),
			//"f", "2916750b48b9dc11672a7c597814b0d4326f8de26a013c6292eee70e5e7fb43ff"+strconv.Itoa(i),
			//"g", "2916750b48b9dc11672a7c597814b0d4326f8de26a013c6292eee70e5e7fb43fg"+strconv.Itoa(i),
			//"h", "2916750b48b9dc11672a7c597814b0d4326f8de26a013c6292eee70e5e7fb43fh"+strconv.Itoa(i),
			//"i", "2916750b48b9dc11672a7c597814b0d4326f8de26a013c6292eee70e5e7fb43fi"+strconv.Itoa(i),
			//"j", "2916750b48b9dc11672a7c597814b0d4326f8de26a013c6292eee70e5e7fb43fj"+strconv.Itoa(i),
		))
	}
}

func BenchmarkHeadStripeSeriesCreateParallel(b *testing.B) {
	// Put a series, select it. GC it and then access it.
	h, err := NewHead("default", nil, nil, nil, 1000, DefaultStripeSize, DefaultWaterMark)
	testutil.Ok(b, err)
	defer h.Close()

	var count int64

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := int(atomic.AddInt64(&count, 1))
			h.getOrCreate(uint64(i), labels.FromStrings("a", "a"+strconv.Itoa(i),
				"b", "2916750b48b9dc11672a7c597814b0d4326f8de26a013c6292eee70e5e7fb43f"+strconv.Itoa(i),
				"c", "2916750b48b9dc11672a7c597814b0d4326f8de26a013c6292eee70e5e7fb43fc"+strconv.Itoa(i),
				"d", "2916750b48b9dc11672a7c597814b0d4326f8de26a013c6292eee70e5e7fb43fd"+strconv.Itoa(i),
				"e", "2916750b48b9dc11672a7c597814b0d4326f8de26a013c6292eee70e5e7fb43fe"+strconv.Itoa(i),
				"f", "2916750b48b9dc11672a7c597814b0d4326f8de26a013c6292eee70e5e7fb43ff"+strconv.Itoa(i),
				"g", "2916750b48b9dc11672a7c597814b0d4326f8de26a013c6292eee70e5e7fb43fg"+strconv.Itoa(i),
				"h", "2916750b48b9dc11672a7c597814b0d4326f8de26a013c6292eee70e5e7fb43fh"+strconv.Itoa(i),
				"i", "2916750b48b9dc11672a7c597814b0d4326f8de26a013c6292eee70e5e7fb43fi"+strconv.Itoa(i),
				"j", "2916750b48b9dc11672a7c597814b0d4326f8de26a013c6292eee70e5e7fb43fj"+strconv.Itoa(i),
			))
		}
	})
}
