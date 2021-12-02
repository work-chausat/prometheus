package chunkenc

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"
)

var origin = make(map[int64]float64)

func init() {
	for i := 1; i < 10000; i++ {
		t := int64(rand.Int31n(100000000))
		origin[t] = float64(i)
	}
}

func Test_combineAppender_Append(t *testing.T) {
	start := time.Now()
	chunk := NewUnorderedXORChunk()
	app, _ := chunk.Appender()
	expected := make(map[int64]float64)

	for i, v := range origin {
		app.Append(i, v)
		expected[i] = v
	}

	fmt.Println("samples size:", chunk.NumSamples(), "cost:", time.Now().Sub(start).Milliseconds())

	actual := make(map[int64]float64)
	iter := chunk.Iterator(nil)
	for iter.Next() {
		t, v := iter.At()
		val := v
		actual[t] = val
	}

	if !reflect.DeepEqual(expected, actual) {
		fmt.Errorf("unexpected result\n\ngot: %v\n\nexp: %v", expected, actual)
	}

}

func BenchmarkUnorderedAppender(b *testing.B) {
	benchmarkAppender(b, func() Chunk {
		return NewUnorderedXORChunk()
	})
}

func BenchmarkUnordered_ChaosData(b *testing.B) {
	benchmarkUnorderedAppender(b, func() Chunk {
		return NewUnorderedXORChunk()
	})
}

func benchmarkUnorderedAppender(b *testing.B, newChunk func() Chunk) {
	var (
		t = int64(1234123324)
		v = 1243535.123
	)
	var exp []pair
	for i := 0; i < b.N; i++ {
		if i%10 == 0 {
			t -= int64(rand.Intn(1000))
		} else {
			t += int64(1000)
		}
		// v = rand.Float64()
		v += float64(100)
		exp = append(exp, pair{t: t, v: v})
	}

	b.ReportAllocs()
	b.ResetTimer()

	var chunks []Chunk
	for i := 0; i < b.N; {
		c := newChunk()

		a, err := c.Appender()
		if err != nil {
			b.Fatalf("get appender: %s", err)
		}
		j := 0
		for _, p := range exp {
			if j > 250 {
				break
			}
			a.Append(p.t, p.v)
			i++
			j++
		}
		chunks = append(chunks, c)
	}

	fmt.Println("num", b.N, "created chunks", len(chunks))
}

func BenchmarkUnorderedIterator(b *testing.B) {
	benchmarkIterator(b, func() Chunk {
		return NewUnorderedXORChunk()
	})
}

func BenchmarkCopy(b *testing.B) {
	cap := 16
	source := make([]Point, 0, cap)
	for i := 0; i < cap; i++ {
		source = append(source, Point{
			t: int64(i),
			v: float64(i),
		})
	}
	b.ReportAllocs()
	b.ResetTimer()
	//println("bbbb")

	for i := 0; i < b.N; i++ {
		dst := make([]Point, 0, len(source))
		copy(dst, source)
	}
}
