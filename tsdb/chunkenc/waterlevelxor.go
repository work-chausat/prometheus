package chunkenc

import (
	"github.com/prometheus/prometheus/tsdb/waterlevel"
)

type WaterLevelXORChunk struct {
	*XORChunk
}

func NewWaterLevelXORChunk() *WaterLevelXORChunk {
	c := NewXORChunk()
	waterlevel.Delta(len(c.Bytes()))
	return &WaterLevelXORChunk{
		XORChunk: c,
	}
}

func (c *WaterLevelXORChunk) Appender() (Appender, error) {
	a, err := c.XORChunk.Appender()
	if err != nil {
		return nil, err
	}
	return &waterLevelXORAppender{
		xorAppender: a.(*xorAppender),
	}, nil
}

type waterLevelXORAppender struct {
	*xorAppender
}

func (a *waterLevelXORAppender) Append(t int64, v float64) {
	bytesNum := len(a.b.bytes())
	a.xorAppender.Append(t, v)
	waterlevel.Delta(len(a.b.bytes()) - bytesNum)
}
