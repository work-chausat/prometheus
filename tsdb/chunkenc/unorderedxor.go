package chunkenc

import "github.com/prometheus/prometheus/tsdb/waterlevel"

const outOfOrderSize = 1<<4 - 1

type UnorderedXORChunk struct {
	sortedPoints *sortedPoints

	xorChunk *XORChunk
	xorApp   *xorAppender
}

func NewUnorderedXORChunk() *UnorderedXORChunk {
	xorChunk := NewXORChunk()
	waterlevel.Delta(len(xorChunk.Bytes()))

	app, _ := xorChunk.Appender()
	return &UnorderedXORChunk{
		xorChunk: xorChunk,
		xorApp:   app.(*xorAppender),
	}
}

func (c *UnorderedXORChunk) merge(force bool) error {
	if c.sortedPoints == nil || !force && len(*c.sortedPoints) < outOfOrderSize {
		return nil
	}

	xorChunk := NewXORChunk()
	app, err := xorChunk.Appender()
	if err != nil {
		return err
	}

	iter := c.mergeIter(nil, force)
	for iter.Next() {
		app.Append(iter.At())
	}

	waterlevel.Delta(len(xorChunk.Bytes()) - len(c.xorChunk.Bytes()))
	c.xorChunk = xorChunk
	c.xorApp = app.(*xorAppender)

	if force {
		c.sortedPoints = nil
	} else {
		*c.sortedPoints = (*c.sortedPoints)[:0]
	}

	return nil
}

func (c *UnorderedXORChunk) Encoding() Encoding {
	return c.xorChunk.Encoding()
}

func (c *UnorderedXORChunk) Bytes() []byte {
	c.merge(true)
	return c.xorChunk.Bytes()
}

func (c *UnorderedXORChunk) NumSamples() int {
	if c.sortedPoints == nil {
		return c.xorChunk.NumSamples()
	}
	return c.xorChunk.NumSamples() + len(*c.sortedPoints)
}

func (c *UnorderedXORChunk) Appender() (Appender, error) {
	return c, nil
}

func (c *UnorderedXORChunk) Append(t int64, v float64) {
	if t > c.xorApp.t {
		bytesNum := len(c.xorChunk.Bytes())
		c.xorApp.Append(t, v)
		waterlevel.Delta(len(c.xorChunk.Bytes()) - bytesNum)
	} else {
		if c.sortedPoints == nil {
			c.sortedPoints = new(sortedPoints)
		}
		c.sortedPoints.insertOrUpdate(t, v)
	}

	c.merge(false)
}

func (c *UnorderedXORChunk) Iterator(it Iterator) Iterator {
	return c.mergeIter(it, true)
}

func (c *UnorderedXORChunk) mergeIter(it Iterator, copyOnWrite bool) Iterator {
	if c.sortedPoints == nil || len(*c.sortedPoints) == 0 {
		return c.xorChunk.iterator(it)
	}

	var points []Point
	if copyOnWrite {
		points = make([]Point, len(*c.sortedPoints))
		copy(points, *c.sortedPoints)
	} else {
		points = *c.sortedPoints
	}

	return &mergeIterator{
		aIt: c.xorChunk.iterator(it),
		bIt: &sortedSampleIterator{
			points: points,
			i:      -1,
		},
	}
}

type Point struct {
	t int64
	v float64
}
type sortedPoints []Point

func (ps *sortedPoints) insertOrUpdate(t int64, v float64) (insert bool) {
	idx, size := 0, len(*ps)

	for i := size - 1; i >= 0; i-- {
		delta := (*ps)[i].t - t
		if delta < 0 {
			idx = i + 1
			break
		} else if delta == 0 {
			(*ps)[i].v = v
			return false
		}
	}

	if *ps == nil {
		*ps = append(make([]Point, 0, 4), Point{t: t, v: v})
	} else if idx == size {
		*ps = append(*ps, Point{t: t, v: v})
	} else {
		*ps = append((*ps)[:idx+1], (*ps)[idx:]...)
		(*ps)[idx] = Point{t: t, v: v}
	}
	return true
}

type sortedSampleIterator struct {
	points []Point
	i      int
}

func (p *sortedSampleIterator) At() (int64, float64) {
	return p.points[p.i].t, p.points[p.i].v
}

func (p *sortedSampleIterator) Err() error {
	return nil
}

func (p *sortedSampleIterator) Next() bool {
	if p.i+1 < len(p.points) {
		p.i++
		return true
	}
	return false
}
