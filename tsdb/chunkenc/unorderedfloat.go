package chunkenc

const outOfOrderSize = 1<<4 - 1

type UnorderedFloatChunk struct {
	sortedPoints *sortedPoints
	xorChunk     *XORChunk
	appender     *combineAppender
}

func NewUnorderedFloatChunk() *UnorderedFloatChunk {
	return &UnorderedFloatChunk{xorChunk: NewXORChunk(), sortedPoints: new(sortedPoints)}
}

func (c *UnorderedFloatChunk) Bytes() []byte {
	c.merge()
	return c.xorChunk.Bytes()
}

func (c *UnorderedFloatChunk) Encoding() Encoding {
	return c.xorChunk.Encoding()
}

func (c *UnorderedFloatChunk) Appender() (Appender, error) {
	app, _ := c.xorChunk.Appender()
	c.appender = &combineAppender{
		xorLatest:   0,
		chunk:       c,
		xorAppender: app,
	}
	return c.appender, nil
}

func (c *UnorderedFloatChunk) Iterator(it Iterator) Iterator {
	if len(*c.sortedPoints) == 0 {
		return c.xorChunk.Iterator(it)
	}

	if c.xorChunk.NumSamples() == 0 {
		return &sortedSampleIterator{points: c.sortedPoints, current: -1}
	}

	iter, ok := it.(*unorderedFloatIterator)
	if !ok {
		iter = &unorderedFloatIterator{
			xorIter: c.xorChunk.iterator(nil),
			chaosIter: &sortedSampleIterator{
				points:  c.sortedPoints,
				current: -1,
			},
		}
	}
	return iter
}

func (c *UnorderedFloatChunk) NumSamples() int {
	return c.xorChunk.NumSamples() + len(*c.sortedPoints)
}

func (c *UnorderedFloatChunk) merge() (int, error) {
	if len(*c.sortedPoints) == 0 {
		return 0, nil
	}

	var xorChunk = NewXORChunk()
	app, err := xorChunk.Appender()
	if err != nil {
		return 0, err
	}

	byteCreated := 0
	iter := c.Iterator(nil)
	for iter.Next() {
		byteCreated += app.Append(iter.At())
	}

	c.xorChunk = xorChunk
	c.sortedPoints = new(sortedPoints)

	latest, _ := iter.At()
	c.appender.xorLatest = latest
	c.appender.xorAppender = app
	return byteCreated, nil

}

type unorderedFloatIterator struct {
	xorIter, chaosIter    Iterator
	aok, bok, initialized bool

	curT int64
	curV float64
}

func (c *unorderedFloatIterator) At() (int64, float64) {
	return c.curT, c.curV
}

func (c *unorderedFloatIterator) Err() error {
	if c.xorIter.Err() != nil {
		return c.xorIter.Err()
	}
	return c.chaosIter.Err()
}

func (c *unorderedFloatIterator) Next() bool {
	if !c.initialized {
		c.aok = c.xorIter.Next()
		c.bok = c.chaosIter.Next()
		c.initialized = true
	}

	if !c.aok && !c.bok {
		return false
	}

	if !c.aok {
		c.curT, c.curV = c.chaosIter.At()
		c.bok = c.chaosIter.Next()
		return true
	}
	if !c.bok {
		c.curT, c.curV = c.xorIter.At()
		c.aok = c.xorIter.Next()
		return true
	}
	acurT, acurV := c.xorIter.At()
	bcurT, bcurV := c.chaosIter.At()
	if acurT < bcurT {
		c.curT, c.curV = acurT, acurV
		c.aok = c.xorIter.Next()
	} else if acurT > bcurT {
		c.curT, c.curV = bcurT, bcurV
		c.bok = c.chaosIter.Next()
	} else {
		c.curT, c.curV = bcurT, bcurV
		c.aok = c.xorIter.Next()
		c.bok = c.chaosIter.Next()
	}
	return true

}

type combineAppender struct {
	chunk       *UnorderedFloatChunk
	xorAppender Appender
	xorLatest   int64
}

func (app *combineAppender) Append(t int64, v float64) (bytesCreated int) {
	if t > app.xorLatest {
		bytesCreated += app.xorAppender.Append(t, v)
		app.xorLatest = t
	} else {
		if len(*app.chunk.sortedPoints) > outOfOrderSize {
			bytesDelta, _ := app.chunk.merge()
			bytesCreated += bytesDelta
		}
		app.chunk.sortedPoints.insertOrUpdate(Point{t: t, v: v})
	}
	return
}

type Point struct {
	t int64
	v float64
}
type sortedPoints []Point

func (ps *sortedPoints) insertOrUpdate(p Point) (insert bool) {
	idx, size := 0, len(*ps)

	for i := size - 1; i >= 0; i-- {
		delta := (*ps)[i].t - p.t
		if delta < 0 {
			idx = i + 1
			break
		} else if delta == 0 {
			(*ps)[i].v = p.v
			return false
		}
	}
	if *ps == nil {
		*ps = append(make([]Point, 0, 4), p)
	} else if idx == size {
		*ps = append(*ps, p)
	} else {
		*ps = append((*ps)[:idx+1], (*ps)[idx:]...)
		(*ps)[idx] = p
	}
	return true
}

type sortedSampleIterator struct {
	points  *sortedPoints
	current int
}

func (p *sortedSampleIterator) At() (int64, float64) {
	point := (*p.points)[p.current]

	return point.t, point.v
}

func (p *sortedSampleIterator) Err() error {
	return nil
}

func (p *sortedSampleIterator) Next() bool {
	p.current += 1
	return len(*p.points) > p.current
}
