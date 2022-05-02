package chunkenc

import (
	"github.com/oklog/ulid"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestXORChunk_IteratorWithCopyData(t *testing.T) {

	t.Log(ulid.ULID{}.String())
	for i := 0; i < 50; i++ {
		go RandomAdd()
	}

	time.Sleep(10 * time.Second)
}

func RandomAdd() {
	xorChunk := NewUnorderedXORChunk() //NewXORChunk()
	app, _ := xorChunk.Appender()
	ch := make(chan struct{}, 0)
	sampleBuf := make([]Point, 4)
	set := sync.Map{}
	counter := 0
	go func(c chan struct{}, app Appender) {
		for true {
			c <- struct{}{}
			v := float64(rand.Intn(10000000000))
			i := rand.Int63()
			i = int64(counter)
			sampleBuf[0] = sampleBuf[1]
			sampleBuf[1] = sampleBuf[2]
			sampleBuf[2] = sampleBuf[3]
			sampleBuf[3] = Point{t: i, v: v}
			_, ok := set.Load(i)
			if !ok {
				set.Store(sampleBuf[3].t, sampleBuf[3].v)
				app.Append(sampleBuf[3].t, sampleBuf[3].v)
			}
			//set[v] = struct{}{}
		}
	}(ch, app)
	time.Sleep(time.Second)

	go func(c chan struct{}, chk Chunk) {
		for true {
			iter := chk.Iterator(nil)
			<-c
			counter++
			for iter.Next() {
				t, v := iter.At()
				_, ok := set.Load(t)
				if !ok {
					println(t, "-----------", v, "counter:", counter)
				}
			}
			//println("----")
		}
	}(ch, xorChunk)

	time.Sleep(11 * time.Second)
	println("counter:", counter)
}

func BenchmarkAddRead(b *testing.B) {
	pool := NewPointPool()
	pool.addRead()
}

type PointPool struct {
	xorChunk  *UnorderedXORChunk
	ch        chan struct{}
	sampleBuf []Point
	app       Appender
	readBuf   []Point
	counter   int64
	temp      []Point
	//xorChunk := NewUnorderedXORChunk() //NewXORChunk()
	//app, _ := xorChunk.Appender()
	//ch := make(chan struct{}, 0)
	//sampleBuf := make([]Point, 4)
}

func NewPointPool() *PointPool {
	pool := &PointPool{
		xorChunk:  NewUnorderedXORChunk(),
		ch:        make(chan struct{}, 0),
		sampleBuf: make([]Point, 4),
		app:       nil,
		readBuf:   make([]Point, 4),
		temp:      make([]Point, 4),
	}
	app, _ := pool.xorChunk.Appender()
	pool.app = app
	return pool
}

func (p *PointPool) addRead() {
	go func() {
		for true {
			p.add()
		}
	}()
	time.Sleep(50 * time.Millisecond)

	go func() {
		for true {
			p.read()
		}
	}()
}

func (p *PointPool) add() {

	v := float64(rand.Intn(1000))
	i := atomic.AddInt64(&p.counter, 1)
	np := Point{t: i, v: v}
	p.app.Append(np.t, np.v)

	p.sampleBuf[0] = p.sampleBuf[1]
	p.sampleBuf[1] = p.sampleBuf[2]
	p.sampleBuf[2] = p.sampleBuf[3]
	p.sampleBuf[3] = np
	p.ch <- struct{}{}
}

func (p *PointPool) read() {
	<-p.ch
	iter := p.xorChunk.Iterator(nil)
	copy(p.readBuf, p.temp)
	for iter.Next() {
		t, v := iter.At()
		p.readBuf[0] = p.readBuf[1]
		p.readBuf[1] = p.readBuf[2]
		p.readBuf[2] = p.readBuf[3]
		p.readBuf[3] = Point{t: t, v: v}
	}
	if !same(p.sampleBuf, p.readBuf) {
		println("no same……")
		//panic("no same……")
	}

}

func same(a, b []Point) bool {
	if len(a) != len(b) {
		return false
	}
	for i, p := range a {
		if p.t != b[i].t {
			return false
		}
		if p.v != b[i].v {
			return false
		}
	}

	return true
}
