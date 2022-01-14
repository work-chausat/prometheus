package tsdbutil

import (
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultShardCount = 1 << 8
	defaultLifeWindow = 60
)

var nowSecond = uint32(0)

type StringBank struct {
	shards        []*shard
	shardCount    uint32
	lifeWindowSec uint32
}

type shard struct {
	items []*entity

	sync.RWMutex
	_ [40]byte
}

type entity struct {
	value      string
	latestUsed uint32
}

func NewStringBank(shards uint32) *StringBank {
	return NewStringBankWithExpire(shards, defaultLifeWindow)
}

func NewStringBankWithExpire(shardCount uint32, lifeWindowSec uint32) *StringBank {
	// must be a power of 2
	if shardCount < 1 {
		shardCount = defaultShardCount
	} else if shardCount&(shardCount-1) != 0 {
		panic("shardCount must be a power of 2")
	}

	m := &StringBank{
		shards:        make([]*shard, shardCount),
		shardCount:    shardCount,
		lifeWindowSec: lifeWindowSec,
	}

	for i := range m.shards {
		m.shards[i] = &shard{items: make([]*entity, 0)}
	}
	if m.lifeWindowSec > 0 {
		m.startEvicted()
	}
	return m
}

func (m *StringBank) getShard(key string) *shard {
	return m.shards[hash(key)&(m.shardCount-1)]
}

func (m *StringBank) Add(str string) {
	m.getShard(str).add(str)
}

func (m *StringBank) Delete(str string) {
	m.getShard(str).remove(str)
}

func (m *StringBank) Get(str string) (string, bool) {
	return m.getShard(str).get(str)
}

func (m *StringBank) startEvicted() {
	atomic.StoreUint32(&nowSecond, uint32(time.Now().Unix()))
	go func() {
		for {
			now := uint32(time.Now().Unix())
			atomic.StoreUint32(&nowSecond, now)
			time.Sleep(time.Second)
			if now%m.lifeWindowSec == 0 {
				go m.evicted()
			}
		}
	}()

}

func (m *StringBank) evicted() {
	for _, s := range m.shards {
		s.evictedBefore(currentTimeSecond() - m.lifeWindowSec)
	}
}

func (s *shard) add(str string) {
	s.Lock()
	defer s.Unlock()

	index, ok := SearchSeat(len(s.items), func(i int) int {
		return strings.Compare(s.items[i].value, str)
	})

	if ok {
		return
	}
	entry := &entity{value: str, latestUsed: currentTimeSecond()}
	if index >= len(s.items) {
		s.items = append(s.items, entry)
	} else {
		s.items = append(s.items[:index+1], s.items[index:]...)
		s.items[index] = entry
	}
}

func (s *shard) get(str string) (string, bool) {
	s.RLock()
	defer s.RUnlock()

	index := Search(len(s.items), func(i int) int {
		return strings.Compare(s.items[i].value, str)
	})

	if index == -1 {
		return "", false
	}
	s.items[index].latestUsed = currentTimeSecond()
	return s.items[index].value, true
}

func (s *shard) remove(str string) {
	s.Lock()
	defer s.Unlock()

	index := Search(len(s.items), func(i int) int {
		return strings.Compare(s.items[i].value, str)
	})

	if index != -1 {
		s.items = append(s.items[:index], s.items[index+1:]...)
	}
}

func (s *shard) evictedBefore(deadline uint32) {
	s.Lock()
	defer s.Unlock()

	items := s.items
	for i, el := 0, len(items); i < el; i++ {
		j := i - (el - len(items))
		item := items[j]
		if item.latestUsed < deadline {
			items = append(items[:j], items[j+1:]...)
		}
	}
	if len(items)*3/2 < cap(items) {
		dest := make([]*entity, len(items))
		copy(dest, items)
		items = dest
	}

	s.items = items
}

func currentTimeSecond() uint32 {
	return atomic.LoadUint32(&nowSecond)
}

func hash(key string) uint32 {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	keyLength := len(key)
	for i := 0; i < keyLength; i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}
