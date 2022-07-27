// Copyright 2017 The Prometheus Authors
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

// Package tsdb implements a time series storage for float64 sample data.
package tsdb

import (
	"context"
	"fmt"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/prometheus/prometheus/tsdb/fileutil"
	_ "github.com/prometheus/prometheus/tsdb/goversion"
	"github.com/prometheus/prometheus/tsdb/wal"
	"golang.org/x/sync/errgroup"
)

// Default duration of a block in milliseconds - 2h.
const (
	DefaultBlockDuration = int64(2 * 60 * 60 * 1000)
)

var (
	HeadMaxNumSeries = math.MaxInt64
)

// DefaultOptions used for the DB. They are sane for setups using
// millisecond precision timestamps.
var DefaultOptions = &Options{
	WALSegmentSize:         wal.DefaultSegmentSize,
	RetentionDuration:      15 * 24 * 60 * 60 * 1000, // 15 days in milliseconds
	BlockRanges:            ExponentialBlockRanges(DefaultBlockDuration, 3, 5),
	NoLockfile:             false,
	AllowOverlappingBlocks: false,
	WALCompression:         false,
	StripeSize:             DefaultStripeSize,
	WarmUpDBOnOpening:      true,
	WaterMarkCompact:       DefaultWaterMark,
}

// Options of the DB storage.
type Options struct {
	// Segments (wal files) max size.
	// WALSegmentSize = 0, segment size is default size.
	// WALSegmentSize > 0, segment size is WALSegmentSize.
	// WALSegmentSize < 0, wal is disabled.
	WALSegmentSize int

	// Duration of persisted data to keep.
	RetentionDuration uint64

	MigrateDuration uint64
	// Maximum number of bytes in blocks to be retained.
	// 0 or less means disabled.
	// NOTE: For proper storage calculations need to consider
	// the size of the WAL folder which is not added when calculating
	// the current size of the database.
	MaxBytes int64

	// The sizes of the Blocks.
	BlockRanges []int64

	// NoLockfile disables creation and consideration of a lock file.
	NoLockfile bool

	// Overlapping blocks are allowed if AllowOverlappingBlocks is true.
	// This in-turn enables vertical compaction and vertical query merge.
	AllowOverlappingBlocks bool

	// WALCompression will turn on Snappy compression for records on the WAL.
	WALCompression bool

	// StripeSize is the size in entries of the series hash map. Reducing the size will save memory but impact perfomance.
	StripeSize int

	WarmUpDBOnOpening  bool
	ForceFlushResident bool
	WaterMarkCompact   WaterMark
	DeleteIgnores      []DeleteIgnore
}

type DeleteIgnore struct {
	Start DateExpr
	End   DateExpr
}

// Returns true if the chunk overlaps [mint, maxt].
func (r *DeleteIgnore) OverlapsClosedInterval(mint, maxt int64) bool {
	minTime := time.Unix(mint/1000, (mint%1000)*int64(time.Millisecond))
	maxTime := time.Unix(maxt/1000, (maxt%1000)*int64(time.Millisecond))
	return r.Start.Before(maxTime) && r.End.After(minTime)
}

type DateExpr struct {
	expr  string
	year  int
	month int
	day   int
}

func NewDateExpr(expr string) DateExpr {
	s := strings.Split(expr, "-")
	if len(s) != 3 {
		panic("date expr formatting error")
	}
	d := DateExpr{expr: expr}

	//parse the year
	if s[0] == "*" {
		d.year = math.MinInt32
	} else if year, err := strconv.ParseInt(s[0], 10, 0); err == nil {
		d.year = int(year)
	} else {
		panic("date expr formatting error")
	}

	//parse the month
	if s[1] == "*" {
		d.month = math.MinInt32
	} else if month, err := strconv.ParseInt(s[1], 10, 0); err == nil {
		d.month = int(month)
	} else {
		panic("date expr formatting error")
	}
	//parse the day
	if s[2] == "*" {
		d.day = math.MinInt32
	} else if day, err := strconv.ParseInt(s[2], 10, 0); err == nil {
		d.day = int(day)
	} else {
		panic("date expr formatting error")
	}

	return d
}

func (e DateExpr) Before(t time.Time) bool {
	if e.year != math.MinInt32 && e.year != t.Year() {
		return e.year < t.Year()
	}
	if e.month != math.MinInt32 && e.month != int(t.Month()) {
		return e.month < int(t.Month())
	}

	if e.day != math.MinInt32 && e.day != t.Day() {
		return e.day < t.Day()
	}

	return true
}

func (e DateExpr) After(t time.Time) bool {
	if e.year != math.MinInt32 && e.year != t.Year() {
		return e.year > t.Year()
	}
	if e.month != math.MinInt32 && e.month != int(t.Month()) {
		return e.month > int(t.Month())
	}

	if e.day != math.MinInt32 && e.day != t.Day() {
		return e.day > t.Day()
	}

	return true
}

// Appender allows appending a batch of data. It must be completed with a
// call to Commit or Rollback and must not be reused afterwards.
//
// Operations on the Appender interface are not goroutine-safe.
type Appender interface {
	// Add adds a sample pair for the given series. A reference number is
	// returned which can be used to add further samples in the same or later
	// transactions.
	// Returned reference numbers are ephemeral and may be rejected in calls
	// to AddFast() at any point. Adding the sample via Add() returns a new
	// reference number.
	// If the reference is 0 it must not be used for caching.
	Add(l labels.Labels, t int64, v float64) (uint64, error)

	// AddFast adds a sample pair for the referenced series. It is generally
	// faster than adding a sample by providing its full label set.
	AddFast(ref uint64, t int64, v float64) error

	// Commit submits the collected samples and purges the batch.
	Commit() error

	// Rollback rolls back all modifications made in the appender so far.
	Rollback() error
}

// DB handles reads and writes of time series falling into
// a hashed partition of a seriedb.
type DB struct {
	dir   string
	name  string
	lockf fileutil.Releaser

	logger    log.Logger
	metrics   *dbMetrics
	opts      *Options
	chunkPool chunkenc.Pool
	compactor Compactor

	// Mutex for that must be held when modifying the general block layout.
	mtx    sync.RWMutex
	blocks []*Block

	head *Head

	compactc chan struct{}
	donec    chan struct{}
	stopc    chan struct{}

	// cmtx ensures that compactions and deletions don't run simultaneously.
	cmtx sync.Mutex

	// autoCompactMtx ensures that no compaction gets triggered while
	// changing the autoCompact var.
	autoCompactMtx sync.Mutex
	autoCompact    bool
	headCompactMtx sync.Mutex
	// Cancel a running compaction when a shutdown is initiated.
	compactCancel     context.CancelFunc
	migrate           func(uid ulid.ULID)
	latestCompactTime time.Time
}

type dbMetrics struct {
	loadedBlocks          prometheus.GaugeFunc
	symbolTableSize       prometheus.GaugeFunc
	reloads               prometheus.Counter
	reloadsFailed         prometheus.Counter
	compactionsFailed     prometheus.Counter
	compactionsTriggered  prometheus.Counter
	compactionsSkipped    prometheus.Counter
	compactBlockTriggered prometheus.Counter
	compactBlockFailed    prometheus.Counter
	sizeRetentionCount    prometheus.Counter
	timeRetentionCount    prometheus.Counter
	startTime             prometheus.GaugeFunc
	tombCleanTimer        prometheus.Histogram
	blocksBytes           prometheus.Gauge
	maxBytes              prometheus.Gauge
}

func newDBMetrics(db *DB, r prometheus.Registerer) *dbMetrics {
	m := &dbMetrics{}
	constLabels := prometheus.Labels{
		"db": db.name,
	}

	m.loadedBlocks = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name:        "prometheus_tsdb_blocks_loaded",
		Help:        "Number of currently loaded data blocks",
		ConstLabels: constLabels,
	}, func() float64 {
		db.mtx.RLock()
		defer db.mtx.RUnlock()
		return float64(len(db.blocks))
	})
	m.symbolTableSize = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name:        "prometheus_tsdb_symbol_table_size_bytes",
		Help:        "Size of symbol table on disk (in bytes)",
		ConstLabels: constLabels,
	}, func() float64 {
		db.mtx.RLock()
		blocks := db.blocks[:]
		db.mtx.RUnlock()
		symTblSize := uint64(0)
		for _, b := range blocks {
			symTblSize += b.GetSymbolTableSize()
		}
		return float64(symTblSize)
	})
	m.reloads = prometheus.NewCounter(prometheus.CounterOpts{
		Name:        "prometheus_tsdb_reloads_total",
		Help:        "Number of times the database reloaded block data from disk.",
		ConstLabels: constLabels,
	})
	m.reloadsFailed = prometheus.NewCounter(prometheus.CounterOpts{
		Name:        "prometheus_tsdb_reloads_failures_total",
		Help:        "Number of times the database failed to Reload block data from disk.",
		ConstLabels: constLabels,
	})
	m.compactionsTriggered = prometheus.NewCounter(prometheus.CounterOpts{
		Name:        "prometheus_tsdb_compactions_triggered_total",
		Help:        "Total number of triggered compactions for the partition.",
		ConstLabels: constLabels,
	})
	m.compactionsFailed = prometheus.NewCounter(prometheus.CounterOpts{
		Name:        "prometheus_tsdb_compactions_failed_total",
		Help:        "Total number of compactions that failed for the partition.",
		ConstLabels: constLabels,
	})
	m.compactionsSkipped = prometheus.NewCounter(prometheus.CounterOpts{
		Name:        "prometheus_tsdb_compactions_skipped_total",
		Help:        "Total number of skipped compactions due to disabled auto compaction.",
		ConstLabels: constLabels,
	})

	m.compactBlockTriggered = prometheus.NewCounter(prometheus.CounterOpts{
		Name:        "prometheus_tsdb_compact_block_triggered_total",
		Help:        "number of triggered compact block",
		ConstLabels: constLabels,
	})
	m.compactBlockFailed = prometheus.NewCounter(prometheus.CounterOpts{
		Name:        "prometheus_tsdb_compact_block_failed_total",
		Help:        "number of compact block",
		ConstLabels: constLabels,
	})
	m.timeRetentionCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name:        "prometheus_tsdb_time_retentions_total",
		Help:        "The number of times that blocks were deleted because the maximum time limit was exceeded.",
		ConstLabels: constLabels,
	})

	m.startTime = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name:        "prometheus_tsdb_lowest_timestamp",
		Help:        "Lowest timestamp value stored in the database. The unit is decided by the library consumer.",
		ConstLabels: constLabels,
	}, func() float64 {
		db.mtx.RLock()
		defer db.mtx.RUnlock()
		if len(db.blocks) == 0 {
			return float64(db.head.MinTime())
		}
		return float64(db.blocks[0].meta.MinTime)
	})
	m.tombCleanTimer = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:        "prometheus_tsdb_tombstone_cleanup_seconds",
		Help:        "The time taken to recompact blocks to remove tombstones.",
		ConstLabels: constLabels,
	})
	m.blocksBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Name:        "prometheus_tsdb_storage_blocks_bytes",
		Help:        "The number of bytes that are currently used for local storage by all blocks.",
		ConstLabels: constLabels,
	})
	m.maxBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Name:        "prometheus_tsdb_retention_limit_bytes",
		Help:        "Max number of bytes to be retained in the tsdb blocks, configured 0 means disabled",
		ConstLabels: constLabels,
	})
	m.sizeRetentionCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name:        "prometheus_tsdb_size_retentions_total",
		Help:        "The number of times that blocks were deleted because the maximum number of bytes was exceeded.",
		ConstLabels: constLabels,
	})

	if r != nil {
		r.MustRegister(
			m.loadedBlocks,
			m.symbolTableSize,
			m.reloads,
			m.reloadsFailed,
			m.compactionsFailed,
			m.compactionsTriggered,
			m.compactionsSkipped,
			m.compactBlockTriggered,
			m.compactBlockFailed,
			m.sizeRetentionCount,
			m.timeRetentionCount,
			m.startTime,
			m.tombCleanTimer,
			m.blocksBytes,
			m.maxBytes,
		)
	}
	return m
}

// ErrClosed is returned when the db is closed.
var ErrClosed = errors.New("db already closed")

// DBReadOnly provides APIs for read only operations on a database.
// Current implementation doesn't support concurrency so
// all API calls should happen in the same go routine.
type DBReadOnly struct {
	logger  log.Logger
	dir     string
	closers []io.Closer
	closed  chan struct{}
}

// OpenDBReadOnly opens DB in the given directory for read only operations.
func OpenDBReadOnly(dir string, l log.Logger) (*DBReadOnly, error) {
	if _, err := os.Stat(dir); err != nil {
		return nil, errors.Wrap(err, "opening the db dir")
	}

	if l == nil {
		l = log.NewNopLogger()
	}

	return &DBReadOnly{
		logger: l,
		dir:    dir,
		closed: make(chan struct{}),
	}, nil
}

// FlushWAL creates a new block containing all data that's currently in the memory buffer/WAL.
// Samples that are in existing blocks will not be written to the new block.
// Note that if the read only database is running concurrently with a
// writable database then writing the WAL to the database directory can race.
func (db *DBReadOnly) FlushWAL(dir string) error {
	blockReaders, err := db.Blocks()
	if err != nil {
		return errors.Wrap(err, "read blocks")
	}
	maxBlockTime := int64(math.MinInt64)
	if len(blockReaders) > 0 {
		maxBlockTime = blockReaders[len(blockReaders)-1].Meta().MaxTime
	}
	w, err := wal.Open(db.logger, nil, filepath.Join(db.dir, "wal"))
	if err != nil {
		return err
	}
	head, err := NewHead("default", nil, db.logger, w, 1, DefaultStripeSize, DefaultWaterMark)
	if err != nil {
		return err
	}
	// Set the min valid time for the ingested wal samples
	// to be no lower than the maxt of the last block.
	if err := head.Init(maxBlockTime); err != nil {
		return errors.Wrap(err, "read WAL")
	}
	mint := head.MinTime()
	maxt := head.MaxTime()
	rh := &rangeHead{
		head: head,
		mint: mint,
		maxt: maxt,
	}
	compactor, err := NewLeveledCompactor(context.Background(), nil, db.logger, DefaultOptions.BlockRanges, nil, chunkenc.NewPool())
	if err != nil {
		return errors.Wrap(err, "create leveled compactor")
	}
	// Add +1 millisecond to block maxt because block intervals are half-open: [b.MinTime, b.MaxTime).
	// Because of this block intervals are always +1 than the total samples it includes.
	_, err = compactor.Write(dir, rh, mint, maxt+1, nil)
	return errors.Wrap(err, "writing WAL")
}

// Querier loads the wal and returns a new querier over the data partition for the given time range.
// Current implementation doesn't support multiple Queriers.
func (db *DBReadOnly) Querier(mint, maxt int64) (Querier, error) {
	select {
	case <-db.closed:
		return nil, ErrClosed
	default:
	}
	blockReaders, err := db.Blocks()
	if err != nil {
		return nil, err
	}
	blocks := make([]*Block, len(blockReaders))
	for i, b := range blockReaders {
		b, ok := b.(*Block)
		if !ok {
			return nil, errors.New("unable to convert a read only block to a normal block")
		}
		blocks[i] = b
	}

	head, err := NewHead("default", nil, db.logger, nil, 1, DefaultStripeSize, DefaultWaterMark)
	if err != nil {
		return nil, err
	}
	maxBlockTime := int64(math.MinInt64)
	if len(blocks) > 0 {
		maxBlockTime = blocks[len(blocks)-1].Meta().MaxTime
	}

	// Also add the WAL if the current blocks don't cover the requests time range.
	if maxBlockTime <= maxt {
		w, err := wal.Open(db.logger, nil, filepath.Join(db.dir, "wal"))
		if err != nil {
			return nil, err
		}
		head, err = NewHead("default", nil, db.logger, w, 1, DefaultStripeSize, DefaultWaterMark)
		if err != nil {
			return nil, err
		}
		// Set the min valid time for the ingested wal samples
		// to be no lower than the maxt of the last block.
		if err := head.Init(maxBlockTime); err != nil {
			return nil, errors.Wrap(err, "read WAL")
		}
		// Set the wal to nil to disable all wal operations.
		// This is mainly to avoid blocking when closing the head.
		head.wal = nil

		db.closers = append(db.closers, head)
	}

	// TODO: Refactor so that it is possible to obtain a Querier without initializing a writable DB instance.
	// Option 1: refactor DB to have the Querier implementation using the DBReadOnly.Querier implementation not the opposite.
	// Option 2: refactor Querier to use another independent func which
	// can than be used by a read only and writable db instances without any code duplication.
	dbWritable := &DB{
		dir:    db.dir,
		logger: db.logger,
		blocks: blocks,
		head:   head,
	}

	return dbWritable.Querier(mint, maxt)
}

// Blocks returns a slice of block readers for persisted blocks.
func (db *DBReadOnly) Blocks() ([]BlockReader, error) {
	select {
	case <-db.closed:
		return nil, ErrClosed
	default:
	}
	loadable, corrupted, err := openBlocks(db.logger, db.dir, nil, nil)
	if err != nil {
		return nil, err
	}

	// Corrupted blocks that have been superseded by a loadable block can be safely ignored.
	for _, block := range loadable {
		for _, b := range block.Meta().Compaction.Parents {
			delete(corrupted, b.ULID)
		}
	}
	if len(corrupted) > 0 {
		for _, b := range loadable {
			if err := b.Close(); err != nil {
				level.Warn(db.logger).Log("msg", "closing a block", err)
			}
		}
		return nil, errors.Errorf("unexpected corrupted block:%v", corrupted)
	}

	if len(loadable) == 0 {
		return nil, nil
	}

	sort.Slice(loadable, func(i, j int) bool {
		return loadable[i].Meta().MinTime < loadable[j].Meta().MinTime
	})

	blockMetas := make([]BlockMeta, 0, len(loadable))
	for _, b := range loadable {
		blockMetas = append(blockMetas, b.Meta())
	}
	if overlaps := OverlappingBlocks(blockMetas); len(overlaps) > 0 {
		level.Warn(db.logger).Log("msg", "overlapping blocks found during opening", "detail", overlaps.String())
	}

	// Close all previously open readers and add the new ones to the cache.
	for _, closer := range db.closers {
		closer.Close()
	}

	blockClosers := make([]io.Closer, len(loadable))
	blockReaders := make([]BlockReader, len(loadable))
	for i, b := range loadable {
		blockClosers[i] = b
		blockReaders[i] = b
	}
	db.closers = blockClosers

	return blockReaders, nil
}

// Close all block readers.
func (db *DBReadOnly) Close() error {
	select {
	case <-db.closed:
		return ErrClosed
	default:
	}
	close(db.closed)

	var merr tsdb_errors.MultiError

	for _, b := range db.closers {
		merr.Add(b.Close())
	}
	return merr.Err()
}

type MigratedDB struct {
	*DB
	Cold    *DB
	Status  string     //loading、recover、running
	wMutex  sync.Mutex //migrated lock
	waiting sync.Map
}

func OpenMigratedDB(rwDir string, rwOpts *Options, rDir string, rOpts *Options, l log.Logger, reg prometheus.Registerer) (mixedDB *MigratedDB, err error) {
	mixedDB = &MigratedDB{}
	hot, err := OpenDB("hot", rwDir, l, reg, rwOpts)
	if err != nil {
		return nil, err
	}
	mixedDB.DB = hot

	if rDir != "" {
		cold, err := OpenDB("cold", rDir, l, reg, rOpts)
		if err != nil {
			return nil, err
		}
		cold.autoCompact = true
		mixedDB.Cold = cold
		hot.migrate = mixedDB.notifyMigrate
	}

	return mixedDB, nil
}

func (db *MigratedDB) Close() error {
	if db.Cold == nil {
		return db.DB.Close()
	} else {
		var merr tsdb_errors.MultiError
		merr.Add(db.DB.Close())
		merr.Add(db.Cold.Close())
		return merr.Err()
	}
}

func (db *MigratedDB) UnitiveBlocks() []*Block {
	if db.Cold == nil {
		return db.DB.blocks
	}

	var combines []*Block

	db.Cold.mtx.RLock()
	combines = append(combines, db.Cold.blocks...)
	db.Cold.mtx.RUnlock()

	db.DB.mtx.RLock()
	combines = append(combines, db.DB.blocks...)
	db.DB.mtx.RUnlock()

	return combines
}

func (db *MigratedDB) Querier(mint, maxt int64) (Querier, error) {
	maxBlockTime := int64(math.MinInt64)
	if db.Cold != nil && len(db.Cold.blocks) > 0 {
		maxBlockTime = db.Cold.blocks[len(db.Cold.blocks)-1].Meta().MaxTime
	}
	if db.Cold == nil || mint > maxBlockTime {
		return db.DB.Querier(mint, maxt)
	}

	var blocks []BlockReader
	var blockMetas []BlockMeta
	var combines []*Block
	db.mtx.RLock()
	db.Cold.mtx.RLock()
	defer db.mtx.RUnlock()
	defer db.Cold.mtx.RUnlock()

	combines = append(combines, db.Cold.blocks...)
	combines = append(combines, db.DB.blocks...)
	sort.Slice(combines, func(i, j int) bool {
		return combines[i].Meta().MinTime < combines[j].Meta().MinTime
	})

	for _, b := range combines {
		if b.OverlapsClosedInterval(mint, maxt) {
			blocks = append(blocks, b)
			blockMetas = append(blockMetas, b.Meta())
		}
	}
	if maxt >= db.head.MinTime() {
		blocks = append(blocks, &rangeHead{
			head: db.head,
			mint: mint,
			maxt: maxt,
		})
	}

	blockQueriers := make([]Querier, 0, len(blocks))
	for _, b := range blocks {
		q, err := NewBlockQuerier(b, mint, maxt)
		if err == nil {
			blockQueriers = append(blockQueriers, q)
			continue
		}
		for _, q := range blockQueriers {
			q.Close()
		}
		return nil, errors.Wrapf(err, "open querier for block %s", b)
	}

	if len(OverlappingBlocks(blockMetas)) > 0 {
		return &verticalQuerier{
			querier: querier{
				blocks: blockQueriers,
			},
		}, nil
	}

	return &querier{
		blocks: blockQueriers,
	}, nil
}

func (db *MigratedDB) notifyMigrate(uid ulid.ULID) {
	db.waiting.Store(uid, struct{}{})
	go func() {
		db.wMutex.Lock()
		defer db.wMutex.Unlock()

		db.waiting.Range(func(key, value interface{}) bool {
			db.migratingBlock(key.(ulid.ULID))
			return true
		})
	}()
}

func (db *MigratedDB) migratingBlock(uid ulid.ULID) (err error) {
	defer db.waiting.Delete(uid)
	if !db.exists(uid) {
		return
	}
	level.Info(db.logger).Log("msg", "migrate starting", "uid", uid)
	defer func(t time.Time) {
		if db.Cold.exists(uid) {
			db.DB.DeleteBlock(uid)
		}
		if err != nil {
			level.Error(db.logger).Log("msg", "migrate failed", "uid", uid, "costTime", time.Since(t).String(), "err", err)
		} else {
			level.Info(db.logger).Log("msg", "migrate finish", "uid", uid, "costTime", time.Since(t).String())
		}
	}(time.Now())

	if err = fileutil.CopyDirsSafely(filepath.Join(db.dir, uid.String()), filepath.Join(db.Cold.dir, uid.String())); err != nil {
		err = errors.Wrapf(err, "copy dir failed")
		return
	}
	err = db.Cold.Reload()

	return
}

func (db *MigratedDB) CombinesBlocks() []*Block {
	if db.Cold == nil {
		return db.DB.Blocks()
	}
	var combines []*Block
	cBlock := db.Cold.Blocks()
	hBlock := db.DB.Blocks()
	if len(cBlock) != 0 {
		combines = append(combines, cBlock...)
	}
	if len(hBlock) != 0 {
		combines = append(combines, hBlock...)
	}

	sort.Slice(combines, func(i, j int) bool {
		return combines[i].Meta().MinTime < combines[j].Meta().MinTime
	})

	return combines
}

// Open returns a new DB in the given directory.
func Open(dir string, l log.Logger, reg prometheus.Registerer, opts *Options) (db *DB, err error) {
	return OpenDB("", dir, l, reg, opts)
}
func OpenDB(name, dir string, l log.Logger, reg prometheus.Registerer, opts *Options) (db *DB, err error) {
	var dispensableReg prometheus.Registerer
	if name == "default" || name == "hot" {
		dispensableReg = reg
	}

	if err := os.MkdirAll(dir, 0777); err != nil {
		return nil, err
	}
	if l == nil {
		l = log.NewNopLogger()
	}
	if opts == nil {
		opts = DefaultOptions
	}
	if opts.StripeSize <= 0 {
		opts.StripeSize = DefaultStripeSize
	}
	// Fixup bad format written by Prometheus 2.1.
	if err := repairBadIndexVersion(l, dir); err != nil {
		return nil, err
	}
	// Migrate old WAL if one exists.
	if err := MigrateWAL(l, filepath.Join(dir, "wal")); err != nil {
		return nil, errors.Wrap(err, "migrate WAL")
	}

	db = &DB{
		name:              name,
		dir:               dir,
		logger:            l,
		opts:              opts,
		compactc:          make(chan struct{}, 1),
		donec:             make(chan struct{}),
		stopc:             make(chan struct{}),
		autoCompact:       true,
		chunkPool:         chunkenc.NewPool(),
		latestCompactTime: time.Now(),
	}
	db.metrics = newDBMetrics(db, reg)

	maxBytes := opts.MaxBytes
	if maxBytes < 0 {
		maxBytes = 0
	}
	db.metrics.maxBytes.Set(float64(maxBytes))

	if !opts.NoLockfile {
		absdir, err := filepath.Abs(dir)
		if err != nil {
			return nil, err
		}
		lockf, _, err := fileutil.Flock(filepath.Join(absdir, "lock"))
		if err != nil {
			return nil, errors.Wrap(err, "lock DB directory")
		}
		db.lockf = lockf
	}

	for i := 0; i < len(opts.BlockRanges); i++ {
		opts.BlockRanges[i] = rangeForStart(opts.BlockRanges[i], chunkenc.ChunkWindowMs)
	}

	ctx, cancel := context.WithCancel(context.Background())
	db.compactor, err = NewLeveledCompactor(ctx, dispensableReg, l, opts.BlockRanges, opts.DeleteIgnores, db.chunkPool)
	if err != nil {
		cancel()
		return nil, errors.Wrap(err, "create leveled compactor")
	}
	db.compactCancel = cancel

	var wlog *wal.WAL
	segmentSize := wal.DefaultSegmentSize
	// Wal is enabled.
	if opts.WALSegmentSize >= 0 {
		// Wal is set to a custom size.
		if opts.WALSegmentSize > 0 {
			segmentSize = opts.WALSegmentSize
		}
		wlog, err = wal.NewSize(l, dispensableReg, filepath.Join(dir, "wal"), segmentSize, opts.WALCompression)
		if err != nil {
			return nil, err
		}
	}

	if opts.WaterMarkCompact.High <= 0 || opts.WaterMarkCompact.Low >= opts.WaterMarkCompact.High {
		return nil, errors.Errorf("can't compact under the watermark, low: %d, high: %d",
			opts.WaterMarkCompact.Low, opts.WaterMarkCompact.High)
	}

	db.head, err = NewHead(name, reg, l, wlog, opts.BlockRanges[0], opts.StripeSize, opts.WaterMarkCompact)
	if err != nil {
		return nil, err
	}

	if err := db.Reload(); err != nil {
		return nil, err
	}
	// Set the min valid time for the ingested samples
	// to be no lower than the maxt of the last block.
	blocks := db.Blocks()
	minValidTime := int64(math.MinInt64)
	if len(blocks) > 0 {
		minValidTime = blocks[len(blocks)-1].Meta().MaxTime - db.head.maxOffsetWindow
	}

	if initErr := db.head.Init(minValidTime); initErr != nil {
		db.head.metrics.walCorruptionsTotal.Inc()
		level.Warn(db.logger).Log("msg", "encountered WAL read error, attempting repair", "err", initErr)
		if err := wlog.Repair(initErr); err != nil {
			return nil, errors.Wrap(err, "repair corrupted WAL")
		}
	}
	if opts.WarmUpDBOnOpening {
		db.warmUpDataBase()
	}

	go db.run()
	return db, nil
}

func (db *DB) warmUpDataBase() error {
	if db.name != "hot" || len(db.blocks) == 0 {
		return nil
	}
	level.Info(db.logger).Log("msg", "starting warm up database……")
	b := db.blocks[len(db.blocks)-1]
	ir, err := b.Index()
	if err != nil {
		return err
	}
	defer ir.Close()

	p, err := ir.Postings("", "") // The special all key.
	if err != nil {
		return err
	}
	lbls := labels.Labels{}
	chks := []chunks.Meta{}
	for p.Next() {
		if err = ir.Series(p.At(), &lbls, &chks); err != nil {
			return err
		}
		lset := make(labels.Labels, lbls.Len())
		copy(lset, lbls)

		db.head.getOrCreate(lset.Hash(), lset)
	}

	return nil
}

// Dir returns the directory of the database.
func (db *DB) Dir() string {
	return db.dir
}

func (db *DB) run() {
	defer close(db.donec)

	go db.compactBlockTrigger()

	backoff := time.Duration(0)
	for {
		select {
		case <-db.stopc:
			return
		case <-time.After(backoff):
		}

		select {
		case <-time.After(1 * time.Minute):
			select {
			case db.compactc <- struct{}{}:
			default:
			}
		case <-db.compactc:
			db.metrics.compactionsTriggered.Inc()

			db.autoCompactMtx.Lock()
			if db.autoCompact {
				forceFlush := false
				if db.opts.ForceFlushResident {
					forceFlush = time.Now().After(db.latestCompactTime.Add(time.Duration(db.head.chunkRange*4) * time.Millisecond))
				}
				if err := db.CompactHead(forceFlush); err != nil {
					level.Error(db.logger).Log("msg", "compaction failed", "err", err)

					db.metrics.compactionsFailed.Inc()
					backoff = exponential(backoff, 1*time.Second, 1*time.Minute)
				} else {
					backoff = 0
				}
			} else {
				db.metrics.compactionsSkipped.Inc()
			}
			db.autoCompactMtx.Unlock()
		case <-db.stopc:
			return
		}
	}
}

func (db *DB) compactBlockTrigger() {
	for {
		select {
		case <-time.After(1 * time.Minute):
			db.metrics.compactBlockTriggered.Inc()

			if err := db.compactBlocks(); err != nil {
				db.metrics.compactBlockFailed.Inc()

				level.Error(db.logger).Log("msg", "compactBlocks failed", "err", err)
			}
		case <-db.stopc:
			return
		}
	}
}

// Appender opens a new appender against the database.
func (db *DB) Appender() Appender {
	return dbAppender{db: db, Appender: db.head.Appender()}
}

func (db *DB) exists(uid ulid.ULID) bool {
	for _, block := range db.blocks {
		if block.meta.ULID.Compare(uid) == 0 {
			return true
		}
	}
	return false
}

func (db *DB) DeleteBlock(uid ulid.ULID) error {
	blocks := make(map[ulid.ULID]*Block, 0)
	db.mtx.Lock()
	for i, block := range db.blocks {
		if block.meta.ULID.Compare(uid) == 0 {
			blocks[uid] = block
			db.blocks = append(db.blocks[:i], db.blocks[i+1:]...)
			break
		}
	}
	db.mtx.Unlock()

	return db.deleteBlocks(blocks)
}

func (db *DB) UpdateBlockMeta(uid ulid.ULID, ignore bool) error {
	db.mtx.Lock()
	for _, block := range db.blocks {
		if block.meta.ULID.Compare(uid) == 0 {
			block.meta.DeleteIgnore = ignore
			if _, err := writeMetaFile(block.logger, block.dir, &block.meta); err != nil {
				return err
			}
			break
		}
	}
	db.mtx.Unlock()
	return nil
}

// dbAppender wraps the DB's head appender and triggers compactions on commit
// if necessary.
type dbAppender struct {
	Appender
	db *DB
}

func (a dbAppender) Commit() error {
	err := a.Appender.Commit()

	// We could just run this check every few minutes practically. But for benchmarks
	// and high frequency use cases this is the safer way.
	if a.db.head.compactable() {
		select {
		case a.db.compactc <- struct{}{}:
		default:
		}
	}
	return err
}

// CompactHead data if possible. After successful compaction blocks are reloaded
// which will also trigger blocks to be deleted that fall out of the retention
// window.
// If no blocks are compacted, the retention window state doesn't change. Thus,
// this is sufficient to reliably delete old data.
// Old blocks are only deleted on Reload based on the new block's parent information.
// See DB.Reload documentation for further information.
func (db *DB) CompactHead(forceFlush bool) (err error) {
	db.headCompactMtx.Lock()
	defer db.headCompactMtx.Unlock()
	defer db.recover(&err)

	var (
		mint, maxt int64
	)
	// Check whether we have pending head blocks that are ready to be persisted.
	// They have the highest priority.
	for i := 0; db.head.compactable() || forceFlush; i++ {
		db.latestCompactTime = time.Now()
		select {
		case <-db.stopc:
			return nil
		default:
		}

		if db.head.minTime > db.head.maxTime {
			level.Info(db.logger).Log("msg", "no data exist in memory")
			break
		}

		if i == 0 {
			mint = db.head.splitMinTime()
		} else {
			mint = maxt
		}
		maxt = rangeForTimestamp(mint, db.head.chunkRange)
		//maybe middle chunks is all empty
		//maybe tail chunks can compact
		//flush blocks at the most
		if maxt < db.head.MinTime() || mint > db.head.MaxTime() || (i != 0 && i >= int(db.head.maxOffsetWindow/db.head.chunkRange)) {
			level.Info(db.logger).Log("maxt", maxt, "mint", mint, "headMin", db.head.MinTime(), "headMax", db.head.MaxTime(), "i", i)
			break
		}
		// Wrap head into a range that bounds all reads to it.
		head := &compactRangeHead{rangeHead: &rangeHead{
			head: db.head,
			mint: mint,
			// We remove 1 millisecond from maxt because block
			// intervals are half-open: [b.MinTime, b.MaxTime). But
			// chunk intervals are closed: [c.MinTime, c.MaxTime];
			// so in order to make sure that overlaps are evaluated
			// consistently, we explicitly remove the last value
			// from the block interval here.
			maxt: maxt - 1,
		}}

		uid, flushErr := db.compactor.Write(db.dir, head, mint, maxt, nil)
		reloadErr := db.Reload()

		if i == 0 {
			db.head.mergeMinTime(flushErr)
		}

		if flushErr != nil {
			return errors.Wrap(flushErr, "persist head block")
		}
		if reloadErr != nil {
			if err := os.RemoveAll(filepath.Join(db.dir, uid.String())); err != nil {
				return errors.Wrapf(err, "delete persisted head block after failed db Reload:%s", uid)
			}
			return errors.Wrap(reloadErr, "Reload blocks")
		}
		// Compaction resulted in an empty block.
		// Head truncating during db.Reload() depends on the persisted blocks and
		// in this case no new block will be persisted so manually truncate the head.
		db.head.gc(maxt, false, false)
		runtime.GC()
		if uid.Compare(ulid.ULID{}) == 0 {
			break
		}
	}

	return nil
}

// compactBlocks compacts all the eligible on-disk blocks.
// The compaction mutex should be held before calling this method.
func (db *DB) compactBlocks() (err error) {
	db.cmtx.Lock()
	defer db.cmtx.Unlock()
	defer db.recover(&err)

	// Check for compactions of multiple blocks.
	for {
		plan, err := db.compactor.Plan(db.dir)
		if err != nil {
			return errors.Wrap(err, "plan compaction")
		}
		if len(plan) == 0 {
			break
		}

		select {
		case <-db.stopc:
			return nil
		default:
		}

		uid, err := db.compactor.Compact(db.dir, plan, db.blocks)
		if err != nil {
			return errors.Wrapf(err, "compact %s", plan)
		}
		runtime.GC()

		if err := db.Reload(); err != nil {
			if err := os.RemoveAll(filepath.Join(db.dir, uid.String())); err != nil {
				return errors.Wrapf(err, "delete compacted block after failed db Reload:%s", uid)
			}
			return errors.Wrap(err, "Reload blocks")
		}
	}
	return nil
}

// getBlock iterates a given block range to find a block by a given id.
// If found it returns the block itself and a boolean to indicate that it was found.
func getBlock(allBlocks []*Block, id ulid.ULID) (*Block, bool) {
	for _, b := range allBlocks {
		if b.Meta().ULID == id {
			return b, true
		}
	}
	return nil, false
}

func (db *DB) recover(errp *error) {
	e := recover()
	if e == nil {
		return
	}
	if err, ok := e.(runtime.Error); ok {
		// Print the stack trace but do not inhibit the running application.
		buf := make([]byte, 64<<10)
		buf = buf[:runtime.Stack(buf, false)]

		level.Error(db.logger).Log("msg", "runtime panic in compact", "err", e, "stacktrace", string(buf))
		*errp = fmt.Errorf("unexpected error: %s", err)
	} else {
		*errp = e.(error)
	}
}

// Reload blocks and trigger head truncation if new blocks appeared.
// Blocks that are obsolete due to replacement or retention will be deleted.
func (db *DB) Reload() (err error) {
	defer func() {
		if err != nil {
			db.metrics.reloadsFailed.Inc()
		}
		db.metrics.reloads.Inc()
	}()

	loadable, corrupted, err := openBlocks(db.logger, db.dir, db.blocks, db.chunkPool)
	if err != nil {
		return err
	}

	deletable := db.deletableBlocks(loadable)

	// Corrupted blocks that have been superseded by a loadable block can be safely ignored.
	// This makes it resilient against the process crashing towards the end of a compaction.
	// Creation of a new block and deletion of its parents cannot happen atomically.
	// By creating blocks with their parents, we can pick up the deletion where it left off during a crash.
	for _, block := range loadable {
		for _, b := range block.Meta().Compaction.Parents {
			delete(corrupted, b.ULID)
			deletable[b.ULID] = nil
		}
	}
	if len(corrupted) > 0 {
		// Close all new blocks to release the lock for windows.
		for _, block := range loadable {
			if _, open := getBlock(db.blocks, block.Meta().ULID); !open {
				block.Close()
			}
		}
		return fmt.Errorf("unexpected corrupted block:%v", corrupted)
	}

	// All deletable blocks should not be loaded.
	var (
		bb         []*Block
		blocksSize int64
	)
	for _, block := range loadable {
		if _, ok := deletable[block.Meta().ULID]; ok {
			deletable[block.Meta().ULID] = block
			continue
		}
		bb = append(bb, block)
		blocksSize += block.Size()

	}
	loadable = bb
	db.metrics.blocksBytes.Set(float64(blocksSize))

	sort.Slice(loadable, func(i, j int) bool {
		return loadable[i].Meta().MinTime < loadable[j].Meta().MinTime
	})
	if !db.opts.AllowOverlappingBlocks {
		if err := validateBlockSequence(loadable); err != nil {
			return errors.Wrap(err, "invalid block sequence")
		}
	}

	// Swap new blocks first for subsequently created readers to be seen.
	db.mtx.Lock()
	oldBlocks := db.blocks
	db.blocks = loadable
	db.mtx.Unlock()

	blockMetas := make([]BlockMeta, 0, len(loadable))
	for _, b := range loadable {
		blockMetas = append(blockMetas, b.Meta())
	}
	if overlaps := OverlappingBlocks(blockMetas); len(overlaps) > 0 {
		level.Warn(db.logger).Log("msg", "overlapping blocks found during Reload", "detail", overlaps.String())
	}

	for _, b := range oldBlocks {
		if _, ok := deletable[b.Meta().ULID]; ok {
			deletable[b.Meta().ULID] = b
		}
	}

	if err := db.deleteBlocks(deletable); err != nil {
		return err
	}

	// Garbage collect data in the head if the most recent persisted block
	// covers data of its current time range.
	if len(loadable) == 0 {
		return nil
	}

	return nil
}

func openBlocks(l log.Logger, dir string, loaded []*Block, chunkPool chunkenc.Pool) (blocks []*Block, corrupted map[ulid.ULID]error, err error) {
	bDirs, err := blockDirs(dir)
	if err != nil {
		return nil, nil, errors.Wrap(err, "find blocks")
	}
	corrupted = make(map[ulid.ULID]error)
	var wg sync.WaitGroup
	var rwMutex sync.RWMutex

	for i, blockDir := range bDirs {
		wg.Add(1)
		go func(bDir string) {
			defer wg.Done()

			meta, _, err := readMetaFile(bDir)
			if err != nil {
				level.Error(l).Log("msg", "failed to read meta.json for a block", "dir", bDir, "err", err)
				return
			}

			// See if we already have the block in memory or open it otherwise.
			block, open := getBlock(loaded, meta.ULID)
			if !open {
				block, err = OpenBlock(l, bDir, chunkPool)
				if err != nil {
					rwMutex.Lock()
					corrupted[meta.ULID] = err
					rwMutex.Unlock()
					return
				}
			}

			rwMutex.Lock()
			blocks = append(blocks, block)
			rwMutex.Unlock()
		}(blockDir)

		if i%16 == 0 || i == len(bDirs)-1 {
			wg.Wait()
		}
	}
	return blocks, corrupted, nil
}

// deletableBlocks returns all blocks past retention policy.
func (db *DB) deletableBlocks(blocks []*Block) map[ulid.ULID]*Block {
	deletable := make(map[ulid.ULID]*Block)

	// Sort the blocks by time - newest to oldest (largest to smallest timestamp).
	// This ensures that the retentions will remove the oldest  blocks.
	sort.Slice(blocks, func(i, j int) bool {
		return blocks[i].Meta().MaxTime > blocks[j].Meta().MaxTime
	})

	for _, block := range blocks {
		if block.Meta().Compaction.Deletable {
			deletable[block.Meta().ULID] = block
		}
	}

	for ulid, block := range db.beyondTimeRetention(blocks) {
		deletable[ulid] = block
	}

	for ulid, block := range db.beyondSizeRetention(blocks) {
		deletable[ulid] = block
	}

	db.beyondTimeMigrate(blocks)

	return deletable
}

func (db *DB) beyondTimeRetention(blocks []*Block) (deletable map[ulid.ULID]*Block) {
	// Time retention is disabled or no blocks to work with.
	if len(db.blocks) == 0 || db.opts.RetentionDuration == 0 {
		return
	}

	deletable = make(map[ulid.ULID]*Block)
	for i, block := range blocks {
		// The difference between the first block and this block is larger than
		// the retention period so any blocks after that are added as deletable.
		if i > 0 && blocks[0].Meta().MaxTime-block.Meta().MaxTime > int64(db.opts.RetentionDuration) {
			for _, b := range blocks[i:] {
				//ignore block retention forever
				if b.meta.DeleteIgnore {
					continue
				}
				deletable[b.meta.ULID] = b
			}
			db.metrics.timeRetentionCount.Inc()
			break
		}
	}
	return deletable
}

func (db *DB) beyondSizeRetention(blocks []*Block) (deletable map[ulid.ULID]*Block) {
	// Size retention is disabled or no blocks to work with.
	if len(db.blocks) == 0 || db.opts.MaxBytes <= 0 {
		return
	}

	deletable = make(map[ulid.ULID]*Block)

	sort.Slice(blocks, func(i, j int) bool {
		if blocks[i].Meta().DeleteIgnore == blocks[j].Meta().DeleteIgnore {
			return blocks[i].Meta().MaxTime > blocks[j].Meta().MaxTime
		} else if blocks[i].Meta().DeleteIgnore {
			return true
		} else {
			return false
		}
	})

	walSize, _ := db.Head().wal.Size()
	// Initializing size counter with WAL size,
	// as that is part of the retention strategy.
	blocksSize := walSize
	for i, block := range blocks {
		blocksSize += block.Size()
		if blocksSize > db.opts.MaxBytes {
			// Add this and all following blocks for deletion.
			for _, b := range blocks[i:] {
				if b.meta.DeleteIgnore {
					continue
				}
				deletable[b.meta.ULID] = b
			}
			db.metrics.sizeRetentionCount.Inc()
			break
		}
	}
	return deletable
}

func (db *DB) beyondTimeMigrate(blocks []*Block) {
	if len(db.blocks) == 0 || db.opts.MigrateDuration <= 0 {
		return
	}
	for i, block := range blocks {
		if i > 0 && blocks[0].Meta().MaxTime-block.Meta().MaxTime > int64(db.opts.MigrateDuration) {
			for _, b := range blocks[i:] {
				if db.migrate != nil {
					db.migrate(b.meta.ULID)
				}
			}
		}
	}
}

// deleteBlocks closes and deletes blocks from the disk.
// When the map contains a non nil block object it means it is loaded in memory
// so needs to be closed first as it might need to wait for pending readers to complete.
func (db *DB) deleteBlocks(blocks map[ulid.ULID]*Block) error {
	for ulid, block := range blocks {
		if block != nil {
			if err := block.Close(); err != nil {
				level.Warn(db.logger).Log("msg", "closing block failed", "err", err)
			}
		}
		if err := os.RemoveAll(filepath.Join(db.dir, ulid.String())); err != nil {
			return errors.Wrapf(err, "delete obsolete block %s", ulid)
		}
	}
	return nil
}

// validateBlockSequence returns error if given block meta files indicate that some blocks overlaps within sequence.
func validateBlockSequence(bs []*Block) error {
	if len(bs) <= 1 {
		return nil
	}

	var metas []BlockMeta
	for _, b := range bs {
		metas = append(metas, b.meta)
	}

	overlaps := OverlappingBlocks(metas)
	if len(overlaps) > 0 {
		return errors.Errorf("block time ranges overlap: %s", overlaps)
	}

	return nil
}

// TimeRange specifies minTime and maxTime range.
type TimeRange struct {
	Min, Max int64
}

// Overlaps contains overlapping blocks aggregated by overlapping range.
type Overlaps map[TimeRange][]BlockMeta

// String returns human readable string form of overlapped blocks.
func (o Overlaps) String() string {
	var res []string
	for r, overlaps := range o {
		var groups []string
		for _, m := range overlaps {
			groups = append(groups, fmt.Sprintf(
				"<ulid: %s, mint: %d, maxt: %d, range: %s>",
				m.ULID.String(),
				m.MinTime,
				m.MaxTime,
				(time.Duration((m.MaxTime-m.MinTime)/1000)*time.Second).String(),
			))
		}
		res = append(res, fmt.Sprintf(
			"[mint: %d, maxt: %d, range: %s, blocks: %d]: %s",
			r.Min, r.Max,
			(time.Duration((r.Max-r.Min)/1000)*time.Second).String(),
			len(overlaps),
			strings.Join(groups, ", ")),
		)
	}
	return strings.Join(res, "\n")
}

// OverlappingBlocks returns all overlapping blocks from given meta files.
func OverlappingBlocks(bm []BlockMeta) Overlaps {
	if len(bm) <= 1 {
		return nil
	}
	var (
		overlaps [][]BlockMeta

		// pending contains not ended blocks in regards to "current" timestamp.
		pending = []BlockMeta{bm[0]}
		// continuousPending helps to aggregate same overlaps to single group.
		continuousPending = true
	)

	// We have here blocks sorted by minTime. We iterate over each block and treat its minTime as our "current" timestamp.
	// We check if any of the pending block finished (blocks that we have seen before, but their maxTime was still ahead current
	// timestamp). If not, it means they overlap with our current block. In the same time current block is assumed pending.
	for _, b := range bm[1:] {
		var newPending []BlockMeta

		for _, p := range pending {
			// "b.MinTime" is our current time.
			if b.MinTime >= p.MaxTime {
				continuousPending = false
				continue
			}

			// "p" overlaps with "b" and "p" is still pending.
			newPending = append(newPending, p)
		}

		// Our block "b" is now pending.
		pending = append(newPending, b)
		if len(newPending) == 0 {
			// No overlaps.
			continue
		}

		if continuousPending && len(overlaps) > 0 {
			overlaps[len(overlaps)-1] = append(overlaps[len(overlaps)-1], b)
			continue
		}
		overlaps = append(overlaps, append(newPending, b))
		// Start new pendings.
		continuousPending = true
	}

	// Fetch the critical overlapped time range foreach overlap groups.
	overlapGroups := Overlaps{}
	for _, overlap := range overlaps {

		minRange := TimeRange{Min: 0, Max: math.MaxInt64}
		for _, b := range overlap {
			if minRange.Max > b.MaxTime {
				minRange.Max = b.MaxTime
			}

			if minRange.Min < b.MinTime {
				minRange.Min = b.MinTime
			}
		}
		overlapGroups[minRange] = overlap
	}

	return overlapGroups
}

func (db *DB) String() string {
	return "HEAD"
}

// Blocks returns the databases persisted blocks.
func (db *DB) Blocks() []*Block {
	db.mtx.RLock()
	defer db.mtx.RUnlock()

	return db.blocks
}

// Head returns the databases's head.
func (db *DB) Head() *Head {
	return db.head
}

// Close the partition.
func (db *DB) Close() error {
	close(db.stopc)
	db.compactCancel()
	<-db.donec

	db.mtx.Lock()
	defer db.mtx.Unlock()

	var g errgroup.Group

	// blocks also contains all head blocks.
	for _, pb := range db.blocks {
		g.Go(pb.Close)
	}

	var merr tsdb_errors.MultiError

	merr.Add(g.Wait())

	if db.lockf != nil {
		merr.Add(db.lockf.Release())
	}
	merr.Add(db.head.Close())
	return merr.Err()
}

// DisableCompactions disables auto compactions.
func (db *DB) DisableCompactions() {
	db.autoCompactMtx.Lock()
	defer db.autoCompactMtx.Unlock()

	db.autoCompact = false
	level.Info(db.logger).Log("msg", "compactions disabled")
}

// EnableCompactions enables auto compactions.
func (db *DB) EnableCompactions() {
	db.autoCompactMtx.Lock()
	defer db.autoCompactMtx.Unlock()

	db.autoCompact = true
	level.Info(db.logger).Log("msg", "compactions enabled")
}

// Snapshot writes the current data to the directory. If withHead is set to true it
// will create a new block containing all data that's currently in the memory buffer/WAL.
func (db *DB) Snapshot(dir string, withHead bool) error {
	if dir == db.dir {
		return errors.Errorf("cannot snapshot into base directory")
	}
	if _, err := ulid.ParseStrict(dir); err == nil {
		return errors.Errorf("dir must not be a valid ULID")
	}

	db.headCompactMtx.Lock()
	defer db.headCompactMtx.Unlock()

	db.mtx.RLock()
	for _, b := range db.blocks {
		level.Info(db.logger).Log("msg", "snapshotting block", "block", b)

		if err := b.Snapshot(dir); err != nil {
			db.mtx.RUnlock()
			return errors.Wrapf(err, "error snapshotting block: %s", b.Dir())
		}
	}
	db.mtx.RUnlock()

	if !withHead {
		return nil
	}

	mint := db.head.MinTime()
	maxt := db.head.MaxTime()
	head := &compactRangeHead{rangeHead: &rangeHead{
		head: db.head,
		mint: mint,
		maxt: maxt,
	}}
	// Add +1 millisecond to block maxt because block intervals are half-open: [b.MinTime, b.MaxTime).
	// Because of this block intervals are always +1 than the total samples it includes.
	if _, err := db.compactor.Write(dir, head, mint, maxt+1, nil); err != nil {
		return errors.Wrap(err, "snapshot head block")
	}
	return nil
}

// Querier returns a new querier over the data partition for the given time range.
// A goroutine must not handle more than one open Querier.
func (db *DB) Querier(mint, maxt int64) (Querier, error) {
	var blocks []BlockReader
	var blockMetas []BlockMeta

	db.mtx.RLock()
	defer db.mtx.RUnlock()

	for _, b := range db.blocks {
		if b.OverlapsClosedInterval(mint, maxt) {
			blocks = append(blocks, b)
			blockMetas = append(blockMetas, b.Meta())
		}
	}
	if maxt >= db.head.MinTime() {
		blocks = append(blocks, &rangeHead{
			head: db.head,
			mint: mint,
			maxt: maxt,
		})
	}

	overlapping := len(OverlappingBlocks(blockMetas)) > 0
	if overlapping {
		sort.Slice(blocks, func(i, j int) bool {
			return blocks[i].Meta().CompactTime < blocks[i].Meta().CompactTime
		})
	}
	blockQueriers := make([]Querier, 0, len(blocks))
	for _, b := range blocks {
		q, err := NewBlockQuerier(b, mint, maxt)
		if err == nil {
			blockQueriers = append(blockQueriers, q)
			continue
		}
		// If we fail, all previously opened queriers must be closed.
		for _, q := range blockQueriers {
			q.Close()
		}
		return nil, errors.Wrapf(err, "open querier for block %s", b)
	}

	if overlapping {
		return &verticalQuerier{
			querier: querier{
				blocks: blockQueriers,
			},
		}, nil
	}

	return &querier{
		blocks: blockQueriers,
	}, nil
}

func rangeForTimestamp(t int64, width int64) (maxt int64) {
	return (t/width)*width + width
}

func rangeForStart(t int64, width int64) (maxt int64) {
	return (t / width) * width
}

// Delete implements deletion of metrics. It only has atomicity guarantees on a per-block basis.
func (db *DB) Delete(mint, maxt int64, ms ...*labels.Matcher) error {
	db.cmtx.Lock()
	defer db.cmtx.Unlock()

	var g errgroup.Group

	db.mtx.RLock()
	defer db.mtx.RUnlock()

	for _, b := range db.blocks {
		if b.OverlapsClosedInterval(mint, maxt) {
			g.Go(func(b *Block) func() error {
				return func() error { return b.Delete(mint, maxt, ms...) }
			}(b))
		}
	}
	g.Go(func() error {
		return db.head.Delete(mint, maxt, ms...)
	})
	return g.Wait()
}

// CleanTombstones re-writes any blocks with tombstones.
func (db *DB) CleanTombstones() (err error) {
	db.cmtx.Lock()
	defer db.cmtx.Unlock()

	start := time.Now()
	defer db.metrics.tombCleanTimer.Observe(time.Since(start).Seconds())

	newUIDs := []ulid.ULID{}
	defer func() {
		// If any error is caused, we need to delete all the new directory created.
		if err != nil {
			for _, uid := range newUIDs {
				dir := filepath.Join(db.Dir(), uid.String())
				if err := os.RemoveAll(dir); err != nil {
					level.Error(db.logger).Log("msg", "failed to delete block after failed `CleanTombstones`", "dir", dir, "err", err)
				}
			}
		}
	}()

	db.mtx.RLock()
	blocks := db.blocks[:]
	db.mtx.RUnlock()

	for _, b := range blocks {
		if uid, er := b.CleanTombstones(db.Dir(), db.compactor); er != nil {
			err = errors.Wrapf(er, "clean tombstones: %s", b.Dir())
			return err
		} else if uid != nil { // New block was created.
			newUIDs = append(newUIDs, *uid)
		}
	}
	return errors.Wrap(db.Reload(), "Reload blocks")
}

func isBlockDir(fi os.FileInfo) bool {
	if !fi.IsDir() {
		return false
	}
	_, err := ulid.ParseStrict(fi.Name())
	return err == nil
}

func blockDirs(dir string) ([]string, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var dirs []string

	for _, fi := range files {
		if isBlockDir(fi) {
			dirs = append(dirs, filepath.Join(dir, fi.Name()))
		}
	}
	return dirs, nil
}

func sequenceFiles(dir string) ([]string, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var res []string

	for _, fi := range files {
		if _, err := strconv.ParseUint(fi.Name(), 10, 64); err != nil {
			continue
		}
		res = append(res, filepath.Join(dir, fi.Name()))
	}
	return res, nil
}

func nextSequenceFile(dir string) (string, int, error) {
	names, err := fileutil.ReadDir(dir)
	if err != nil {
		return "", 0, err
	}

	i := uint64(0)
	for _, n := range names {
		j, err := strconv.ParseUint(n, 10, 64)
		if err != nil {
			continue
		}
		i = j
	}
	return filepath.Join(dir, fmt.Sprintf("%0.6d", i+1)), int(i + 1), nil
}

func closeAll(cs []io.Closer) error {
	var merr tsdb_errors.MultiError

	for _, c := range cs {
		merr.Add(c.Close())
	}
	return merr.Err()
}

func exponential(d, min, max time.Duration) time.Duration {
	d *= 2
	if d < min {
		d = min
	}
	if d > max {
		d = max
	}
	return d
}
