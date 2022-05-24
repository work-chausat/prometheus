package tsdbutil

import (
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"
)

func TestExceptSlice(t *testing.T) {
	type args struct {
		origin []uint64
		except []uint64
	}
	tests := []struct {
		name string
		args args
		want []uint64
	}{
		{name: "allUp",
			args: args{
				origin: []uint64{1, 2, 3},
				except: []uint64{4, 5, 6},
			},
			want: []uint64{1, 2, 3},
		},
		{name: "allDown",
			args: args{
				origin: []uint64{4, 5, 6},
				except: []uint64{1, 2, 3},
			},
			want: []uint64{4, 5, 6},
		},
		{name: "union_1",
			args: args{
				origin: []uint64{1, 2, 3, 4},
				except: []uint64{1, 4, 6},
			},
			want: []uint64{2, 3},
		},
		{name: "union_1",
			args: args{
				origin: []uint64{1, 2, 3, 4},
				except: []uint64{4, 5, 6},
			},
			want: []uint64{1, 2, 3},
		},
		{name: "union_1",
			args: args{
				origin: []uint64{1, 2, 3, 4},
				except: []uint64{2, 4, 5},
			},
			want: []uint64{1, 3},
		},
		{name: "union_1",
			args: args{
				origin: []uint64{1, 2, 3, 4},
				except: []uint64{2, 3},
			},
			want: []uint64{1, 4},
		},
		{name: "union_1",
			args: args{
				origin: []uint64{2, 3, 4},
				except: []uint64{2, 4, 5},
			},
			want: []uint64{3},
		},
		{name: "union_nil",
			args: args{
				origin: nil,
				except: []uint64{2, 4, 5},
			},
			want: nil,
		},
		{name: "union_nil",
			args: args{
				origin: []uint64{2, 4, 5},
				except: nil,
			},
			want: []uint64{2, 4, 5},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ExceptSlice(tt.args.origin, tt.args.except); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ExceptSlice() = %v, want %v", got, tt.want)
			}

			deleted := make(map[uint64]struct{})
			for _, u := range tt.args.except {
				deleted[u] = struct{}{}
			}
			if got := ExceptHash(tt.args.origin, deleted); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ExceptHash() = %v, want %v", got, tt.want)
			}
		})
	}
}

func BenchmarkHash(b *testing.B) {
	deleted := make(map[int]struct{}, 100000)
	for i := 0; i < 60000; i++ {
		deleted[i] = struct{}{}
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for i := 0; i < len(deleted); i++ {
			if _, ok := deleted[i]; ok {

			}
		}
	}
}

func BenchmarkSlice(b *testing.B) {
	deleted := make([]int, 0, 100000)
	for i := 0; i < 60000; i++ {
		deleted = append(deleted, i)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for i := 0; i < len(deleted); i++ {

		}
	}
}

func TestExportData(t *testing.T) {
	rng := &RNG{}

	cost1 := time.Duration(0)
	cost2 := time.Duration(0)
	for i := 0; i < 10000; i++ {
		postingSize := rng.Uint32n(2e4)
		posting := make(map[uint64]struct{}, postingSize)
		for i := 0; i < int(postingSize); i++ {
			posting[uint64(rng.Uint32n(postingSize))] = struct{}{}
		}
		p := make([]uint64, 0, len(posting))
		for k := range posting {
			p = append(p, k)
		}
		sort.Slice(p, func(i, j int) bool {
			return p[i] < p[j]
		})

		deleteSize := rng.Uint32n(1e5)
		deleted := make(map[uint64]struct{}, deleteSize)
		for i := 0; i < int(deleteSize); i++ {
			deleted[uint64(rng.Uint32n(deleteSize))] = struct{}{}
		}
		d := make([]uint64, 0, len(deleted))
		for k := range deleted {
			d = append(d, k)
		}
		sort.Slice(d, func(i, j int) bool {
			return d[i] < d[j]
		})
		t1 := time.Now()
		set := ExceptHash(p, deleted)
		cost1 = cost1 + time.Now().Sub(t1)

		t2 := time.Now()
		slice := ExceptSlice(p, d)
		cost2 = cost2 + time.Now().Sub(t2)
		if len(set) != len(slice) {
			fmt.Println("origin:", p)
			fmt.Println("deleted:", d)
			fmt.Println("set:", set)
			fmt.Println("slice:", slice)
			panic("")
		}
		for i2, u := range set {
			if u != slice[i2] {
				fmt.Println("origin:", p)
				fmt.Println("deleted:", d)
				fmt.Println("set:", set)
				fmt.Println("slice:", slice)
				panic("")
			}
		}
	}

	fmt.Println("set:", cost1, "slice:", cost2)
}
