package tsdbutil

//Search uses binary search to find index i in [0, n)
// return -1 if it does not exist

// search front part if func(x) >0 and after if func(x) <0
// fun(x)==0 return index
func Search(n int, f func(int) int) int {
	low, high := 0, n
	for low < high {
		mid := int(uint(low+high) >> 1)
		d := f(mid)
		if d < 0 {
			low = mid + 1
		} else if d > 0 {
			high = mid
		} else {
			return mid
		}
	}
	return -1
}

//Search uses binary search to find index i in [0, n]
// return -1 if it exist

func SearchSeat(n int, f func(int) int) (int, bool) {
	idx, low, high := 0, 0, n
	for low < high {
		mid := int(uint(low+high) >> 1)
		d := f(mid)
		if d == 0 {
			return mid, true
		} else if d < 0 {
			low = mid + 1
			idx = low
		} else {
			high = mid
			idx = mid
		}
	}
	return idx, false
}
