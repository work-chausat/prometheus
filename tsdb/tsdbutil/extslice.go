package tsdbutil

func ExceptHash(origin []uint64, except map[uint64]struct{}) []uint64 {
	found := false
	for _, id := range origin {
		if _, ok := except[id]; ok {
			found = true
			break
		}
	}
	if !found {
		return origin
	}

	repl := make([]uint64, 0, len(origin))

	for _, id := range origin {
		if _, ok := except[id]; !ok {
			repl = append(repl, id)
		}
	}
	return repl
}

/**
the two slice must be in ascending order
*/
func ExceptSlice(origin []uint64, except []uint64) []uint64 {
	oIndex, eIndex := 0, 0
	oLen, eLen := len(origin), len(except)

	found := false
	for oIndex < oLen && eIndex < eLen {
		if origin[oIndex] == except[eIndex] {
			found = true
			break
		} else if origin[oIndex] < except[eIndex] {
			oIndex++
		} else {
			eIndex++
		}
	}
	if !found {
		return origin
	}

	repl := make([]uint64, 0, len(origin))
	repl = append(repl, origin[0:oIndex]...)

	for oIndex < oLen {
		if eIndex == eLen {
			repl = append(repl, origin[oIndex:]...)
			break
		}

		if origin[oIndex] < except[eIndex] {
			repl = append(repl, origin[oIndex])
			oIndex++
			continue
		} else if origin[oIndex] == except[eIndex] {
			oIndex++
			eIndex++
			continue
		} else {
			eIndex++
		}
	}

	return repl
}
