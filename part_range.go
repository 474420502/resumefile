package resumefile

import "fmt"

// PartRange SubdividedFile的Data里的块
type PartRange struct {
	Start uint64
	End   uint64
}

func (pd *PartRange) String() string {
	return fmt.Sprintf("[%d-%d]", pd.Start, pd.End)
}

// Merge 合并其他范围
func (pd *PartRange) Merge(other *PartRange) {
	if pd.Start > other.Start {
		pd.Start = other.Start
	}

	if pd.End < other.End {
		pd.End = other.End
	}
}

func partRangeCompare(k1, k2 interface{}) int {
	d1 := k1.(*PartRange)
	d2 := k2.(*PartRange)
	if d1.End < d2.Start {
		return -1
	} else if d1.Start > d2.End {
		return 1
	} else {
		return 0
	}
}
