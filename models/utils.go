package models

import (
	"reflect"
	"sort"
)

// Orderedentries implements sort.Interface for []*TransactionLine based on
// the AccountID and amount fields.
type Orderedentries []*TransactionLine

func (entries Orderedentries) Len() int      { return len(entries) }
func (entries Orderedentries) Swap(i, j int) { entries[i], entries[j] = entries[j], entries[i] }
func (entries Orderedentries) Less(i, j int) bool {
	if entries[i].AccountID == entries[j].AccountID {
		return entries[i].amount < entries[j].amount
	}
	return entries[i].AccountID < entries[j].AccountID
}

func containsSameElements(l1 []*TransactionLine, l2 []*TransactionLine) bool {
	lc1 := make([]*TransactionLine, len(l1))
	copy(lc1, l1)
	lc2 := make([]*TransactionLine, len(l2))
	copy(lc2, l2)
	sort.Sort(Orderedentries(lc1))
	sort.Sort(Orderedentries(lc2))
	return reflect.DeepEqual(lc1, lc2)
}
