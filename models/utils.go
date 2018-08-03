package models

import (


		"sort"

			"strings"
)

// Orderedentries implements sort.Interface for []*TransactionEntry based on
// the AccountID and Amount fields.
type Orderedentries []*TransactionEntry

func (entries Orderedentries) Len() int      { return len(entries) }
func (entries Orderedentries) Swap(i, j int) { entries[i], entries[j] = entries[j], entries[i] }
func (entries Orderedentries) Less(i, j int) bool {
	if entries[i].Account == entries[j].Account {
		return entries[i].Amount < entries[j].Amount
	}
	return entries[i].Account < entries[j].Account
}



func containsSameElements(l1 []*TransactionEntry, l2 []*TransactionEntry) bool {
	lc1 := make([]*TransactionEntry, len(l1))
	copy(lc1, l1)
	lc2 := make([]*TransactionEntry, len(l2))
	copy(lc2, l2)
	sort.Sort(Orderedentries(lc1))
	sort.Sort(Orderedentries(lc2))

	for i, entry := range lc1 {

		if strings.ToLower(entry.Account) != strings.ToLower(lc2[i].Account)  || entry.Amount != lc2[i].Amount{
			return false
		}

	}
	return true
}
