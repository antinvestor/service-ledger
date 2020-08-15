package models

import (
	"database/sql"
	"fmt"
	"github.com/rs/xid"
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
		return entries[i].Amount.Int64 < entries[j].Amount.Int64
	}
	return entries[i].Account.String < entries[j].Account.String
}

func containsSameElements(l1 []*TransactionEntry, l2 []*TransactionEntry) bool {
	lc1 := make([]*TransactionEntry, len(l1))
	copy(lc1, l1)
	lc2 := make([]*TransactionEntry, len(l2))
	copy(lc2, l2)
	sort.Sort(Orderedentries(lc1))
	sort.Sort(Orderedentries(lc2))

	if len(lc1) != len(lc2) {
		return false
	}

	for i, entry := range lc1 {

		if strings.ToUpper(entry.Account.String) != strings.ToUpper(lc2[i].Account.String) || entry.Amount.Int64 != lc2[i].Amount.Int64 {
			return false
		}

	}
	return true
}

func generateReference(prefix string) sql.NullString {
	return sql.NullString{String: fmt.Sprintf("%s_%s", prefix, xid.New().String()), Valid: true}
}
