package models

import (
	"database/sql"
	"fmt"
	"github.com/rs/xid"
	"strings"
)

func Abs(n int64) int64 {
	y := n >> 63
	return (n ^ y) - y
}

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

	l1Map := make(map[string]*TransactionEntry)

	if len(l1) != len(l2) {
		return false
	}

	for _, entry := range l1 {
		l1Account := strings.ToUpper(entry.Account.String)
		l1Map[l1Account] = entry
	}

	for _, entry2 := range l2 {
		l2Account := strings.ToUpper(entry2.Account.String)
		entry := l1Map[l2Account]

		if entry == nil{
			return false
		}

		// Fix to tolerate floating point errors from elsewhere
		amount1 := Abs(entry.Amount.Int64)
		amount2 := Abs(entry2.Amount.Int64)
		if Abs(amount1 - amount2) > 1  {
			return false
		}
	}
	return true
}

func generateReference(prefix string) sql.NullString {
	newId := fmt.Sprintf("%s_%s", prefix, xid.New().String())
	return sql.NullString{String: strings.ToUpper(newId), Valid: true}
}
