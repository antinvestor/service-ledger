package repositories

import (
	"github.com/antinvestor/service-ledger/models"
)

// Orderedentries implements sort.Interface for []*TransactionEntry based on
// the AccountID and Amount fields.
type Orderedentries []models.TransactionEntry

func (entries Orderedentries) Len() int      { return len(entries) }
func (entries Orderedentries) Swap(i, j int) { entries[i], entries[j] = entries[j], entries[i] }
func (entries Orderedentries) Less(i, j int) bool {
	if entries[i].AccountID == entries[j].AccountID {
		return entries[i].Amount.Decimal.LessThan(entries[j].Amount.Decimal)
	}
	return entries[i].AccountID < entries[j].AccountID
}

func containsSameElements(l1 []*models.TransactionEntry, l2 []*models.TransactionEntry) bool {

	l1Map := make(map[string]*models.TransactionEntry)

	if len(l1) != len(l2) {
		return false
	}

	for _, entry := range l1 {
		l1Map[entry.AccountID] = entry
	}

	for _, entry2 := range l2 {
		entry, ok := l1Map[entry2.AccountID]

		if !ok {
			return false
		}

		// Fix to tolerate floating point errors from elsewhere
		amount1 := entry.Amount.Decimal.Abs()
		amount2 := entry2.Amount.Decimal.Abs()
		if !amount1.Equal(amount2) {
			return false
		}
	}
	return true
}
