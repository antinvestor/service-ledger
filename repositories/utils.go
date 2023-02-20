package repositories

import (
	"github.com/antinvestor/service-ledger/models"
	"log"
	"math/big"
	"strings"
)

// Orderedentries implements sort.Interface for []*TransactionEntry based on
// the AccountID and Amount fields.
type Orderedentries []*models.TransactionEntry

func (entries Orderedentries) Len() int      { return len(entries) }
func (entries Orderedentries) Swap(i, j int) { entries[i], entries[j] = entries[j], entries[i] }
func (entries Orderedentries) Less(i, j int) bool {
	if entries[i].AccountID == entries[j].AccountID {
		return entries[i].Amount.Cmp(entries[j].Amount) == -1
	}
	return entries[i].AccountID < entries[j].AccountID
}

func containsSameElements(l1 []*models.TransactionEntry, l2 []*models.TransactionEntry) bool {

	l1Map := make(map[string]*models.TransactionEntry)

	if len(l1) != len(l2) {
		log.Printf(" Transactions have different lengths of %d and %d", len(l1), len(l2))
		return false
	}

	for _, entry := range l1 {
		l1Account := strings.ToUpper(entry.AccountID)
		l1Map[l1Account] = entry
	}

	for _, entry2 := range l2 {
		l2Account := strings.ToUpper(entry2.AccountID)
		entry := l1Map[l2Account]

		if entry == nil {
			log.Printf(" Transaction account entry matching %s is missing", l2Account)
			return false
		}

		// Fix to tolerate floating point errors from elsewhere
		amount1 := big.NewInt(0).Abs(entry.Amount)
		amount2 := big.NewInt(0).Abs(entry2.Amount)
		if amount1.CmpAbs(amount2) != 0 {
			log.Printf(" Transacting account %s has mismatching amounts of %d and %d", l2Account, amount1, amount2)
			return false
		}
	}
	return true
}
