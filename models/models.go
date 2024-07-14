package models

import (
	"github.com/pitabwire/frame"
	"github.com/shopspring/decimal"
	"gorm.io/datatypes"
)

// Ledger represents the hierarchy for organizing ledgers with information such as type, and JSON data
type Ledger struct {
	frame.BaseModel
	Type     string            `gorm:"type:varchar(50)" json:"type"`
	ParentID string            `gorm:"type:varchar(50)" json:"parent_id"`
	Data     datatypes.JSONMap `json:"data"`
}

// Account represents the ledger account with information such as Reference, balance and JSON data
type Account struct {
	frame.BaseModel
	Currency   string              `gorm:"type:varchar(10)" json:"currency"`
	Balance    decimal.NullDecimal `gorm:"-" json:"balance"`
	LedgerID   string              `gorm:"type:varchar(50)" json:"ledger_id"`
	Data       datatypes.JSONMap   `json:"data"`
	LedgerType string              `gorm:"type:varchar(50)" json:"ledger_type"`
}

// Transaction represents a transaction in a ledger
type Transaction struct {
	frame.BaseModel
	Currency     string              `gorm:"type:varchar(10)" json:"currency"`
	Data         datatypes.JSONMap   `json:"data"`
	TransactedAt string              `gorm:"type:varchar(50)" json:"transacted_at"`
	Entries      []*TransactionEntry `json:"entries"`
}

// TransactionEntry represents a transaction line in a ledger
type TransactionEntry struct {
	frame.BaseModel
	AccountID     string              `gorm:"type:varchar(50)" json:"account_id"`
	TransactionID string              `gorm:"type:varchar(50)" json:"transaction_id"`
	Amount        decimal.NullDecimal `gorm:"type:numeric" json:"amount"`
	Credit        bool                `json:"credit"`
	Balance       decimal.NullDecimal `gorm:"type:numeric"  json:"balance"`
	Currency      string              `gorm:"type:varchar(10)" json:"currency"`
	TransactedAt  string              `gorm:"type:varchar(50)" json:"transacted_at"`
}

func (t *TransactionEntry) Equal(ot TransactionEntry) bool {
	return t.AccountID == ot.AccountID && t.Amount.Valid && ot.Amount.Valid && t.Amount.Decimal.Equal(ot.Amount.Decimal)
}

// IsZeroSum validates the Amount list of a transaction
func (t *Transaction) IsZeroSum() bool {

	sum := decimal.NewFromInt(0)
	for _, entry := range t.Entries {
		if entry.Credit {
			sum = sum.Add(entry.Amount.Decimal)
		} else {
			sum = sum.Sub(entry.Amount.Decimal)
		}

	}
	return sum.IsZero()
}

// IsTrueDrCr validates that there is one debit and at least one credit entry
func (t *Transaction) IsTrueDrCr() bool {

	crEntries := 0
	drEntries := 0

	for _, entry := range t.Entries {
		if entry.Credit {
			crEntries += 1
		} else {
			drEntries += 1
		}
	}
	return drEntries == 1 && crEntries >= 1
}
