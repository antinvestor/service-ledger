package models

import (
	"github.com/pitabwire/frame"
	"github.com/shopspring/decimal"
	"time"
)

// Ledger represents the hierarchy for organizing ledgers with information such as type, and JSON data
type Ledger struct {
	frame.BaseModel
	Type     string        `gorm:"type:varchar(50)" json:"type"`
	ParentID string        `gorm:"type:varchar(50)" json:"parent_id"`
	Data     frame.JSONMap `gorm:"type:jsonb;index:,gin:jsonb_path_ops" json:"data"`
}

// Account represents the ledger account with information such as Reference, balance and JSON data
type Account struct {
	frame.BaseModel
	Currency         string              `gorm:"type:varchar(10)" json:"currency"`
	Balance          decimal.NullDecimal `gorm:"-" json:"balance"`
	UnClearedBalance decimal.NullDecimal `gorm:"-" json:"un_cleared_balance"`
	ReservedBalance  decimal.NullDecimal `gorm:"-" json:"reserved_balance"`
	LedgerID         string              `gorm:"type:varchar(50)" json:"ledger_id"`
	Data             frame.JSONMap       `gorm:"type:jsonb;index:,gin:jsonb_path_ops" json:"data"`
	LedgerType       string              `gorm:"type:varchar(50)" json:"ledger_type"`
}

// Transaction represents a transaction in a ledger
type Transaction struct {
	frame.BaseModel
	Currency        string              `gorm:"type:varchar(10);not null" json:"currency"`
	TransactionType string              `gorm:"type:varchar(50)" json:"transaction_type"`
	Data            frame.JSONMap       `gorm:"type:jsonb;index:,gin:jsonb_path_ops" json:"data"`
	ClearedAt       *time.Time          `gorm:"type:timestamp" json:"cleared_at"`
	TransactedAt    *time.Time          `gorm:"type:timestamp" json:"transacted_at"`
	Entries         []*TransactionEntry `gorm:"foreignKey:TransactionID" json:"entries"`
}

// TransactionEntry represents a transaction line in a ledger
type TransactionEntry struct {
	frame.BaseModel
	AccountID     string              `gorm:"type:varchar(50);not null;index" json:"account_id"`
	TransactionID string              `gorm:"type:varchar(50);not null;index" json:"transaction_id"`
	Currency      string              `gorm:"-" json:"currency"`
	Amount        decimal.NullDecimal `gorm:"type:numeric(29,9)" json:"amount"`
	Credit        bool                `json:"credit"`
	Balance       decimal.NullDecimal `gorm:"type:numeric(29,9)"  json:"balance"`
	ClearedAt     *time.Time          `gorm:"-" json:"cleared_at"`
	TransactedAt  *time.Time          `gorm:"-" json:"transacted_at"`
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
