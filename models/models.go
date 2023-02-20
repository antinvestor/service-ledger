package models

import (
	"github.com/pitabwire/frame"
	"gorm.io/datatypes"
	"math/big"
)

// Ledger represents the hierachy for organizing ledgers with information such as type, and JSON data
type Ledger struct {
	frame.BaseModel
	Type     string            `gorm:"type:varchar(50)" json:"type"`
	ParentID string            `gorm:"type:varchar(50)" json:"parent_id"`
	Data     datatypes.JSONMap `json:"data"`
}

// Account represents the ledger account with information such as Reference, balance and JSON data
type Account struct {
	frame.BaseModel
	Currency   string            `gorm:"type:varchar(10)" json:"currency"`
	Balance    *big.Int          `gorm:"-" json:"balance"`
	LedgerID   string            `gorm:"type:varchar(50)" json:"ledger_id"`
	Data       datatypes.JSONMap `json:"data"`
	LedgerType string            `json:"ledger_type"`
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
	AccountID     string   `gorm:"type:varchar(50)" json:"account_id"`
	TransactionID string   `gorm:"type:varchar(50)" json:"transaction_id"`
	Amount        *big.Int `gorm:"type:bigint" json:"amount"`
	Credit        bool     `json:"credit"`
	Balance       *big.Int `gorm:"type:bigint"  json:"balance"`
	Currency      string   `gorm:"type:varchar(10)" json:"currency"`
	TransactedAt  string   `gorm:"type:varchar(50)" json:"transacted_at"`
}

func (t *TransactionEntry) Equal(ot TransactionEntry) bool {
	return t.AccountID == ot.AccountID && t.Amount.Cmp(ot.Amount) == 0
}

// IsValid validates the Amount list of a transaction
func (t *Transaction) IsValid() bool {
	sum := big.NewInt(0)
	for _, entry := range t.Entries {
		if entry.Credit {
			sum = big.NewInt(0).Add(sum, entry.Amount)
		} else {
			sum = big.NewInt(0).Sub(sum, entry.Amount)
		}

	}
	return big.NewInt(0).Cmp(sum) == 0
}
