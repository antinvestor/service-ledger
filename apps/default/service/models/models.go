package models

import (
	"context"
	"time"

	ledgerv1 "buf.build/gen/go/antinvestor/ledger/protocolbuffers/go/ledger/v1"
	utility2 "github.com/antinvestor/service-ledger/internal/utility"
	"github.com/pitabwire/frame/data"
	"github.com/shopspring/decimal"
	"google.golang.org/genproto/googleapis/type/money"
)

// Ledger represents the hierarchy for organising ledgers with information such as type, and JSON data.
type Ledger struct {
	data.BaseModel
	Type     string       `gorm:"type:varchar(50)"                     json:"type"`
	ParentID string       `gorm:"type:varchar(50)"                     json:"parent_id"`
	Data     data.JSONMap `gorm:"type:jsonb;index:,gin:jsonb_path_ops" json:"data"`
}

func FromLedgerType(raw ledgerv1.LedgerType) string {
	return ledgerv1.LedgerType_name[int32(raw)]
}

func ToLedgerType(model string) ledgerv1.LedgerType {
	ledgerType := ledgerv1.LedgerType_value[model]
	return ledgerv1.LedgerType(ledgerType)
}

func (mLg *Ledger) ToAPI() *ledgerv1.Ledger {
	return &ledgerv1.Ledger{Id: mLg.ID, Type: ToLedgerType(mLg.Type),
		Parent: mLg.ParentID, Data: mLg.Data.ToProtoStruct()}
}

// Account represents the ledger account with information such as Reference, balance and JSON data.
type Account struct {
	data.BaseModel
	Currency         string              `gorm:"type:varchar(10)"                     json:"currency"`
	Balance          decimal.NullDecimal `gorm:"-"                                    json:"balance"`
	UnClearedBalance decimal.NullDecimal `gorm:"-"                                    json:"un_cleared_balance"`
	ReservedBalance  decimal.NullDecimal `gorm:"-"                                    json:"reserved_balance"`
	LedgerID         string              `gorm:"type:varchar(50)"                     json:"ledger_id"`
	Data             data.JSONMap        `gorm:"type:jsonb;index:,gin:jsonb_path_ops" json:"data"`
	LedgerType       string              `gorm:"type:varchar(50)"                     json:"ledger_type"`
}

func (mAcc *Account) ToAPI() *ledgerv1.Account {
	accountBalance := decimal.Zero
	if mAcc.Balance.Valid {
		accountBalance = mAcc.Balance.Decimal
	}
	balance := utility2.ToMoney(mAcc.Currency, accountBalance)

	reservedBalanceAmt := decimal.Zero
	if mAcc.ReservedBalance.Valid {
		reservedBalanceAmt = mAcc.ReservedBalance.Decimal
	}

	reservedBalance := utility2.ToMoney(mAcc.Currency, reservedBalanceAmt)

	unClearedBalanceAmt := decimal.Zero
	if mAcc.UnClearedBalance.Valid {
		unClearedBalanceAmt = mAcc.UnClearedBalance.Decimal
	}
	unClearedBalance := utility2.ToMoney(mAcc.Currency, unClearedBalanceAmt)

	return &ledgerv1.Account{
		Id: mAcc.ID, Ledger: mAcc.LedgerID,
		Balance: &balance, ReservedBalance: &reservedBalance, UnclearedBalance: &unClearedBalance,
		Data: mAcc.Data.ToProtoStruct()}
}

func TransactionFromAPI(ctx context.Context, aTxn *ledgerv1.Transaction) *Transaction {
	dataMap := &data.JSONMap{}
	transaction := &Transaction{
		Currency:        aTxn.GetCurrencyCode(),
		TransactionType: aTxn.GetType().String(),
		Data:            dataMap.FromProtoStruct(aTxn.GetData()),
	}

	transaction.GenID(ctx)
	transaction.ID = aTxn.GetId()

	// Parse transacted_at timestamp
	if aTxn.GetTransactedAt() != "" {
		if transactedAt, err := time.Parse(time.RFC3339, aTxn.GetTransactedAt()); err == nil {
			transaction.TransactedAt = transactedAt
		}
	}

	// Set cleared_at if transaction is cleared
	if aTxn.GetCleared() {
		transaction.ClearedAt = time.Now()
	}

	// Convert entries
	if len(aTxn.GetEntries()) > 0 {
		transaction.Entries = make([]*TransactionEntry, len(aTxn.GetEntries()))
		for index, aEntry := range aTxn.GetEntries() {
			transaction.Entries[index] = TransactionEntryFromAPI(aEntry)
		}
	}

	return transaction
}

func (mTxn *Transaction) ToAPI() *ledgerv1.Transaction {
	apiEntries := make([]*ledgerv1.TransactionEntry, len(mTxn.Entries))
	for index, mEntry := range mTxn.Entries {
		apiEntries[index] = mEntry.ToAPI()
	}

	trx := &ledgerv1.Transaction{
		Id:           mTxn.ID,
		CurrencyCode: mTxn.Currency,
		Cleared:      !mTxn.ClearedAt.IsZero(),
		Data:         mTxn.Data.ToProtoStruct(),
		Entries:      apiEntries,
	}

	// Convert transaction type
	if txnType, ok := ledgerv1.TransactionType_value[mTxn.TransactionType]; ok {
		trx.Type = ledgerv1.TransactionType(txnType)
	}

	// Format transacted_at timestamp
	if !mTxn.TransactedAt.IsZero() {
		trx.TransactedAt = mTxn.TransactedAt.Format(time.RFC3339)
	}

	return trx
}

func TransactionEntryFromAPI(aEntry *ledgerv1.TransactionEntry) *TransactionEntry {
	return &TransactionEntry{
		AccountID: aEntry.GetAccountId(),
		Amount:    decimal.NewNullDecimal(utility2.FromMoney(aEntry.GetAmount())),
		Credit:    aEntry.GetCredit(),
	}
}

func (mEntry *TransactionEntry) ToAPI() *ledgerv1.TransactionEntry {
	var amount *money.Money
	if mEntry.Amount.Valid {
		amt := utility2.ToMoney("", mEntry.Amount.Decimal)
		amount = &amt
	}

	return &ledgerv1.TransactionEntry{
		Id:            mEntry.ID,
		AccountId:     mEntry.AccountID,
		TransactionId: mEntry.TransactionID,
		Amount:        amount,
		Credit:        mEntry.Credit,
	}
}

// Transaction represents a transaction in a ledger.
type Transaction struct {
	data.BaseModel
	Currency        string              `gorm:"type:varchar(10);not null"            json:"currency"`
	TransactionType string              `gorm:"type:varchar(50)"                     json:"transaction_type"`
	Data            data.JSONMap        `gorm:"type:jsonb;index:,gin:jsonb_path_ops" json:"data"`
	ClearedAt       time.Time           `gorm:"type:timestamp"                       json:"cleared_at"`
	TransactedAt    time.Time           `gorm:"type:timestamp"                       json:"transacted_at"`
	Entries         []*TransactionEntry `gorm:"foreignKey:TransactionID"             json:"entries"`
}

// TransactionEntry represents a transaction line in a ledger.
type TransactionEntry struct {
	data.BaseModel
	AccountID     string              `gorm:"type:varchar(50);not null;index" json:"account_id"`
	TransactionID string              `gorm:"type:varchar(50);not null;index" json:"transaction_id"`
	Currency      string              `gorm:"-"                               json:"currency"`
	Amount        decimal.NullDecimal `gorm:"type:numeric(29,9)"              json:"amount"`
	Credit        bool                `                                       json:"credit"`
	Balance       decimal.NullDecimal `gorm:"type:numeric(29,9)"              json:"balance"`
	ClearedAt     time.Time           `gorm:"-"                               json:"cleared_at"`
	TransactedAt  time.Time           `gorm:"-"                               json:"transacted_at"`
}

func (t *TransactionEntry) Equal(ot TransactionEntry) bool {
	return t.AccountID == ot.AccountID && t.Amount.Valid && ot.Amount.Valid && t.Amount.Decimal.Equal(ot.Amount.Decimal)
}

// IsZeroSum validates the Amount list of a transaction.
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

// IsTrueDrCr validates that there is one debit and at least one credit entry.
func (t *Transaction) IsTrueDrCr() bool {
	crEntries := 0
	drEntries := 0

	for _, entry := range t.Entries {
		if entry.Credit {
			crEntries++
		} else {
			drEntries++
		}
	}
	return drEntries == 1 && crEntries >= 1
}
