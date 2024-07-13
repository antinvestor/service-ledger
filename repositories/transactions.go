package repositories

import (
	"context"
	"github.com/antinvestor/service-ledger/models"
	"github.com/pitabwire/frame"
	"log"
	"math/big"
	"strings"
	"time"

	"fmt"
	"github.com/antinvestor/service-ledger/ledger"
	"github.com/pkg/errors"
)

const (
	// LedgerTimestampLayout is the timestamp layout followed in Ledger
	LedgerTimestampLayout = "2006-01-02 15:04:05.000"
)

type TransactionRepository interface {
	GetByID(ctx context.Context, id string) (*models.Transaction, ledger.ApplicationLedgerError)
	Search(ctx context.Context, query string) ([]*models.Transaction, ledger.ApplicationLedgerError)
	SearchEntries(ctx context.Context, query string) ([]*models.TransactionEntry, ledger.ApplicationLedgerError)
	Validate(ctx context.Context, transaction *models.Transaction) (map[string]*models.Account, ledger.ApplicationLedgerError)
	IsConflict(ctx context.Context, transaction2 *models.Transaction) (bool, ledger.ApplicationLedgerError)
	Transact(ctx context.Context, transaction *models.Transaction) (*models.Transaction, ledger.ApplicationLedgerError)
	Update(ctx context.Context, transaction *models.Transaction) (*models.Transaction, ledger.ApplicationLedgerError)
	Reverse(ctx context.Context, id string) (*models.Transaction, ledger.ApplicationLedgerError)
}

// transactionRepository is the interface to all transaction operations
type transactionRepository struct {
	service     *frame.Service
	accountRepo AccountRepository
}

// NewTransactionRepository returns a new instance of `transactionRepository`
func NewTransactionRepository(service *frame.Service, accountRepo AccountRepository) TransactionRepository {
	return &transactionRepository{
		service:     service,
		accountRepo: accountRepo,
	}
}

func (t *transactionRepository) Search(ctx context.Context, query string) ([]*models.Transaction, ledger.ApplicationLedgerError) {

	rawQuery, aerr := NewSearchRawQuery(ctx, query)
	if aerr != nil {
		return nil, aerr
	}

	sqlQuery := rawQuery.ToQueryConditions()
	var transactionsList []*models.Transaction

	conditions := append([]interface{}{sqlQuery.sql}, sqlQuery.args...)

	err := t.service.DB(ctx, true).Preload("Entries").
		Find(&transactionsList, conditions...).Offset(sqlQuery.offset).Limit(sqlQuery.limit).Error
	if err != nil {
		if frame.DBErrorIsRecordNotFound(err) {
			return nil, ledger.ErrorLedgerNotFound
		}
		return nil, ledger.ErrorSystemFailure.Override(err)
	}

	return transactionsList, nil
}

func (t *transactionRepository) SearchEntries(ctx context.Context, query string) ([]*models.TransactionEntry, ledger.ApplicationLedgerError) {

	rawQuery, aerr := NewSearchRawQuery(ctx, query)
	if aerr != nil {
		return nil, aerr
	}

	sqlQuery := rawQuery.ToQueryConditions()
	var entriesList []*models.TransactionEntry

	conditions := append([]interface{}{sqlQuery.sql}, sqlQuery.args...)

	err := t.service.DB(ctx, true).Find(&entriesList, conditions).Error
	if err != nil {
		if frame.DBErrorIsRecordNotFound(err) {
			return nil, ledger.ErrorLedgerNotFound
		}
		return nil, ledger.ErrorSystemFailure.Override(err)
	}

	return entriesList, nil
}

// Validate checks all issues around transaction are satisfied
func (t *transactionRepository) Validate(ctx context.Context, txn *models.Transaction) (map[string]*models.Account, ledger.ApplicationLedgerError) {

	// Skip if the transaction is invalid
	// by validating the amount values
	if !txn.IsValid() {
		return nil, ledger.ErrorTransactionHasNonZeroSum
	}

	accountIdSet := map[string]bool{}
	for _, entry := range txn.Entries {
		accountIdSet[entry.AccountID] = true
	}

	accountIds := make([]string, 0, len(accountIdSet))
	for k := range accountIdSet {
		accountIds = append(accountIds, k)
	}

	accountsMap, errAcc := t.accountRepo.ListByID(ctx, accountIds...)
	if errAcc != nil {
		return nil, errAcc
	}

	for _, entry := range txn.Entries {

		if big.NewInt(0).Cmp(entry.Amount.ToInt()) == 0 {
			return nil, ledger.ErrorTransactionEntryHasZeroAmount.Extend(fmt.Sprintf("A transaction entry for account : %s has a zero amount", entry.AccountID))
		}

		account, ok := accountsMap[entry.AccountID]
		if !ok {
			//// Accounts have to be predefined hence check all references exist.
			return nil, ledger.ErrorAccountNotFound.Extend(fmt.Sprintf("Account %s was not found in the system", entry.AccountID))
		}

		if !strings.EqualFold(txn.Currency, account.Currency) {
			t.service.L().Println(fmt.Sprintf("Account %s has differing currency of %s to transaction currency of %s", entry.AccountID, account.Currency, txn.Currency))
			return nil, ledger.ErrorTransactionAccountsDifferCurrency.Extend(fmt.Sprintf("Account %s has differing currency of %s to transaction currency of %s", entry.AccountID, account.Currency, txn.Currency))
		}

		// Helps us lock the account balance just before the transaction
		entry.Balance = account.Balance
	}

	return accountsMap, nil
}

// IsConflict says whether a transaction conflicts with an existing transaction
func (t *transactionRepository) IsConflict(ctx context.Context, transaction2 *models.Transaction) (bool, ledger.ApplicationLedgerError) {

	transaction1, err := t.GetByID(ctx, transaction2.ID)
	if err != nil {
		return false, err
	}

	// Compare new and existing transaction Entries
	return !containsSameElements(transaction1.Entries, transaction2.Entries), nil
}

// Transact creates the input transaction in the DB
func (t *transactionRepository) Transact(ctx context.Context, transaction *models.Transaction) (*models.Transaction, ledger.ApplicationLedgerError) {

	// Check if a transaction with same Reference already exists
	existingTransaction, err1 := t.GetByID(ctx, transaction.ID)
	if err1 != nil && !errors.Is(err1, ledger.ErrorTransactionNotFound) {
		return nil, err1
	}

	if existingTransaction != nil {
		// Check if the transaction entries are different
		// and conflicts with the existing entries
		isConflict, err1 := t.IsConflict(ctx, transaction)
		if err1 != nil {
			return nil, err1
		}
		if isConflict {
			log.Printf(" Transaction %s has conflicts", transaction.ID)
			// The conflicting transactions are denied
			return nil, ledger.ErrorTransactionIsConfilicting
		}
		// Otherwise the transaction is just a duplicate
		// The exactly duplicate transactions are ignored
		return existingTransaction, nil
	}

	accountsMap, err1 := t.Validate(ctx, transaction)
	if err1 != nil {
		return nil, err1
	}

	if transaction.TransactedAt == "" {
		transaction.TransactedAt = time.Now().UTC().Format(LedgerTimestampLayout)
	}

	// Add transaction Entries in one go to succeed or fail all
	for _, line := range transaction.Entries {
		account := accountsMap[line.AccountID]
		line.Currency = account.Currency

		entryAmount := line.Amount
		// Decide the signage of entry based on : https://en.wikipedia.org/wiki/Double-entry_bookkeeping :DEADCLIC
		if line.Credit && (account.LedgerType == models.LEDGER_TYPE_ASSET || account.LedgerType == models.LEDGER_TYPE_EXPENSE) ||
			!line.Credit && (account.LedgerType == models.LEDGER_TYPE_LIABILITY || account.LedgerType == models.LEDGER_TYPE_INCOME || account.LedgerType == models.LEDGER_TYPE_CAPITAL) {
			line.Amount = entryAmount.ToNeg()
		}
	}

	err := t.service.DB(ctx, false).Debug().Create(&transaction).Error
	if err != nil {
		return nil, ledger.ErrorSystemFailure.Override(err)
	}

	return t.GetByID(ctx, transaction.ID)
}

// GetByID returns a transaction with the given Reference
func (t *transactionRepository) GetByID(ctx context.Context, id string) (*models.Transaction, ledger.ApplicationLedgerError) {

	if id == "" {
		return nil, ledger.ErrorUnspecifiedReference
	}

	var transaction models.Transaction

	err := t.service.DB(ctx, true).
		Preload("Entries").
		First(&transaction, "id = ?", id).
		Error

	if err != nil {
		if frame.DBErrorIsRecordNotFound(err) {
			return nil, ledger.ErrorTransactionNotFound
		}
		return nil, ledger.ErrorSystemFailure.Override(err)
	}

	return &transaction, nil
}

// Update updates data of the given transaction
func (t *transactionRepository) Update(ctx context.Context, txn *models.Transaction) (*models.Transaction, ledger.ApplicationLedgerError) {
	existingTransaction, errTx := t.GetByID(ctx, txn.ID)
	if errTx != nil {
		return nil, errTx
	}

	for key, value := range txn.Data {
		if value != "" && value != existingTransaction.Data[key] {
			existingTransaction.Data[key] = value
		}
	}

	err := t.service.DB(ctx, false).Save(&existingTransaction).Error
	if err != nil {
		t.service.L().WithError(err).Error("could not save the transaction")
		return nil, ledger.ErrorSystemFailure.Override(err)
	}
	return existingTransaction, nil

}

// Reverse creates a reversal  of the input transaction by creating a new transaction
func (t *transactionRepository) Reverse(ctx context.Context, id string) (*models.Transaction, ledger.ApplicationLedgerError) {

	// Check if a transaction with same Reference already exists
	reversalTxn, err1 := t.GetByID(ctx, id)
	if err1 != nil {
		return nil, err1
	}

	for _, entry := range reversalTxn.Entries {
		entry.Credit = !entry.Credit
		entry.Amount = entry.Amount.ToAbs()
	}

	reversalTxn.ID = fmt.Sprintf("REVERSAL_%s", reversalTxn.ID)
	return t.Transact(ctx, reversalTxn)
}
