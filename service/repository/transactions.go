package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	ledgerV1 "github.com/antinvestor/apis/go/ledger/v1"
	"github.com/antinvestor/service-ledger/service/models"
	"github.com/antinvestor/service-ledger/service/utility"
	"github.com/pitabwire/frame"
	"github.com/pkg/errors"
	"github.com/shopspring/decimal"
	"strings"
	"time"
)

// DefaultTimestamLayout is the timestamp layout followed in Ledger
const DefaultTimestamLayout = "2006-01-02T15:04:05.999999999"

type TransactionRepository interface {
	GetByID(ctx context.Context, id string) (*models.Transaction, utility.ApplicationLedgerError)
	Search(ctx context.Context, query string) (frame.JobResultPipe, error)
	SearchEntries(ctx context.Context, query string) (frame.JobResultPipe, error)
	Validate(ctx context.Context, transaction *models.Transaction) (map[string]*models.Account, utility.ApplicationLedgerError)
	IsConflict(ctx context.Context, transaction2 *models.Transaction) (bool, utility.ApplicationLedgerError)
	Transact(ctx context.Context, transaction *models.Transaction) (*models.Transaction, utility.ApplicationLedgerError)
	Update(ctx context.Context, transaction *models.Transaction) (*models.Transaction, utility.ApplicationLedgerError)
	Reverse(ctx context.Context, id string) (*models.Transaction, utility.ApplicationLedgerError)
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

func (t *transactionRepository) Search(ctx context.Context, query string) (frame.JobResultPipe, error) {

	service := t.service
	job := service.NewJob(func(ctx context.Context, jobResult frame.JobResultPipe) error {

		rawQuery, err := NewSearchRawQuery(ctx, query)
		if err != nil {
			return jobResult.WriteResult(ctx, err)
		}

		sqlQuery := rawQuery.ToQueryConditions()
		var transactionList []*models.Transaction

		for sqlQuery.canLoad() {

			result := service.DB(ctx, true).Where(sqlQuery.sql, sqlQuery.args...).Offset(sqlQuery.offset).
				Limit(sqlQuery.batchSize).Find(&transactionList)
			err1 := result.Error
			if err1 != nil {

				return jobResult.WriteResult(ctx, utility.ErrorSystemFailure.Override(err))
			}

			if len(transactionList) > 0 {

				var transactionIds []string
				for _, transaction := range transactionList {
					transactionIds = append(transactionIds, transaction.GetID())
				}

				entriesMap, err2 := t.SearchEntriesByTransactionID(ctx, transactionIds...)
				if err2 != nil {
					return jobResult.WriteResult(ctx, utility.ErrorSystemFailure.Override(err))
				}

				for _, transaction := range transactionList {
					entries, ok := entriesMap[transaction.GetID()]
					if ok {
						transaction.Entries = entries
					}
				}
			}

			err1 = jobResult.WriteResult(ctx, transactionList)
			if err1 != nil {
				return err1
			}

			if sqlQuery.stop(len(transactionList)) {
				break
			}
		}
		return nil

	})

	err := service.SubmitJob(ctx, job)
	if err != nil {
		return nil, err
	}

	return job, nil

}

func (t *transactionRepository) SearchEntriesByTransactionID(ctx context.Context, transactionIDs ...string) (map[string][]*models.TransactionEntry, error) {

	entriesMap := make(map[string][]*models.TransactionEntry)

	queryMap := map[string]any{
		"query": map[string]any{
			"must": map[string]any{
				"fields": []map[string]any{
					{
						"transaction_id": map[string][]string{
							"in": transactionIDs,
						},
					},
				},
			},
		},
	}

	queryBytes, err := json.Marshal(queryMap)
	if err != nil {
		return nil, utility.ErrorSystemFailure.Override(err).Extend("Json marshalling error")
	}

	logger := t.service.L()

	query := string(queryBytes)

	logger.WithField("query", query).Info("Query from database")

	jobResult, err := t.SearchEntries(ctx, query)
	if err != nil {
		logger.WithError(err).Info("could not query for entries")

		return nil, utility.ErrorSystemFailure.Override(err).Extend(fmt.Sprintf("db query error [%s]", query))
	}

	for {

		logger.Info("reading results")

		result, ok, err0 := jobResult.ReadResult(ctx)
		if err0 != nil {
			logger.WithError(err).Info("could not read results")
			return nil, utility.ErrorSystemFailure.Override(err0)
		}

		if !ok {
			return entriesMap, nil
		}

		logger.WithField("result", result).Info("found transaction entry")
		switch v := result.(type) {
		case []*models.TransactionEntry:

			for _, entry := range v {

				entries, ok0 := entriesMap[entry.TransactionID]
				if !ok0 {
					entries = make([]*models.TransactionEntry, 0)
				}

				entriesMap[entry.TransactionID] = append(entries, entry)
			}

		case error:
			return nil, utility.ErrorSystemFailure.Override(v)
		default:
			return nil, utility.ErrorBadDataSupplied.Extend(fmt.Sprintf(" unsupported type supplied %v", v))
		}

	}

}

func (t *transactionRepository) SearchEntries(ctx context.Context, query string) (frame.JobResultPipe, error) {

	service := t.service

	job := service.NewJob(func(ctx context.Context, jobResult frame.JobResultPipe) error {

		rawQuery, err := NewSearchRawQuery(ctx, query)
		if err != nil {
			return jobResult.WriteResult(ctx, err)
		}

		sqlQuery := rawQuery.ToQueryConditions()
		var transactionEntriesList []*models.TransactionEntry

		for sqlQuery.canLoad() {

			result := service.DB(ctx, true).Offset(sqlQuery.offset).Limit(sqlQuery.batchSize).
				Where(sqlQuery.sql, sqlQuery.args...).Find(&transactionEntriesList)

			err1 := result.Error
			if err1 != nil {
				return jobResult.WriteResult(ctx, utility.ErrorSystemFailure.Override(err1))
			}

			err1 = jobResult.WriteResult(ctx, transactionEntriesList)
			if err1 != nil {
				return err1
			}

			if sqlQuery.stop(len(transactionEntriesList)) {
				break
			}
		}

		return nil

	})

	err := service.SubmitJob(ctx, job)
	if err != nil {
		return nil, err
	}

	return job, nil

}

// Validate checks all issues around transaction are satisfied
func (t *transactionRepository) Validate(ctx context.Context, txn *models.Transaction) (map[string]*models.Account, utility.ApplicationLedgerError) {

	if ledgerV1.TransactionType_NORMAL.String() == txn.TransactionType || ledgerV1.TransactionType_REVERSAL.String() == txn.TransactionType {
		// Skip if the transaction is invalid
		// by validating the amount values
		if !txn.IsZeroSum() {
			return nil, utility.ErrorTransactionHasNonZeroSum
		}

		if !txn.IsTrueDrCr() {
			return nil, utility.ErrorTransactionHasInvalidDrCrEntry
		}

	} else {

		if ledgerV1.TransactionType_RESERVATION.String() == txn.TransactionType {
			if len(txn.Entries) != 1 {
				return nil, utility.ErrorTransactionHasInvalidDrCrEntry
			}
		}

	}

	if len(txn.Entries) == 0 {
		return nil, utility.ErrorTransactionEntriesNotFound
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

		if entry.Amount.Decimal.IsZero() {
			return nil, utility.ErrorTransactionEntryHasZeroAmount.Extend(fmt.Sprintf("A transaction entry for account : %s has a zero amount", entry.AccountID))
		}

		account, ok := accountsMap[entry.AccountID]
		if !ok {
			//// Accounts have to be predefined hence check all references exist.
			return nil, utility.ErrorAccountNotFound.Extend(fmt.Sprintf("Account %s was not found in the system", entry.AccountID))
		}

		if !strings.EqualFold(txn.Currency, account.Currency) {
			return nil, utility.ErrorTransactionAccountsDifferCurrency.Extend(fmt.Sprintf("Account %s has differing currency of %s to transaction currency of %s", entry.AccountID, account.Currency, txn.Currency))
		}
	}

	return accountsMap, nil
}

// IsConflict says whether a transaction conflicts with an existing transaction
func (t *transactionRepository) IsConflict(ctx context.Context, transaction2 *models.Transaction) (bool, utility.ApplicationLedgerError) {

	transaction1, err := t.GetByID(ctx, transaction2.ID)
	if err != nil {
		return false, err
	}

	// CompareMoney new and existing transaction Entries
	return !containsSameElements(transaction1.Entries, transaction2.Entries), nil
}

// Transact creates the input transaction in the DB
func (t *transactionRepository) Transact(ctx context.Context, transaction *models.Transaction) (*models.Transaction, utility.ApplicationLedgerError) {

	// Check if a transaction with Reference already exists
	existingTransaction, aerr := t.GetByID(ctx, transaction.ID)
	if aerr != nil && !errors.Is(aerr, utility.ErrorTransactionNotFound) {
		return nil, aerr
	}

	if existingTransaction != nil {

		isConflict := false
		// Check if the transaction entries are different
		// and conflicts with the existing entries
		isConflict, aerr = t.IsConflict(ctx, transaction)
		if aerr != nil {
			return nil, aerr
		}
		if isConflict {
			// The conflicting transactions are denied
			return nil, utility.ErrorTransactionIsConfilicting
		}

		// Otherwise the transaction is just a duplicate
		// The exactly duplicate transactions are ignored
		return existingTransaction, nil
	}

	accountsMap, aerr := t.Validate(ctx, transaction)
	if aerr != nil {
		return nil, aerr
	}

	// Add transaction Entries in one go to succeed or fail all
	for _, line := range transaction.Entries {

		line.TransactionID = transaction.GetID()
		account := accountsMap[line.AccountID]

		line.Balance = decimal.NewNullDecimal(account.Balance.Decimal)

		// Decide the signage of entry based on : https://en.wikipedia.org/wiki/Double-entry_bookkeeping :DEADCLIC
		if line.Credit && (account.LedgerType == models.LEDGER_TYPE_ASSET || account.LedgerType == models.LEDGER_TYPE_EXPENSE) ||
			!line.Credit && (account.LedgerType == models.LEDGER_TYPE_LIABILITY || account.LedgerType == models.LEDGER_TYPE_INCOME || account.LedgerType == models.LEDGER_TYPE_CAPITAL) {
			line.Amount = decimal.NewNullDecimal(line.Amount.Decimal.Neg())
		}
	}

	logger := t.service.L().WithField("transaction", transaction)
	// Start a transaction
	// Save the transaction without entries
	logger.Info("Attempting to create a new transaction")
	err := t.service.DB(ctx, false).Create(&transaction).Error
	if err != nil {
		logger.WithError(err).Info("Failed to create a new transaction")
		return nil, utility.ErrorSystemFailure.Override(err)
	}
	logger.Info("Completed creating a new transaction")

	logger.Info("Create transaction entries for the transaction")
	// Save entries separately
	err = t.service.DB(ctx, false).CreateInBatches(transaction.Entries, len(transaction.Entries)).Error
	if err != nil {
		logger.WithError(err).Info("Failed to create transaction entries")
		return nil, utility.ErrorSystemFailure.Override(err)
	}
	logger.Info("Completed creating transaction entries")

	return t.GetByID(ctx, transaction.ID)
}

// GetByID returns a transaction with the given Reference
func (t *transactionRepository) GetByID(ctx context.Context, id string) (*models.Transaction, utility.ApplicationLedgerError) {

	if id == "" {
		return nil, utility.ErrorUnspecifiedReference
	}

	queryMap := map[string]any{
		"query": map[string]any{
			"must": map[string]any{
				"fields": []map[string]any{
					{
						"id": map[string]string{
							"eq": id,
						},
					},
				},
			},
		},
	}

	queryBytes, err := json.Marshal(queryMap)
	if err != nil {
		return nil, utility.ErrorSystemFailure.Override(err).Extend("Json marshalling error")
	}

	query := string(queryBytes)

	jobResult, err := t.Search(ctx, query)
	if err != nil {
		return nil, utility.ErrorSystemFailure.Override(err)
	}

	var transactions []*models.Transaction
	for {

		result, ok, err0 := jobResult.ReadResult(ctx)
		if err0 != nil {
			return nil, utility.ErrorSystemFailure.Override(err0)
		}

		if !ok {
			if len(transactions) > 0 {
				return transactions[0], nil
			}
			return nil, utility.ErrorTransactionNotFound
		}

		switch v := result.(type) {
		case []*models.Transaction:
			transactions = append(transactions, v...)
		case error:
			return nil, utility.ErrorSystemFailure.Override(v)
		default:
			return nil, utility.ErrorBadDataSupplied.Extend(fmt.Sprintf("unsupported type supplied %v", v))
		}

	}

}

// Update updates data of the given transaction
func (t *transactionRepository) Update(ctx context.Context, txn *models.Transaction) (*models.Transaction, utility.ApplicationLedgerError) {
	existingTransaction, errTx := t.GetByID(ctx, txn.ID)
	if errTx != nil {
		return nil, errTx
	}

	for key, value := range txn.Data {
		if value != "" && value != existingTransaction.Data[key] {
			existingTransaction.Data[key] = value
		}
	}

	if !existingTransaction.ClearedAt.Valid {
		if txn.ClearedAt.Valid {

			accountsMap, err1 := t.Validate(ctx, existingTransaction)
			if err1 != nil {
				return nil, err1
			}

			for _, line := range existingTransaction.Entries {
				account := accountsMap[line.AccountID]
				line.Balance = decimal.NewNullDecimal(account.Balance.Decimal)
			}
			existingTransaction.ClearedAt = txn.ClearedAt
		}
	}

	err := t.service.DB(ctx, false).Save(&existingTransaction).Error
	if err != nil {
		t.service.L().WithError(err).Error("could not save the transaction")
		return nil, utility.ErrorSystemFailure.Override(err)
	}
	return existingTransaction, nil

}

// Reverse creates a reversal  of the input transaction by creating a new transaction
func (t *transactionRepository) Reverse(ctx context.Context, id string) (*models.Transaction, utility.ApplicationLedgerError) {

	// Check if a transaction with same Reference already exists
	reversalTxn, err1 := t.GetByID(ctx, id)
	if err1 != nil {
		return nil, err1
	}

	if ledgerV1.TransactionType_NORMAL.String() != reversalTxn.TransactionType {
		return nil, utility.ErrorTransactionTypeNotReversible.Extend(fmt.Sprintf(" supplied type : %s", reversalTxn.TransactionType))
	}

	for _, entry := range reversalTxn.Entries {
		entry.ID = fmt.Sprintf("%s_REVERSAL", entry.ID)
		entry.Credit = !entry.Credit
	}

	reversalTxn.ID = fmt.Sprintf("%s_REVERSAL", reversalTxn.ID)
	reversalTxn.TransactionType = ledgerV1.TransactionType_REVERSAL.String()
	reversalTxn.TransactedAt = sql.NullTime{Time: time.Now(), Valid: true}
	reversalTxn.CreatedAt = time.Now()
	reversalTxn.ModifiedAt = time.Now()

	return t.Transact(ctx, reversalTxn)
}
