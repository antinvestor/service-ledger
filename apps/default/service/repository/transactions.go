package repository

import (
	"context"
	"encoding/json"
	"fmt"
	ledgerV1 "github.com/antinvestor/apis/go/ledger/v1"
	models2 "github.com/antinvestor/service-ledger/apps/default/service/models"
	"github.com/antinvestor/service-ledger/internal/apperrors"
	"github.com/pitabwire/frame"
	"github.com/pkg/errors"
	"github.com/shopspring/decimal"
	"strings"
	"time"
)

// DefaultTimestamLayout is the timestamp layout followed in Ledger
const DefaultTimestamLayout = "2006-01-02T15:04:05.999999999"

type TransactionRepository interface {
	GetByID(ctx context.Context, id string) (*models2.Transaction, apperrors.ApplicationLedgerError)
	Search(ctx context.Context, query string) (frame.JobResultPipe[[]*models2.Transaction], error)
	SearchEntries(ctx context.Context, query string) (frame.JobResultPipe[[]*models2.TransactionEntry], error)
	Validate(ctx context.Context, transaction *models2.Transaction) (map[string]*models2.Account, apperrors.ApplicationLedgerError)
	IsConflict(ctx context.Context, transaction2 *models2.Transaction) (bool, apperrors.ApplicationLedgerError)
	Transact(ctx context.Context, transaction *models2.Transaction) (*models2.Transaction, apperrors.ApplicationLedgerError)
	Update(ctx context.Context, transaction *models2.Transaction) (*models2.Transaction, apperrors.ApplicationLedgerError)
	Reverse(ctx context.Context, id string) (*models2.Transaction, apperrors.ApplicationLedgerError)
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

func (t *transactionRepository) Search(ctx context.Context, query string) (frame.JobResultPipe[[]*models2.Transaction], error) {

	service := t.service
	job := frame.NewJob(func(ctx context.Context, jobResult frame.JobResultPipe[[]*models2.Transaction]) error {

		rawQuery, err := NewSearchRawQuery(ctx, query)
		if err != nil {
			return jobResult.WriteError(ctx, err)
		}

		sqlQuery := rawQuery.ToQueryConditions()
		var transactionList []*models2.Transaction

		for sqlQuery.canLoad() {

			result := service.DB(ctx, true).Where(sqlQuery.sql, sqlQuery.args...).Offset(sqlQuery.offset).
				Limit(sqlQuery.batchSize).Find(&transactionList)
			err1 := result.Error
			if err1 != nil {

				return jobResult.WriteError(ctx, apperrors.ErrorSystemFailure.Override(err))
			}

			if len(transactionList) > 0 {

				var transactionIds []string
				for _, transaction := range transactionList {
					transactionIds = append(transactionIds, transaction.GetID())
				}

				entriesMap, err2 := t.SearchEntriesByTransactionID(ctx, transactionIds...)
				if err2 != nil {
					return jobResult.WriteError(ctx, apperrors.ErrorSystemFailure.Override(err))
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

	err := frame.SubmitJob(ctx, service, job)
	if err != nil {
		return nil, err
	}

	return job, nil

}

func (t *transactionRepository) SearchEntriesByTransactionID(ctx context.Context, transactionIDs ...string) (map[string][]*models2.TransactionEntry, error) {

	entriesMap := make(map[string][]*models2.TransactionEntry)

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
		return nil, apperrors.ErrorSystemFailure.Override(err).Extend("Json marshalling error")
	}

	logger := t.service.Log(ctx)

	query := string(queryBytes)

	logger.WithField("query", query).Info("Query from database")

	jobResult, err := t.SearchEntries(ctx, query)
	if err != nil {
		logger.WithError(err).Info("could not query for entries")

		return nil, apperrors.ErrorSystemFailure.Override(err).Extend(fmt.Sprintf("db query error [%s]", query))
	}

	for {

		logger.Info("reading results")

		result, ok := jobResult.ReadResult(ctx)

		if !ok {
			return entriesMap, nil
		}
		if result.IsError() {
			logger.WithError(result.Error()).Info("could not read results")
			return nil, apperrors.ErrorSystemFailure.Override(result.Error())
		}

		for _, entry := range result.Item() {

			entries, ok0 := entriesMap[entry.TransactionID]
			if !ok0 {
				entries = make([]*models2.TransactionEntry, 0)
			}

			entriesMap[entry.TransactionID] = append(entries, entry)
		}

	}

}

func (t *transactionRepository) SearchEntries(ctx context.Context, query string) (frame.JobResultPipe[[]*models2.TransactionEntry], error) {

	service := t.service

	job := frame.NewJob(func(ctx context.Context, jobResult frame.JobResultPipe[[]*models2.TransactionEntry]) error {

		rawQuery, err := NewSearchRawQuery(ctx, query)
		if err != nil {
			return jobResult.WriteError(ctx, err)
		}

		sqlQuery := rawQuery.ToQueryConditions()
		var transactionEntriesList []*models2.TransactionEntry

		for sqlQuery.canLoad() {

			result := service.DB(ctx, true).Offset(sqlQuery.offset).Limit(sqlQuery.batchSize).
				Where(sqlQuery.sql, sqlQuery.args...).Find(&transactionEntriesList)

			err1 := result.Error
			if err1 != nil {
				return jobResult.WriteError(ctx, apperrors.ErrorSystemFailure.Override(err1))
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

	err := frame.SubmitJob(ctx, service, job)
	if err != nil {
		return nil, err
	}

	return job, nil

}

// Validate checks all issues around transaction are satisfied
func (t *transactionRepository) Validate(ctx context.Context, txn *models2.Transaction) (map[string]*models2.Account, apperrors.ApplicationLedgerError) {

	if ledgerV1.TransactionType_NORMAL.String() == txn.TransactionType || ledgerV1.TransactionType_REVERSAL.String() == txn.TransactionType {
		// Skip if the transaction is invalid
		// by validating the amount values
		if !txn.IsZeroSum() {
			return nil, apperrors.ErrorTransactionHasNonZeroSum
		}

		if !txn.IsTrueDrCr() {
			return nil, apperrors.ErrorTransactionHasInvalidDrCrEntry
		}

	} else {

		if ledgerV1.TransactionType_RESERVATION.String() == txn.TransactionType {
			if len(txn.Entries) != 1 {
				return nil, apperrors.ErrorTransactionHasInvalidDrCrEntry
			}
		}

	}

	if len(txn.Entries) == 0 {
		return nil, apperrors.ErrorTransactionEntriesNotFound
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
			return nil, apperrors.ErrorTransactionEntryHasZeroAmount.Extend(fmt.Sprintf("A transaction entry for account : %s has a zero amount", entry.AccountID))
		}

		account, ok := accountsMap[entry.AccountID]
		if !ok {
			// // Accounts have to be predefined hence check all references exist.
			return nil, apperrors.ErrorAccountNotFound.Extend(fmt.Sprintf("Account %s was not found in the system", entry.AccountID))
		}

		if !strings.EqualFold(txn.Currency, account.Currency) {
			return nil, apperrors.ErrorTransactionAccountsDifferCurrency.Extend(fmt.Sprintf("Account %s has differing currency of %s to transaction currency of %s", entry.AccountID, account.Currency, txn.Currency))
		}
	}

	return accountsMap, nil
}

// IsConflict says whether a transaction conflicts with an existing transaction
func (t *transactionRepository) IsConflict(ctx context.Context, transaction2 *models2.Transaction) (bool, apperrors.ApplicationLedgerError) {

	transaction1, err := t.GetByID(ctx, transaction2.ID)
	if err != nil {
		return false, err
	}

	// CompareMoney new and existing transaction Entries
	return !containsSameElements(transaction1.Entries, transaction2.Entries), nil
}

// Transact creates the input transaction in the DB
func (t *transactionRepository) Transact(ctx context.Context, transaction *models2.Transaction) (*models2.Transaction, apperrors.ApplicationLedgerError) {

	// Check if a transaction with Reference already exists
	existingTransaction, aerr := t.GetByID(ctx, transaction.GetID())
	if aerr != nil && !errors.Is(aerr, apperrors.ErrorTransactionNotFound) {
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
			return nil, apperrors.ErrorTransactionIsConfilicting
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

		account := accountsMap[line.AccountID]

		line.Balance = decimal.NewNullDecimal(account.Balance.Decimal)

		// Decide the signage of entry based on : https://en.wikipedia.org/wiki/Double-entry_bookkeeping :DEADCLIC
		if line.Credit && (account.LedgerType == models2.LEDGER_TYPE_ASSET || account.LedgerType == models2.LEDGER_TYPE_EXPENSE) ||
			!line.Credit && (account.LedgerType == models2.LEDGER_TYPE_LIABILITY || account.LedgerType == models2.LEDGER_TYPE_INCOME || account.LedgerType == models2.LEDGER_TYPE_CAPITAL) {
			line.Amount = decimal.NewNullDecimal(line.Amount.Decimal.Neg())
		}
	}

	// Create the transaction and its entries
	err := t.service.DB(ctx, false).Create(transaction).Error
	if err != nil {
		return nil, apperrors.ErrorSystemFailure.Override(err)
	}

	return t.GetByID(ctx, transaction.GetID())
}

// GetByID returns a transaction with the given Reference
func (t *transactionRepository) GetByID(ctx context.Context, id string) (*models2.Transaction, apperrors.ApplicationLedgerError) {

	if id == "" {
		return nil, apperrors.ErrorUnspecifiedReference
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
		return nil, apperrors.ErrorSystemFailure.Override(err).Extend("Json marshalling error")
	}

	query := string(queryBytes)

	jobResult, err := t.Search(ctx, query)
	if err != nil {
		return nil, apperrors.ErrorSystemFailure.Override(err)
	}

	var transactions []*models2.Transaction
	var terminalError apperrors.ApplicationLedgerError
	for {

		result, ok := jobResult.ReadResult(ctx)

		if !ok {
			if terminalError != nil {
				return nil, terminalError
			}

			if len(transactions) > 0 {
				return transactions[0], nil
			}

			return nil, apperrors.ErrorTransactionNotFound
		}

		if result.IsError() {
			return nil, apperrors.ErrorSystemFailure.Override(result.Error())
		}

		transactions = append(transactions, result.Item()...)
	}

}

// Update updates data of the given transaction
func (t *transactionRepository) Update(ctx context.Context, txn *models2.Transaction) (*models2.Transaction, apperrors.ApplicationLedgerError) {
	existingTransaction, errTx := t.GetByID(ctx, txn.ID)
	if errTx != nil {
		return nil, errTx
	}

	for key, value := range txn.Data {
		if value != "" && value != existingTransaction.Data[key] {
			existingTransaction.Data[key] = value
		}
	}

	if existingTransaction.ClearedAt == nil {
		if txn.ClearedAt != nil {

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

	err := t.service.DB(ctx, false).Save(existingTransaction).Error
	if err != nil {
		t.service.Log(ctx).WithError(err).Error("could not save the transaction")
		return nil, apperrors.ErrorSystemFailure.Override(err)
	}
	return existingTransaction, nil

}

// Reverse creates a reversal  of the input transaction by creating a new transaction
func (t *transactionRepository) Reverse(ctx context.Context, id string) (*models2.Transaction, apperrors.ApplicationLedgerError) {

	// Check if a transaction with same Reference already exists
	reversalTxn, err1 := t.GetByID(ctx, id)
	if err1 != nil {
		return nil, err1
	}

	if ledgerV1.TransactionType_NORMAL.String() != reversalTxn.TransactionType {
		return nil, apperrors.ErrorTransactionTypeNotReversible.Extend(fmt.Sprintf(" supplied type : %s", reversalTxn.TransactionType))
	}

	for _, entry := range reversalTxn.Entries {
		entry.ID = fmt.Sprintf("%s_REVERSAL", entry.ID)
		entry.Credit = !entry.Credit
	}

	reversalTxn.ID = fmt.Sprintf("%s_REVERSAL", reversalTxn.ID)
	reversalTxn.TransactionType = ledgerV1.TransactionType_REVERSAL.String()

	timeNow := time.Now()
	reversalTxn.TransactedAt = &timeNow
	reversalTxn.CreatedAt = time.Now()
	reversalTxn.ModifiedAt = time.Now()

	return t.Transact(ctx, reversalTxn)
}
