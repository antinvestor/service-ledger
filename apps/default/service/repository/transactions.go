package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	ledgerv1 "buf.build/gen/go/antinvestor/ledger/protocolbuffers/go/ledger/v1"
	"github.com/antinvestor/service-ledger/apps/default/service/models"
	"github.com/antinvestor/service-ledger/internal/apperrors"
	"github.com/pitabwire/frame/datastore"
	"github.com/pitabwire/frame/datastore/pool"
	"github.com/pitabwire/frame/workerpool"
	"github.com/pitabwire/util"
	"github.com/shopspring/decimal"
)

// DefaultTimestamLayout is the timestamp layout followed in Ledger.
const DefaultTimestamLayout = "2006-01-02T15:04:05.999999999"

type TransactionRepository interface {
	datastore.BaseRepository[*models.Transaction]
	Validate(ctx context.Context, transaction *models.Transaction,
	) (map[string]*models.Account, apperrors.ApplicationError)
	IsConflict(ctx context.Context, transaction2 *models.Transaction) (bool, apperrors.ApplicationError)
	Transact(ctx context.Context, transaction *models.Transaction,
	) (*models.Transaction, apperrors.ApplicationError)
	Reverse(ctx context.Context, id string) (*models.Transaction, apperrors.ApplicationError)
	SearchAsESQ(ctx context.Context, query string,
	) (workerpool.JobResultPipe[[]*models.Transaction], error)
	SearchEntries(ctx context.Context, query string,
	) (workerpool.JobResultPipe[[]*models.TransactionEntry], error)
}

// transactionRepository is the interface to all transaction operations.
type transactionRepository struct {
	accountRepo AccountRepository
	datastore.BaseRepository[*models.Transaction]
}

// NewTransactionRepository returns a new instance of `transactionRepository`.
func NewTransactionRepository(
	ctx context.Context,
	dbPool pool.Pool,
	workMan workerpool.Manager,
	accountRepo AccountRepository,
) TransactionRepository {
	return &transactionRepository{
		BaseRepository: datastore.NewBaseRepository[*models.Transaction](
			ctx, dbPool, workMan, func() *models.Transaction { return &models.Transaction{} },
		),
		accountRepo: accountRepo,
	}
}

func (t *transactionRepository) searchTransactions(
	ctx context.Context,
	sqlQuery *SearchSQLQuery,
) ([]*models.Transaction, error) {
	var transactionList []*models.Transaction

	result := t.Pool().DB(ctx, true).Where(sqlQuery.sql, sqlQuery.args...).Offset(sqlQuery.offset).
		Limit(sqlQuery.batchSize).Find(&transactionList)
	err1 := result.Error
	if err1 != nil {
		return transactionList, err1
	}

	if len(transactionList) > 0 {
		var transactionIDs []string
		for _, transaction := range transactionList {
			transactionIDs = append(transactionIDs, transaction.GetID())
		}

		entriesMap, err2 := t.SearchEntriesByTransactionID(ctx, transactionIDs...)
		if err2 != nil {
			return transactionList, err2
		}

		for _, transaction := range transactionList {
			entries, ok := entriesMap[transaction.GetID()]
			if ok {
				transaction.Entries = entries
			}
		}
	}

	return transactionList, nil
}

func (t *transactionRepository) SearchAsESQ(
	ctx context.Context, queryStr string,
) (workerpool.JobResultPipe[[]*models.Transaction], error) {
	job := workerpool.NewJob(
		func(ctx context.Context, jobResult workerpool.JobResultPipe[[]*models.Transaction]) error {
			rawQuery, err := NewSearchRawQuery(ctx, queryStr)
			if err != nil {
				return jobResult.WriteError(ctx, err)
			}

			sqlQuery := rawQuery.ToQueryConditions()

			for sqlQuery.canLoad() {
				transactionList, dbErr := t.searchTransactions(ctx, sqlQuery)
				if dbErr != nil {
					return jobResult.WriteError(ctx, apperrors.ErrSystemFailure.Override(dbErr))
				}
				dbErr = jobResult.WriteResult(ctx, transactionList)
				if dbErr != nil {
					return dbErr
				}

				if sqlQuery.stop(len(transactionList)) {
					break
				}
			}
			return nil
		},
	)

	err := workerpool.SubmitJob(ctx, t.WorkManager(), job)
	if err != nil {
		return nil, err
	}

	return job, nil
}

func (t *transactionRepository) SearchEntriesByTransactionID(
	ctx context.Context,
	transactionIDs ...string,
) (map[string][]*models.TransactionEntry, error) {
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
		return nil, apperrors.ErrSystemFailure.Override(err).Extend("Json marshalling error")
	}

	logger := util.Log(ctx)

	query := string(queryBytes)

	logger.WithField("query", query).Info("Query from database")

	jobResult, err := t.SearchEntries(ctx, query)
	if err != nil {
		logger.WithError(err).Info("could not query for entries")

		return nil, apperrors.ErrSystemFailure.Override(err).Extend(fmt.Sprintf("db query error [%s]", query))
	}

	for {
		logger.Info("reading results")

		result, ok := jobResult.ReadResult(ctx)

		if !ok {
			return entriesMap, nil
		}
		if result.IsError() {
			logger.WithError(result.Error()).Info("could not read results")
			return nil, apperrors.ErrSystemFailure.Override(result.Error())
		}

		for _, entry := range result.Item() {
			entries, ok0 := entriesMap[entry.TransactionID]
			if !ok0 {
				entries = make([]*models.TransactionEntry, 0)
			}

			entriesMap[entry.TransactionID] = append(entries, entry)
		}
	}
}

func (t *transactionRepository) SearchEntries(
	ctx context.Context,
	query string,
) (workerpool.JobResultPipe[[]*models.TransactionEntry], error) {
	job := workerpool.NewJob(
		func(ctx context.Context, jobResult workerpool.JobResultPipe[[]*models.TransactionEntry]) error {
			rawQuery, err := NewSearchRawQuery(ctx, query)
			if err != nil {
				return jobResult.WriteError(ctx, err)
			}

			sqlQuery := rawQuery.ToQueryConditions()
			var transactionEntriesList []*models.TransactionEntry

			for sqlQuery.canLoad() {
				result := t.Pool().DB(ctx, true).Offset(sqlQuery.offset).Limit(sqlQuery.batchSize).
					Where(sqlQuery.sql, sqlQuery.args...).Find(&transactionEntriesList)

				err1 := result.Error
				if err1 != nil {
					return jobResult.WriteError(ctx, apperrors.ErrSystemFailure.Override(err1))
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
		},
	)

	err := workerpool.SubmitJob(ctx, t.WorkManager(), job)
	if err != nil {
		return nil, err
	}

	return job, nil
}

// Validate checks all issues around transaction are satisfied.
func (t *transactionRepository) Validate(
	ctx context.Context,
	txn *models.Transaction,
) (map[string]*models.Account, apperrors.ApplicationError) {
	if ledgerv1.TransactionType_NORMAL.String() == txn.TransactionType ||
		ledgerv1.TransactionType_REVERSAL.String() == txn.TransactionType {
		// Skip if the transaction is invalid
		// by validating the amount values
		if !txn.IsZeroSum() {
			return nil, apperrors.ErrTransactionHasNonZeroSum
		}

		if !txn.IsTrueDrCr() {
			return nil, apperrors.ErrTransactionHasInvalidDrCrEntry
		}
	} else if ledgerv1.TransactionType_RESERVATION.String() == txn.TransactionType {
		if len(txn.Entries) != 1 {
			return nil, apperrors.ErrTransactionHasInvalidDrCrEntry
		}
	}

	if len(txn.Entries) == 0 {
		return nil, apperrors.ErrTransactionEntriesNotFound
	}

	accountIDSet := map[string]bool{}
	for _, entry := range txn.Entries {
		accountIDSet[entry.AccountID] = true
	}

	accountIDs := make([]string, 0, len(accountIDSet))
	for accountID := range accountIDSet {
		accountIDs = append(accountIDs, accountID)
	}

	accountsMap, errAcc := t.accountRepo.ListByID(ctx, accountIDs...)
	if errAcc != nil {
		return nil, errAcc
	}

	for _, entry := range txn.Entries {
		if entry.Amount.Decimal.IsZero() {
			return nil, apperrors.ErrTransactionEntryHasZeroAmount.Extend(
				fmt.Sprintf("entry [id=%s, account_id=%s] amount is zero", entry.ID, entry.AccountID),
			)
		}

		account, ok := accountsMap[entry.AccountID]
		if !ok {
			// // Accounts have to be predefined hence check all references exist.
			return nil, apperrors.ErrAccountNotFound.Extend(
				fmt.Sprintf("Account %s was not found in the system", entry.AccountID),
			)
		}

		if !strings.EqualFold(txn.Currency, account.Currency) {
			return nil, apperrors.ErrTransactionAccountsDifferCurrency.Extend(
				fmt.Sprintf(
					"entry [id=%s, account_id=%s] currency [%s] != [%s]",
					entry.ID,
					entry.AccountID,
					account.Currency,
					txn.Currency,
				),
			)
		}
	}

	return accountsMap, nil
}

// IsConflict says whether a transaction conflicts with an existing transaction.
func (t *transactionRepository) IsConflict(
	ctx context.Context,
	transaction2 *models.Transaction,
) (bool, apperrors.ApplicationError) {
	transaction1, err := t.GetByID(ctx, transaction2.ID)
	if err != nil {
		return false, apperrors.ErrSystemFailure.Override(err)
	}

	// CompareMoney new and existing transaction Entries
	return !containsSameElements(transaction1.Entries, transaction2.Entries), nil
}

// Transact creates the input transaction in the DB.
func (t *transactionRepository) Transact(
	ctx context.Context,
	transaction *models.Transaction,
) (*models.Transaction, apperrors.ApplicationError) {
	// Check if a transaction with Reference already exists
	existingTransaction, aerr := t.GetByID(ctx, transaction.GetID())
	if aerr != nil {
		return nil, apperrors.ErrSystemFailure.Override(aerr)
	}

	if existingTransaction != nil {
		var conflictErr apperrors.ApplicationError
		isConflict, conflictErr := t.IsConflict(ctx, transaction)
		if conflictErr != nil {
			return nil, conflictErr
		}
		if isConflict {
			return nil, apperrors.ErrTransactionIsConfilicting
		}
		return existingTransaction, nil
	}

	accountsMap, aerr := t.Validate(ctx, transaction)
	if aerr != nil {
		var appErr apperrors.ApplicationError
		if errors.As(aerr, &appErr) {
			return nil, appErr
		}
		return nil, apperrors.ErrSystemFailure.Override(aerr)
	}

	typedLedgerMap := make(map[string][]string)
	typedLedgerMap[models.LedgerTypeAsset] = []string{"CR", "DR"}
	typedLedgerMap[models.LedgerTypeExpense] = []string{"DR", "CR"}
	typedLedgerMap[models.LedgerTypeLiability] = []string{"CR", "DR"}
	typedLedgerMap[models.LedgerTypeIncome] = []string{"CR", "DR"}
	typedLedgerMap[models.LedgerTypeCapital] = []string{"CR", "DR"}

	// Add transaction Entries in one go to succeed or fail all
	for _, line := range transaction.Entries {
		account := accountsMap[line.AccountID]

		line.Balance = decimal.NewNullDecimal(account.Balance.Decimal)

		// Decide the signage of entry based on : https://en.wikipedia.org/wiki/Double-entry_bookkeeping :DEADCLIC
		if line.Credit &&
			(account.LedgerType == models.LedgerTypeAsset || account.LedgerType == models.LedgerTypeExpense) ||
			!line.Credit &&
				(account.LedgerType == models.LedgerTypeLiability || account.LedgerType == models.LedgerTypeIncome || account.LedgerType == models.LedgerTypeCapital) {
			line.Amount = decimal.NewNullDecimal(line.Amount.Decimal.Neg())
		}
	}

	// Create the transaction and its entries
	err := t.Pool().DB(ctx, false).Create(transaction).Error
	if err != nil {
		return nil, apperrors.ErrSystemFailure.Override(err)
	}

	timeNow := time.Now()
	if transaction.TransactedAt.IsZero() {
		transaction.TransactedAt = timeNow
	}
	// Only set ClearedAt if it was explicitly provided and is not zero
	// Don't automatically clear transactions that should remain uncleared

	result, err := t.GetByID(ctx, transaction.GetID())
	if err != nil {
		return nil, apperrors.ErrSystemFailure.Override(err)
	}
	return result, nil
}

// Update updates data of the given transaction.
func (t *transactionRepository) Update(
	ctx context.Context,
	txn *models.Transaction,
	fields ...string,
) (int64, error) {
	existingTransaction, errTx := t.GetByID(ctx, txn.ID)
	if errTx != nil {
		return 0, errTx
	}

	for key, value := range txn.Data {
		if value != "" && value != existingTransaction.Data[key] {
			existingTransaction.Data[key] = value
		}
	}

	if existingTransaction.ClearedAt.IsZero() {
		if !txn.ClearedAt.IsZero() {
			accountsMap, err1 := t.Validate(ctx, existingTransaction)
			if err1 != nil {
				return 0, err1
			}

			for _, line := range existingTransaction.Entries {
				account := accountsMap[line.AccountID]
				line.Balance = decimal.NewNullDecimal(account.Balance.Decimal)
			}
			existingTransaction.ClearedAt = txn.ClearedAt
		}
	}

	err := t.Pool().DB(ctx, false).Save(existingTransaction).Error
	if err != nil {
		util.Log(ctx).WithError(err).Error("could not save the transaction")
		return 0, err
	}
	return 1, nil
}

// Reverse creates a reversal  of the input transaction by creating a new transaction.
func (t *transactionRepository) Reverse(
	ctx context.Context,
	id string,
) (*models.Transaction, apperrors.ApplicationError) {
	// Check if a transaction with same Reference already exists
	reversalTxn, err1 := t.GetByID(ctx, id)
	if err1 != nil {
		return nil, apperrors.ErrSystemFailure.Override(err1)
	}

	if reversalTxn.TransactionType != ledgerv1.TransactionType_NORMAL.String() {
		return nil, apperrors.ErrTransactionTypeNotReversible.Extend(
			fmt.Sprintf("transaction (type=%s) is not reversible", reversalTxn.TransactionType),
		)
	}

	for _, entry := range reversalTxn.Entries {
		entry.ID = fmt.Sprintf("%s_REVERSAL", entry.ID)
		entry.Credit = !entry.Credit
	}

	reversalTxn.ID = fmt.Sprintf("%s_REVERSAL", reversalTxn.ID)
	reversalTxn.TransactionType = ledgerv1.TransactionType_REVERSAL.String()

	timeNow := time.Now()
	reversalTxn.TransactedAt = timeNow
	reversalTxn.CreatedAt = timeNow
	reversalTxn.ModifiedAt = timeNow

	return t.Transact(ctx, reversalTxn)
}
