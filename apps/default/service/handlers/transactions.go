package handlers

import (
	"context"
	"time"

	commonv1 "github.com/antinvestor/apis/go/common/v1"
	ledgerV1 "github.com/antinvestor/apis/go/ledger/v1"
	"github.com/antinvestor/service-ledger/apps/default/service/models"
	"github.com/antinvestor/service-ledger/apps/default/service/repository"
	"github.com/antinvestor/service-ledger/internal/apperrors"
	utility2 "github.com/antinvestor/service-ledger/internal/utility"
	"github.com/pitabwire/frame"
	"github.com/shopspring/decimal"
)

func transactionToAPI(mTxn *models.Transaction) *ledgerV1.Transaction {
	apiEntries := make([]*ledgerV1.TransactionEntry, len(mTxn.Entries))
	for index, mEntry := range mTxn.Entries {
		mEntry.TransactionID = mTxn.ID
		mEntry.Currency = mTxn.Currency
		mEntry.TransactedAt = mTxn.TransactedAt
		mEntry.ClearedAt = mTxn.ClearedAt

		apiEntries[index] = transactionEntryToAPI(mEntry)
	}
	trx := &ledgerV1.Transaction{
		Reference: mTxn.ID,
		Currency:  mTxn.Currency,
		Cleared:   utility2.IsValidTime(mTxn.ClearedAt),
		Data:      frame.DBPropertiesToMap(mTxn.Data),
		Entries:   apiEntries}

	if !mTxn.TransactedAt.IsZero() {
		trx.TransactedAt = mTxn.TransactedAt.Format(repository.DefaultTimestamLayout)
	}

	trx.Cleared = utility2.IsValidTime(mTxn.ClearedAt)

	trx.Type = ledgerV1.TransactionType(ledgerV1.TransactionType_value[mTxn.TransactionType])

	return trx
}

func transactionFromAPI(aTxn *ledgerV1.Transaction) (*models.Transaction, error) {
	transaction := &models.Transaction{
		BaseModel: frame.BaseModel{
			ID: aTxn.GetReference(),
		},
		Currency:        aTxn.GetCurrency(),
		TransactionType: aTxn.GetType().String(),
		Data:            frame.DBPropertiesFromMap(aTxn.GetData()),
	}

	for _, mEntry := range aTxn.GetEntries() {
		transaction.Entries = append(transaction.Entries, &models.TransactionEntry{
			TransactionID: transaction.GetID(),
			Credit:        mEntry.GetCredit(),
			AccountID:     mEntry.GetAccount(),
			Currency:      mEntry.GetAmount().GetCurrencyCode(),
			Amount:        decimal.NewNullDecimal(utility2.FromMoney(mEntry.GetAmount())),
		})
	}

	var transactedAt time.Time
	if aTxn.GetTransactedAt() == "" {
		transactedAt = time.Now().UTC()
	} else {
		var err error
		transactedAt, err = time.Parse(repository.DefaultTimestamLayout, aTxn.GetTransactedAt())
		if err != nil {
			return nil, err
		}
	}
	transaction.TransactedAt = transactedAt

	if aTxn.GetCleared() {
		transaction.ClearedAt = transactedAt
	}

	return transaction, nil
}

// CreateTransaction a new transaction.
func (ledgerSrv *LedgerServer) CreateTransaction(
	ctx context.Context,
	apiTransaction *ledgerV1.Transaction,
) (*ledgerV1.Transaction, error) {
	accountsRepo := repository.NewAccountRepository(ledgerSrv.Service)
	transactionsDB := repository.NewTransactionRepository(ledgerSrv.Service, accountsRepo)

	dbTransaction, err := transactionFromAPI(apiTransaction)
	if err != nil {
		return nil, err
	}

	// Otherwise, do transaction
	transaction, err := transactionsDB.Transact(ctx, dbTransaction)
	if err != nil {
		return nil, err
	}

	return transactionToAPI(transaction), nil
}

// SearchTransactions for transactions based on details of the query json.
func (ledgerSrv *LedgerServer) SearchTransactions(
	request *commonv1.SearchRequest,
	server ledgerV1.LedgerService_SearchTransactionsServer,
) error {
	ctx := server.Context()

	accountRepository := repository.NewAccountRepository(ledgerSrv.Service)
	transactionRepository := repository.NewTransactionRepository(ledgerSrv.Service, accountRepository)

	jobResult, err := transactionRepository.Search(ctx, request.GetQuery())
	if err != nil {
		return err
	}

	for result := range jobResult.ResultChan() {
		if result.IsError() {
			return apperrors.ErrSystemFailure.Override(result.Error())
		}

		for _, transaction := range result.Item() {
			if err = server.Send(transactionToAPI(transaction)); err != nil {
				return err
			}
		}
	}
	return nil
}

// UpdateTransaction a transaction's details.
func (ledgerSrv *LedgerServer) UpdateTransaction(
	ctx context.Context,
	txn *ledgerV1.Transaction,
) (*ledgerV1.Transaction, error) {
	accountRepository := repository.NewAccountRepository(ledgerSrv.Service)
	transactionRepository := repository.NewTransactionRepository(ledgerSrv.Service, accountRepository)

	transaction, err := transactionFromAPI(txn)
	if err != nil {
		return nil, err
	}
	// Otherwise, update transaction
	mTxn, terr := transactionRepository.Update(ctx, transaction)
	if terr != nil {
		return nil, terr
	}
	return transactionToAPI(mTxn), nil
}

// ReverseTransaction a transaction by creating a new one with inverted entries.
func (ledgerSrv *LedgerServer) ReverseTransaction(
	ctx context.Context,
	txn *ledgerV1.Transaction,
) (*ledgerV1.Transaction, error) {
	accountsRepo := repository.NewAccountRepository(ledgerSrv.Service)
	transactionRepository := repository.NewTransactionRepository(ledgerSrv.Service, accountsRepo)

	// Otherwise, do transaction
	mTxn, err := transactionRepository.Reverse(ctx, txn.GetReference())
	if err != nil {
		return nil, err
	}

	return transactionToAPI(mTxn), nil
}
