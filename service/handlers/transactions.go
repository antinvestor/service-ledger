package handlers

import (
	"context"
	"fmt"
	ledgerV1 "github.com/antinvestor/apis/go/ledger/v1"
	"github.com/antinvestor/service-ledger/service/models"
	"github.com/antinvestor/service-ledger/service/repository"
	"github.com/antinvestor/service-ledger/service/utility"
	"github.com/pitabwire/frame"
	"github.com/shopspring/decimal"
	"time"
)

func transactionToApi(mTxn *models.Transaction) *ledgerV1.Transaction {

	apiEntries := make([]*ledgerV1.TransactionEntry, len(mTxn.Entries))
	for index, mEntry := range mTxn.Entries {
		mEntry.TransactionID = mTxn.ID
		mEntry.Currency = mTxn.Currency
		mEntry.TransactedAt = mTxn.TransactedAt
		mEntry.ClearedAt = mTxn.ClearedAt

		apiEntries[index] = transactionEntryToApi(mEntry)
	}
	trx := &ledgerV1.Transaction{
		Reference: mTxn.ID,
		Currency:  mTxn.Currency,
		Cleared:   utility.IsValidTime(mTxn.ClearedAt),
		Data:      frame.DBPropertiesToMap(mTxn.Data),
		Entries:   apiEntries}

	if mTxn.TransactedAt != nil && !mTxn.TransactedAt.IsZero() {
		trx.TransactedAt = mTxn.TransactedAt.Format(repository.DefaultTimestamLayout)
	}

	trx.Cleared = utility.IsValidTime(mTxn.ClearedAt)

	trx.Type = ledgerV1.TransactionType(ledgerV1.TransactionType_value[mTxn.TransactionType])

	return trx
}

func transactionFromApi(aTxn *ledgerV1.Transaction) (*models.Transaction, error) {
	modelEntries := make([]*models.TransactionEntry, len(aTxn.Entries))
	for index, mEntry := range aTxn.Entries {
		modelEntries[index] = &models.TransactionEntry{
			Credit:    mEntry.Credit,
			AccountID: mEntry.GetAccount(),
			Amount:    decimal.NewNullDecimal(utility.FromMoney(mEntry.GetAmount())),
		}
	}

	transaction := &models.Transaction{
		BaseModel: frame.BaseModel{
			ID: aTxn.GetReference(),
		},
		Currency:        aTxn.GetCurrency(),
		TransactionType: aTxn.GetType().String(),
		Data:            frame.DBPropertiesFromMap(aTxn.Data),
		Entries:         modelEntries,
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
	transaction.TransactedAt = &transactedAt

	if aTxn.Cleared {
		transaction.ClearedAt = &transactedAt
	}

	return transaction, nil
}

// CreateTransaction a new transaction
func (ledgerSrv *LedgerServer) CreateTransaction(ctx context.Context, apiTransaction *ledgerV1.Transaction) (*ledgerV1.Transaction, error) {

	accountsRepo := repository.NewAccountRepository(ledgerSrv.Service)
	transactionsDB := repository.NewTransactionRepository(ledgerSrv.Service, accountsRepo)

	dbTransaction, err := transactionFromApi(apiTransaction)
	if err != nil {
		return nil, err
	}

	// Otherwise, do transaction
	transaction, err := transactionsDB.Transact(ctx, dbTransaction)
	if err != nil {
		return nil, err
	}

	return transactionToApi(transaction), nil
}

// SearchTransactions for transactions based on details of the query json
func (ledgerSrv *LedgerServer) SearchTransactions(request *ledgerV1.SearchRequest, server ledgerV1.LedgerService_SearchTransactionsServer) error {

	ctx := server.Context()

	accountRepository := repository.NewAccountRepository(ledgerSrv.Service)
	transactionRepository := repository.NewTransactionRepository(ledgerSrv.Service, accountRepository)

	jobResult, err := transactionRepository.Search(ctx, request.GetQuery())
	if err != nil {
		return err
	}

	for {

		result, ok, err0 := jobResult.ReadResult(ctx)
		if err0 != nil {
			return utility.ErrorSystemFailure.Override(err0)
		}

		if !ok {
			return nil
		}

		switch v := result.(type) {
		case []*models.Transaction:
			for _, transaction := range v {
				if err = server.Send(transactionToApi(transaction)); err != nil {
					return err
				}
			}

		case error:
			return v
		default:
			return utility.ErrorBadDataSupplied.Extend(fmt.Sprintf(" unsupported type supplied %v", v))
		}

	}
}

// UpdateTransaction a transaction's details
func (ledgerSrv *LedgerServer) UpdateTransaction(ctx context.Context, txn *ledgerV1.Transaction) (*ledgerV1.Transaction, error) {

	accountRepository := repository.NewAccountRepository(ledgerSrv.Service)
	transactionRepository := repository.NewTransactionRepository(ledgerSrv.Service, accountRepository)

	transaction, err := transactionFromApi(txn)
	if err != nil {
		return nil, err
	}
	// Otherwise, update transaction
	mTxn, terr := transactionRepository.Update(ctx, transaction)
	if terr != nil {
		return nil, terr
	}
	return transactionToApi(mTxn), nil
}

// ReverseTransaction a transaction by creating a new one with inverted entries
func (ledgerSrv *LedgerServer) ReverseTransaction(ctx context.Context, txn *ledgerV1.Transaction) (*ledgerV1.Transaction, error) {

	accountsRepo := repository.NewAccountRepository(ledgerSrv.Service)
	transactionRepository := repository.NewTransactionRepository(ledgerSrv.Service, accountsRepo)

	// Otherwise, do transaction
	mTxn, err := transactionRepository.Reverse(ctx, txn.Reference)
	if err != nil {
		return nil, err
	}

	return transactionToApi(mTxn), nil
}
