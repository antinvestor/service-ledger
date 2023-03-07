package controllers

import (
	"context"
	"github.com/antinvestor/service-ledger/ledger"
	"github.com/antinvestor/service-ledger/models"
	"github.com/antinvestor/service-ledger/repositories"
	"github.com/pitabwire/frame"
)

func transactionToApi(mTxn *models.Transaction) *ledger.Transaction {

	apiEntries := make([]*ledger.TransactionEntry, len(mTxn.Entries))
	for index, mEntry := range mTxn.Entries {
		mEntry.Currency = mTxn.Currency
		mEntry.TransactionID = mTxn.ID
		mEntry.TransactedAt = mTxn.TransactedAt
		apiEntries[index] = transactionEntryToApi(mEntry)
	}
	return &ledger.Transaction{
		Reference:    mTxn.ID,
		TransactedAt: mTxn.TransactedAt,
		Data:         frame.DBPropertiesToMap(mTxn.Data),
		Entries:      apiEntries}
}

func transactionFromApi(aTxn *ledger.Transaction) *models.Transaction {
	modelEntries := make([]*models.TransactionEntry, len(aTxn.Entries))
	for index, mEntry := range aTxn.Entries {
		modelEntries[index] = &models.TransactionEntry{
			Credit:    mEntry.Credit,
			AccountID: mEntry.GetAccount(),
			Amount:    fromMoney(mEntry.Amount),
		}
	}
	return &models.Transaction{
		BaseModel: frame.BaseModel{
			ID: aTxn.GetReference(),
		},
		Currency:     aTxn.GetCurrency(),
		TransactedAt: aTxn.GetTransactedAt(),
		Data:         frame.DBPropertiesFromMap(aTxn.Data),
		Entries:      modelEntries,
	}
}

// CreateTransaction a new transaction
func (ledgerSrv *LedgerServer) CreateTransaction(ctx context.Context, txn *ledger.Transaction) (*ledger.Transaction, error) {

	accountsRepo := repositories.NewAccountRepository(ledgerSrv.Service)
	transactionsDB := repositories.NewTransactionRepository(ledgerSrv.Service, accountsRepo)

	apiTransaction := transactionFromApi(txn)

	// Otherwise, do transaction
	transaction, err := transactionsDB.Transact(ctx, apiTransaction)
	if err != nil {
		return nil, err
	}

	return transactionToApi(transaction), nil
}

// SearchTransactions for transactions based on details of the query json
func (ledgerSrv *LedgerServer) SearchTransactions(request *ledger.SearchRequest, server ledger.LedgerService_SearchTransactionsServer) error {

	ctx := server.Context()
	engine, aerr := repositories.NewSearchEngine(ledgerSrv.Service, repositories.SearchNamespaceTransactions)
	if aerr != nil {
		return aerr
	}

	results, aerr := engine.Query(ctx, request.GetQuery())
	if aerr != nil {
		return aerr
	}

	castTransactions, ok := results.([]*models.Transaction)
	if !ok {
		return ledger.ErrorSearchQueryResultsNotCasting
	}

	for _, txn := range castTransactions {
		_ = server.Send(transactionToApi(txn))
	}

	return nil

}

// Updates a transaction's details
func (ledgerSrv *LedgerServer) UpdateTransaction(ctx context.Context, txn *ledger.Transaction) (*ledger.Transaction, error) {

	accountsRepo := repositories.NewAccountRepository(ledgerSrv.Service)
	transactionDB := repositories.NewTransactionRepository(ledgerSrv.Service, accountsRepo)

	// Otherwise, update transaction
	mTxn, terr := transactionDB.Update(ctx, transactionFromApi(txn))
	if terr != nil {
		return nil, terr
	}
	return transactionToApi(mTxn), nil
}

// ReverseTransaction a transaction by creating a new one with inverted entries
func (ledgerSrv *LedgerServer) ReverseTransaction(ctx context.Context, txn *ledger.Transaction) (*ledger.Transaction, error) {

	accountsRepo := repositories.NewAccountRepository(ledgerSrv.Service)
	transactionRepository := repositories.NewTransactionRepository(ledgerSrv.Service, accountsRepo)

	// Otherwise, do transaction
	mTxn, err := transactionRepository.Reverse(ctx, txn.Reference)
	if err != nil {
		return nil, err
	}

	return transactionToApi(mTxn), nil
}
