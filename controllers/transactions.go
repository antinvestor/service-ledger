package controllers

import (
	"context"
	"database/sql"
	"github.com/antinvestor/service-ledger/ledger"
	"github.com/antinvestor/service-ledger/models"
	"strings"
)

func transactionToApi(mTxn *models.Transaction) *ledger.Transaction {

	apiEntries := make([]*ledger.TransactionEntry, len(mTxn.Entries))
	for index, mEntry := range mTxn.Entries {
		mEntry.Currency = mTxn.Currency
		mEntry.TransactionID = mTxn.ID
		mEntry.Transaction = mTxn.Reference
		mEntry.TransactedAt = mTxn.TransactedAt
		apiEntries[index] = transactionEntryToApi(mEntry)
	}
	return &ledger.Transaction{Reference: mTxn.Reference.String,
		TransactedAt: mTxn.TransactedAt.String,
		Data: FromMap(mTxn.Data), Entries: apiEntries}
}

func transactionFromApi(aTxn *ledger.Transaction) *models.Transaction {
	modelEntries := make([]*models.TransactionEntry, len(aTxn.Entries))
	for index, mEntry := range aTxn.Entries {
		amount := fromMoney(mEntry.Amount)
		modelEntries[index] = &models.TransactionEntry{
			Credit:  mEntry.Credit,
			Account: sql.NullString{String: strings.ToUpper(mEntry.GetAccount()), Valid: true},
			Amount:  sql.NullInt64{Int64: amount, Valid: true}}
	}
	return &models.Transaction{
		Reference:    sql.NullString{String: strings.ToUpper(aTxn.Reference), Valid: aTxn.Reference != ""},
 		Currency:     sql.NullString{String: aTxn.Currency, Valid: aTxn.Currency != ""},
		TransactedAt: sql.NullString{String: aTxn.TransactedAt, Valid: aTxn.TransactedAt != ""},
		Data:         ToMap(aTxn.Data),
		Entries:      modelEntries}
}

// Creates a new transaction
func (ledgerSrv *LedgerServer) CreateTransaction(ctx context.Context, txn *ledger.Transaction) (*ledger.Transaction, error) {

	transactionsDB := models.NewTransactionDB(ledgerSrv.DB)

	apiTransaction := transactionFromApi(txn)

	// Otherwise, do transaction
	transaction, err := transactionsDB.Transact(apiTransaction)
	if err != nil {
		return nil, err
	}

	return transactionToApi(transaction), nil
}

// Searches for transactions based on details of the query json
func (ledgerSrv *LedgerServer) SearchTransactions(request *ledger.SearchRequest, server ledger.LedgerService_SearchTransactionsServer) error {

	engine, aerr := models.NewSearchEngine(ledgerSrv.DB, models.SearchNamespaceTransactions)
	if aerr != nil {
		return aerr
	}

	results, aerr := engine.Query(request.GetQuery())
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

	transactionDB := models.NewTransactionDB(ledgerSrv.DB)

	// Otherwise, update transaction
	mTxn, terr := transactionDB.UpdateTransaction(transactionFromApi(txn))
	if terr != nil {
		return nil, terr
	}
	return transactionToApi(mTxn), nil
}

// Reverses a transaction by creating a new one with inverted entries
func (ledgerSrv *LedgerServer) ReverseTransaction(ctx context.Context, txn *ledger.Transaction) (*ledger.Transaction, error) {

	transactionsDB := models.NewTransactionDB(ledgerSrv.DB)

	// Otherwise, do transaction
	mTxn, err := transactionsDB.Reverse(txn.Reference)
	if err != nil {
		return nil, err
	}

	return transactionToApi(mTxn), nil
}
