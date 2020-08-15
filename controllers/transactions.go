package controllers

import (
	"bitbucket.org/caricah/service-ledger/ledger"
	"bitbucket.org/caricah/service-ledger/models"
	"context"
	"database/sql"
	"google.golang.org/genproto/googleapis/type/money"
	"strings"
)

func transactionToApi(mTxn *models.Transaction) *ledger.Transaction {

	apiEntries := make([]*ledger.TransactionEntry, len(mTxn.Entries))
	for _, mEntry := range mTxn.Entries {
		units, nanos := toMoneyInt(mEntry.Amount.Int64)
		entryAmount := money.Money{Units: units, Nanos: nanos, CurrencyCode: mTxn.Currency.String}
		apiEntries = append(apiEntries, &ledger.TransactionEntry{Account: mEntry.Account.String, Amount: &entryAmount})
	}
	return &ledger.Transaction{Reference: mTxn.Reference.String, TransactedAt: mTxn.TransactedAt.String, Data: FromMap(mTxn.Data), Entries: apiEntries}
}

func transactionFromApi(aTxn *ledger.Transaction) *models.Transaction {
	modelEntries := make([]*models.TransactionEntry, len(aTxn.Entries))
	for _, mEntry := range aTxn.Entries {
		amount := fromMoney(mEntry.Amount)
		modelEntries = append(modelEntries, &models.TransactionEntry{
			Account: sql.NullString{String: strings.ToUpper(mEntry.Account), Valid: true},
			Amount:  sql.NullInt64{Int64: amount, Valid: true}})
	}
	return &models.Transaction{
		Reference: sql.NullString{String: strings.ToUpper(aTxn.Reference), Valid: true},
		Currency:     sql.NullString{String: aTxn.Currency, Valid: true},
		TransactedAt: sql.NullString{String: aTxn.TransactedAt, Valid: true},
		Data:         ToMap(aTxn.Data), Entries: modelEntries}
}

// Creates a new transaction
func (ledgerSrv *LedgerServer) CreateTransaction(ctx context.Context, txn *ledger.Transaction) (*ledger.Transaction, error) {

	transactionsDB := models.NewTransactionDB(ledgerSrv.DB)

	// Otherwise, do transaction
	transaction, err := transactionsDB.Transact(transactionFromApi(txn))
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
		server.Send(transactionToApi(txn))
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
	mTxn, err := transactionsDB.Reverse(transactionFromApi(txn))
	if err != nil {
		return nil, err
	}

	return transactionToApi(mTxn), nil
}
