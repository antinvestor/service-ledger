package controllers

import (
	"github.com/antinvestor/service-ledger/ledger"
	"github.com/antinvestor/service-ledger/models"
	"github.com/antinvestor/service-ledger/repositories"
)

func transactionEntryToApi(mEntry *models.TransactionEntry) *ledger.TransactionEntry {

	entryAmount := toMoneyInt(mEntry.Currency, mEntry.Amount)

	balanceAmount := toMoneyInt(mEntry.Currency, mEntry.Balance)

	return &ledger.TransactionEntry{
		Account:      mEntry.AccountID,
		Transaction:  mEntry.TransactionID,
		TransactedAt: mEntry.TransactedAt,
		Amount:       &entryAmount,
		Credit:       mEntry.Credit,
		AccBalance:   &balanceAmount,
	}
}

// SearchTransactionEntries for transactions based on details of the query json
func (ledgerSrv *LedgerServer) SearchTransactionEntries(request *ledger.SearchRequest, server ledger.LedgerService_SearchTransactionEntriesServer) error {

	ctx := server.Context()
	engine, aerr := repositories.NewSearchEngine(ledgerSrv.Service, repositories.SearchNamespaceTransactionEntries)
	if aerr != nil {
		return aerr
	}

	results, aerr := engine.Query(ctx, request.GetQuery())
	if aerr != nil {
		return aerr
	}

	castTransactionEntries, ok := results.([]*models.TransactionEntry)
	if !ok {
		return ledger.ErrorSearchQueryResultsNotCasting
	}

	for _, txn := range castTransactionEntries {
		_ = server.Send(transactionEntryToApi(txn))
	}

	return nil

}
