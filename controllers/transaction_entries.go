package controllers

import (
	"github.com/antinvestor/service-ledger/ledger"
	"github.com/antinvestor/service-ledger/repositories"
	"google.golang.org/genproto/googleapis/type/money"
)

func transactionEntryToApi(mEntry *repositories.TransactionEntry) *ledger.TransactionEntry {

		units, nanos := toMoneyInt(mEntry.Amount.Int64)
		entryAmount := money.Money{Units: units,
			Nanos: nanos, CurrencyCode: mEntry.Currency.String}

		balUnits, balNanos := toMoneyInt(mEntry.Balance.Int64)
		balanceAmount := money.Money{Units: balUnits,
			Nanos: balNanos, CurrencyCode: mEntry.Currency.String}

		return &ledger.TransactionEntry{
			Account: mEntry.Account.String,
			Transaction: mEntry.Transaction.String,
			TransactedAt: mEntry.TransactedAt.String,
			Amount: &entryAmount,
			Credit: mEntry.Credit,
			AccBalance: &balanceAmount,
		}
	}



// Searches for transactions based on details of the query json
func (ledgerSrv *LedgerServer) SearchTransactionEntries(request *ledger.SearchRequest, server ledger.LedgerService_SearchTransactionEntriesServer) error {

	engine, aerr := repositories.NewSearchEngine(ledgerSrv.DB, repositories.SearchNamespaceTransactionEntries)
	if aerr != nil {
		return aerr
	}

	results, aerr := engine.Query(request.GetQuery())
	if aerr != nil {
		return aerr
	}

	castTransactionEntries, ok := results.([]*repositories.TransactionEntry)
	if !ok {
		return ledger.ErrorSearchQueryResultsNotCasting
	}

	for _, txn := range castTransactionEntries {
		_ = server.Send(transactionEntryToApi(txn))
	}

	return nil

}

