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

	accountRepository := repositories.NewAccountRepository(ledgerSrv.Service)
	transactionRepository := repositories.NewTransactionRepository(ledgerSrv.Service, accountRepository)

	castTransactionEntries, aerr := transactionRepository.SearchEntries(ctx, request.GetQuery())
	if aerr != nil {
		return aerr
	}

	for _, entry := range castTransactionEntries {
		_ = server.Send(transactionEntryToApi(entry))
	}

	return nil

}
