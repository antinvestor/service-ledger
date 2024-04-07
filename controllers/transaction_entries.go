package controllers

import (
	ledgerV1 "github.com/antinvestor/apis/go/ledger/v1"
	"github.com/antinvestor/service-ledger/models"
	"github.com/antinvestor/service-ledger/repositories"
)

func transactionEntryToApi(mEntry *models.TransactionEntry) *ledgerV1.TransactionEntry {

	entryAmount := toMoneyInt(mEntry.Currency, mEntry.Amount)

	balanceAmount := toMoneyInt(mEntry.Currency, mEntry.Balance)

	return &ledgerV1.TransactionEntry{
		Account:      mEntry.AccountID,
		Transaction:  mEntry.TransactionID,
		TransactedAt: mEntry.TransactedAt,
		Amount:       &entryAmount,
		Credit:       mEntry.Credit,
		AccBalance:   &balanceAmount,
	}
}

// SearchTransactionEntries for transactions based on details of the query json
func (ledgerSrv *LedgerServer) SearchTransactionEntries(request *ledgerV1.SearchRequest, server ledgerV1.LedgerService_SearchTransactionEntriesServer) error {

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
