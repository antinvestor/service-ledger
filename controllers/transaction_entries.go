package controllers

import (
	"context"
	ledgerV1 "github.com/antinvestor/apis/go/ledger/v1"
	"github.com/antinvestor/service-ledger/models"
	"github.com/antinvestor/service-ledger/repositories"
)

func transactionEntryToApi(mEntry *models.TransactionEntry) *ledgerV1.TransactionEntry {

	entryAmount := toMoneyInt(mEntry.Currency, mEntry.Amount.Decimal)

	balanceAmount := toMoneyInt(mEntry.Currency, mEntry.Balance.Decimal)

	return &ledgerV1.TransactionEntry{
		Account:     mEntry.AccountID,
		Transaction: mEntry.TransactionID,
		Amount:      &entryAmount,
		Credit:      mEntry.Credit,
		AccBalance:  &balanceAmount,
	}
}

// SearchTransactionEntries for transactions based on details of the query json
func (ledgerSrv *LedgerServer) SearchTransactionEntries(request *ledgerV1.SearchRequest, server ledgerV1.LedgerService_SearchTransactionEntriesServer) error {

	ctx := server.Context()
	service := ledgerSrv.Service

	accountRepository := repositories.NewAccountRepository(ledgerSrv.Service)
	transactionRepository := repositories.NewTransactionRepository(ledgerSrv.Service, accountRepository)

	transactionEntriesChannel := make(chan any)
	job := service.NewJob(func(ctx context.Context) error {

		transactionRepository.SearchEntries(ctx, request.GetQuery(), transactionEntriesChannel)
		return nil

	})

	err := service.SubmitJob(ctx, job)
	if err != nil {
		return err
	}

	for {

		select {

		case entry := <-transactionEntriesChannel:

			switch v := entry.(type) {

			case *models.TransactionEntry:
				_ = server.Send(transactionEntryToApi(v))
			case error:
				return err
			}

		default:
			return nil

		}
	}

}
