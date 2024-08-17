package handlers

import (
	"fmt"
	ledgerV1 "github.com/antinvestor/apis/go/ledger/v1"
	"github.com/antinvestor/service-ledger/service/models"
	"github.com/antinvestor/service-ledger/service/repository"
	"github.com/antinvestor/service-ledger/service/utility"
)

func transactionEntryToApi(mEntry *models.TransactionEntry) *ledgerV1.TransactionEntry {

	entryAmount := utility.ToMoney(mEntry.Currency, mEntry.Amount.Decimal)

	balanceAmount := utility.ToMoney(mEntry.Currency, mEntry.Balance.Decimal)

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

	accountRepository := repository.NewAccountRepository(ledgerSrv.Service)
	transactionRepository := repository.NewTransactionRepository(ledgerSrv.Service, accountRepository)

	transactionEntriesChannel, err := transactionRepository.SearchEntries(ctx, request.GetQuery())
	if err != nil {
		return err
	}

	for {
		select {
		case result, ok := <-transactionEntriesChannel:
			if !ok {
				// Channel closed, stop processing
				return nil
			}

			switch v := result.(type) {
			case *models.TransactionEntry:
				if err = server.Send(transactionEntryToApi(v)); err != nil {
					return err
				}
			case error:
				return v
			default:
				return utility.ErrorBadDataSupplied.Extend(fmt.Sprintf(" unsupported type supplied %v", v))
			}

		case <-ctx.Done():
			return ctx.Err()
		}
	}

}
