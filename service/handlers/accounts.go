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
)

func accountToApi(mAcc *models.Account) *ledgerV1.Account {

	accountBalance := decimal.Zero
	if mAcc.Balance.Valid {
		accountBalance = mAcc.Balance.Decimal
	}
	balance := utility.ToMoney(mAcc.Currency, accountBalance)

	reservedBalanceAmt := decimal.Zero
	if mAcc.ReservedBalance.Valid {
		reservedBalanceAmt = mAcc.ReservedBalance.Decimal
	}

	reservedBalance := utility.ToMoney(mAcc.Currency, reservedBalanceAmt)

	unClearedBalanceAmt := decimal.Zero
	if mAcc.UnClearedBalance.Valid {
		unClearedBalanceAmt = mAcc.UnClearedBalance.Decimal
	}
	unClearedBalance := utility.ToMoney(mAcc.Currency, unClearedBalanceAmt)

	return &ledgerV1.Account{
		Reference: mAcc.ID, Ledger: mAcc.LedgerID,
		Balance: &balance, ReservedBalance: &reservedBalance, UnclearedBalance: &unClearedBalance,
		Data: frame.DBPropertiesToMap(mAcc.Data)}
}

func accountFromApi(account *ledgerV1.Account) *models.Account {

	accountBalance := utility.FromMoney(account.GetBalance())

	return &models.Account{
		BaseModel: frame.BaseModel{ID: account.GetReference()},
		LedgerID:  account.GetLedger(),
		Currency:  account.GetBalance().CurrencyCode,
		Balance:   decimal.NewNullDecimal(accountBalance),
		Data:      frame.DBPropertiesFromMap(account.Data)}
}

func (ledgerSrv *LedgerServer) SearchAccounts(
	request *ledgerV1.SearchRequest, server ledgerV1.LedgerService_SearchAccountsServer) error {

	ctx := server.Context()

	accountsRepo := repository.NewAccountRepository(ledgerSrv.Service)

	jobResult, err := accountsRepo.Search(ctx, request.GetQuery())
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
		case []*models.Account:
			for _, acc := range v {
				if err = server.Send(accountToApi(acc)); err != nil {
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

// CreateAccount a new account based on supplied data
func (ledgerSrv *LedgerServer) CreateAccount(ctx context.Context, aAcc *ledgerV1.Account) (*ledgerV1.Account, error) {

	accountsRepo := repository.NewAccountRepository(ledgerSrv.Service)

	// Otherwise, add account
	mAcc, aerr := accountsRepo.Create(ctx, accountFromApi(aAcc))
	if aerr != nil {
		return nil, aerr
	}

	return accountToApi(mAcc), nil

}

// UpdateAccount the data component of the account.
func (ledgerSrv *LedgerServer) UpdateAccount(ctx context.Context, aAcc *ledgerV1.Account) (*ledgerV1.Account, error) {

	accountsRepo := repository.NewAccountRepository(ledgerSrv.Service)

	// Otherwise, add account
	mAcc, aerr := accountsRepo.Update(ctx, aAcc.Reference, aAcc.Data)
	if aerr != nil {
		return nil, aerr
	}

	return accountToApi(mAcc), nil

}
