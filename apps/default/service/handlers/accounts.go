package handlers

import (
	"context"

	commonv1 "github.com/antinvestor/apis/go/common/v1"
	ledgerV1 "github.com/antinvestor/apis/go/ledger/v1"
	"github.com/antinvestor/service-ledger/apps/default/service/models"
	"github.com/antinvestor/service-ledger/apps/default/service/repository"
	"github.com/antinvestor/service-ledger/internal/apperrors"
	utility2 "github.com/antinvestor/service-ledger/internal/utility"
	"github.com/pitabwire/frame"
	"github.com/shopspring/decimal"
)

func accountToAPI(mAcc *models.Account) *ledgerV1.Account {
	accountBalance := decimal.Zero
	if mAcc.Balance.Valid {
		accountBalance = mAcc.Balance.Decimal
	}
	balance := utility2.ToMoney(mAcc.Currency, accountBalance)

	reservedBalanceAmt := decimal.Zero
	if mAcc.ReservedBalance.Valid {
		reservedBalanceAmt = mAcc.ReservedBalance.Decimal
	}

	reservedBalance := utility2.ToMoney(mAcc.Currency, reservedBalanceAmt)

	unClearedBalanceAmt := decimal.Zero
	if mAcc.UnClearedBalance.Valid {
		unClearedBalanceAmt = mAcc.UnClearedBalance.Decimal
	}
	unClearedBalance := utility2.ToMoney(mAcc.Currency, unClearedBalanceAmt)

	return &ledgerV1.Account{
		Reference: mAcc.ID, Ledger: mAcc.LedgerID,
		Balance: &balance, ReservedBalance: &reservedBalance, UnclearedBalance: &unClearedBalance,
		Data: frame.DBPropertiesToMap(mAcc.Data)}
}

func accountFromAPI(account *ledgerV1.Account) *models.Account {
	accountBalance := utility2.FromMoney(account.GetBalance())

	return &models.Account{
		BaseModel: frame.BaseModel{ID: account.GetReference()},
		LedgerID:  account.GetLedger(),
		Currency:  account.GetBalance().GetCurrencyCode(),
		Balance:   decimal.NewNullDecimal(accountBalance),
		Data:      frame.DBPropertiesFromMap(account.GetData())}
}

func (ledgerSrv *LedgerServer) SearchAccounts(
	request *commonv1.SearchRequest, server ledgerV1.LedgerService_SearchAccountsServer) error {
	ctx := server.Context()

	accountsRepo := repository.NewAccountRepository(ledgerSrv.Service)

	jobResult, err := accountsRepo.Search(ctx, request.GetQuery())
	if err != nil {
		return err
	}

	for result := range jobResult.ResultChan() {
		if result.IsError() {
			return apperrors.ErrSystemFailure.Override(result.Error())
		}

		for _, acc := range result.Item() {
			if err = server.Send(accountToAPI(acc)); err != nil {
				return err
			}
		}
	}

	return nil
}

// CreateAccount a new account based on supplied data.
func (ledgerSrv *LedgerServer) CreateAccount(ctx context.Context, aAcc *ledgerV1.Account) (*ledgerV1.Account, error) {
	accountsRepo := repository.NewAccountRepository(ledgerSrv.Service)

	// Otherwise, add account
	mAcc, aerr := accountsRepo.Create(ctx, accountFromAPI(aAcc))
	if aerr != nil {
		return nil, aerr
	}

	return accountToAPI(mAcc), nil
}

// UpdateAccount the data component of the account.
func (ledgerSrv *LedgerServer) UpdateAccount(ctx context.Context, aAcc *ledgerV1.Account) (*ledgerV1.Account, error) {
	accountsRepo := repository.NewAccountRepository(ledgerSrv.Service)

	// Otherwise, add account
	mAcc, aerr := accountsRepo.Update(ctx, aAcc.GetReference(), aAcc.GetData())
	if aerr != nil {
		return nil, aerr
	}

	return accountToAPI(mAcc), nil
}
