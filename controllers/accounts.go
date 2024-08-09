package controllers

import (
	"context"
	ledgerV1 "github.com/antinvestor/apis/go/ledger/v1"
	"github.com/antinvestor/service-ledger/models"
	"github.com/antinvestor/service-ledger/repositories"
	"github.com/pitabwire/frame"
	"github.com/shopspring/decimal"
	"google.golang.org/genproto/googleapis/type/money"
	"math/big"
)

func toMoneyInt(currency string, naive decimal.Decimal) money.Money {
	return money.Money{CurrencyCode: currency, Units: naive.IntPart(), Nanos: naive.Exponent()}
}

func fromMoney(m *money.Money) (naive decimal.Decimal) {
	return decimal.NewFromBigInt(new(big.Int).SetInt64(m.Units), m.Nanos)
}

func accountToApi(mAcc *models.Account) *ledgerV1.Account {

	accountBalance := decimal.Zero
	if mAcc.Balance.Valid {
		accountBalance = mAcc.Balance.Decimal
	}
	balance := toMoneyInt(mAcc.Currency, accountBalance)

	return &ledgerV1.Account{Reference: mAcc.ID,
		Ledger: mAcc.LedgerID, Balance: &balance, Data: frame.DBPropertiesToMap(mAcc.Data)}
}

func accountFromApi(account *ledgerV1.Account) *models.Account {

	accountBalance := fromMoney(account.GetBalance())

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

	accountsRepo := repositories.NewAccountRepository(ledgerSrv.Service)

	accountsChannel, err := accountsRepo.Search(ctx, request.GetQuery())
	if err != nil {
		return err
	}

	for {

		select {

		case result := <-accountsChannel:

			switch v := result.(type) {
			case *models.Account:
				_ = server.Send(accountToApi(v))
			case error:
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil

		}

	}
}

// CreateAccount a new account based on supplied data
func (ledgerSrv *LedgerServer) CreateAccount(ctx context.Context, aAcc *ledgerV1.Account) (*ledgerV1.Account, error) {

	accountsRepo := repositories.NewAccountRepository(ledgerSrv.Service)

	// Otherwise, add account
	mAcc, aerr := accountsRepo.Create(ctx, accountFromApi(aAcc))
	if aerr != nil {
		return nil, aerr
	}

	return accountToApi(mAcc), nil

}

// UpdateAccount the data component of the account.
func (ledgerSrv *LedgerServer) UpdateAccount(ctx context.Context, aAcc *ledgerV1.Account) (*ledgerV1.Account, error) {

	accountsRepo := repositories.NewAccountRepository(ledgerSrv.Service)

	// Otherwise, add account
	mAcc, aerr := accountsRepo.Update(ctx, aAcc.Reference, aAcc.Data)
	if aerr != nil {
		return nil, aerr
	}

	return accountToApi(mAcc), nil

}
