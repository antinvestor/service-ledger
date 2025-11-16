package business_test

import (
	"context"
	"testing"

	ledgerv1 "buf.build/gen/go/antinvestor/ledger/protocolbuffers/go/ledger/v1"
	"github.com/antinvestor/service-ledger/apps/default/service/models"
	"github.com/antinvestor/service-ledger/apps/default/tests"
	_ "github.com/lib/pq"
	"github.com/pitabwire/frame/data"
	"github.com/pitabwire/frame/frametests/definition"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/types/known/structpb"
)

type AccountBusinessSuite struct {
	tests.BaseTestSuite
	ledger *models.Ledger
}

func TestAccountBusinessSuite(t *testing.T) {
	suite.Run(t, new(AccountBusinessSuite))
}

func (as *AccountBusinessSuite) setupFixtures(ctx context.Context, resources *tests.ServiceResources) {
	// Create test ledgers using business layer
	ledgerBusiness := resources.LedgerBusiness

	createLedgerReq := &ledgerv1.CreateLedgerRequest{
		Id:   "test-ledger",
		Type: ledgerv1.LedgerType_ASSET,
	}

	ledger, err := ledgerBusiness.CreateLedger(ctx, createLedgerReq)
	as.Require().NoError(err, "Unable to create ledger for account")
	
	// Convert to model for test use
	as.ledger = &models.Ledger{
		BaseModel: data.BaseModel{ID: ledger.GetId()},
		Type:      models.FromLedgerType(ledger.GetType()),
	}
}

func (as *AccountBusinessSuite) TestCreateAccountWithBusinessValidation() {
	as.WithTestDependencies(as.T(), func(t *testing.T, dep *definition.DependencyOption) {
		ctx, _, resources := as.CreateService(t, dep)
		as.setupFixtures(ctx, resources)

		// Test creating account through business layer
		accountBusiness := resources.AccountBusiness

		createAccountReq := &ledgerv1.CreateAccountRequest{
			Id:       "test-account",
			LedgerId:  as.ledger.ID,
			Currency: "USD",
		}

		account, err := accountBusiness.CreateAccount(ctx, createAccountReq)
		require.NoError(t, err, "Error creating account through business layer")
		require.NotNil(t, account, "Account should be created")

		assert.Equal(t, "test-account", account.GetId(), "Invalid account ID")
		assert.Equal(t, as.ledger.ID, account.GetLedger(), "Invalid ledger ID")
		assert.Equal(t, "USD", account.GetBalance().GetCurrencyCode(), "Invalid currency")
		assert.Equal(t, int64(0), account.GetBalance().GetUnits(), "Initial balance should be zero")
		assert.Equal(t, int32(0), account.GetBalance().GetNanos(), "Initial nanos should be zero")
	})
}

func (as *AccountBusinessSuite) TestCreateAccountWithInvalidCurrency() {
	as.WithTestDependencies(as.T(), func(t *testing.T, dep *definition.DependencyOption) {
		ctx, _, resources := as.CreateService(t, dep)
		as.setupFixtures(ctx, resources)

		accountBusiness := resources.AccountBusiness

		createAccountReq := &ledgerv1.CreateAccountRequest{
			Id:       "invalid-account",
			LedgerId:  as.ledger.ID,
			Currency: "INVALID", // Invalid currency code
		}

		account, err := accountBusiness.CreateAccount(ctx, createAccountReq)
		assert.Error(t, err, "Should fail with invalid currency")
		assert.Nil(t, account, "Account should not be created")
		assert.Contains(t, err.Error(), "currency is invalid", "Error should mention currency validation")
	})
}

func (as *AccountBusinessSuite) TestCreateAccountWithMissingLedger() {
	as.WithTestDependencies(as.T(), func(t *testing.T, dep *definition.DependencyOption) {
		ctx, _, resources := as.CreateService(t, dep)

		accountBusiness := resources.AccountBusiness

		createAccountReq := &ledgerv1.CreateAccountRequest{
			Id:       "orphan-account",
			LedgerId:  "non-existent-ledger",
			Currency: "USD",
		}

		account, err := accountBusiness.CreateAccount(ctx, createAccountReq)
		assert.Error(t, err, "Should fail with non-existent ledger")
		assert.Nil(t, account, "Account should not be created")
	})
}

func (as *AccountBusinessSuite) TestGetAccount() {
	as.WithTestDependencies(as.T(), func(t *testing.T, dep *definition.DependencyOption) {
		ctx, _, resources := as.CreateService(t, dep)
		as.setupFixtures(ctx, resources)

		accountBusiness := resources.AccountBusiness

		// First create an account
		createAccountReq := &ledgerv1.CreateAccountRequest{
			Id:       "get-test-account",
			LedgerId:  as.ledger.ID,
			Currency: "EUR",
		}

		createdAccount, err := accountBusiness.CreateAccount(ctx, createAccountReq)
		require.NoError(t, err, "Error creating account")

		// Now retrieve it
		retrievedAccount, err := accountBusiness.GetAccount(ctx, "get-test-account")
		require.NoError(t, err, "Error retrieving account")

		assert.Equal(t, createdAccount.GetId(), retrievedAccount.GetId(), "Retrieved account should match created account")
		assert.Equal(t, createdAccount.GetLedger(), retrievedAccount.GetLedger(), "Ledger ID should match")
		assert.Equal(t, createdAccount.GetBalance().GetCurrencyCode(), retrievedAccount.GetBalance().GetCurrencyCode(), "Currency should match")
	})
}

func (as *AccountBusinessSuite) TestGetAccountNotFound() {
	as.WithTestDependencies(as.T(), func(t *testing.T, dep *definition.DependencyOption) {
		ctx, _, resources := as.CreateService(t, dep)

		accountBusiness := resources.AccountBusiness

		account, err := accountBusiness.GetAccount(ctx, "non-existent-account")
		assert.Error(t, err, "Should fail with non-existent account")
		assert.Nil(t, account, "Account should be nil")
	})
}

func (as *AccountBusinessSuite) TestUpdateAccount() {
	as.WithTestDependencies(as.T(), func(t *testing.T, dep *definition.DependencyOption) {
		ctx, _, resources := as.CreateService(t, dep)
		as.setupFixtures(ctx, resources)

		accountBusiness := resources.AccountBusiness

		// Create an account first
		createAccountReq := &ledgerv1.CreateAccountRequest{
			Id:       "update-test-account",
			LedgerId:  as.ledger.ID,
			Currency: "GBP",
		}

		_, err := accountBusiness.CreateAccount(ctx, createAccountReq)
		require.NoError(t, err, "Error creating account")

		// Update the account data
		updateAccountReq := &ledgerv1.UpdateAccountRequest{
			Id: "update-test-account",
			Data: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"description": {Kind: &structpb.Value_StringValue{StringValue: "Updated description"}},
					"category":    {Kind: &structpb.Value_StringValue{StringValue: "Test category"}},
				},
			},
		}

		updatedAccount, err := accountBusiness.UpdateAccount(ctx, updateAccountReq)
		require.NoError(t, err, "Error updating account")
		require.NotNil(t, updatedAccount, "Updated account should not be nil")

		// Verify the update
		assert.Equal(t, "Updated description", updatedAccount.GetData().GetFields()["description"].GetStringValue())
		assert.Equal(t, "Test category", updatedAccount.GetData().GetFields()["category"].GetStringValue())
	})
}
